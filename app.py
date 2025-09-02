import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Iterable
import hashlib
import csv
import random  # added
from urllib.parse import quote as url_quote  # added
from contextlib import (
    asynccontextmanager,
)  # retained if needed elsewhere (no longer used for app lifespan)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # python-dotenv not installed, use os.getenv defaults
    pass

import httpx
from aiolimiter import AsyncLimiter
from diskcache import Cache
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, __version__ as PYDANTIC_VERSION
from pytrends.request import TrendReq

# Configuration from environment variables
SCHEMA_VERSION = "3"  # bump when changing output structure
ENABLE_HISTORY = os.getenv("ENABLE_HISTORY", "1") == "1"
HISTORY_FILE = Path(os.getenv("HISTORY_FILE", "history.jsonl"))
PURGE_ON_NEW_JOB = os.getenv("PURGE_ON_NEW_JOB", "1") == "1"
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
RUN_MODE = os.getenv("RUN_MODE", "demo" if not YOUTUBE_API_KEY else "live")
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # 1 hour

# Global resources
GLOBAL_SEMAPHORE = asyncio.Semaphore(CONCURRENCY)
cache = Cache("./cache", size_limit=1024**3)  # 1GB cache
jobs: Dict[str, Dict] = {}

# Rate limiters per API
RATE_LIMITERS = {
    "youtube": AsyncLimiter(100, 100),
    "github": (AsyncLimiter(60, 60) if GITHUB_TOKEN else AsyncLimiter(10, 60)),
    "reddit": AsyncLimiter(60, 60),
    "trends": AsyncLimiter(1, 5),
    "forum": AsyncLimiter(5, 10),  # new
    "search": AsyncLimiter(5, 10),  # new
}

# Sources list constant
ALL_SOURCES = ["youtube", "trends", "github", "reddit", "forum", "search"]  # extended

# Runtime flags (STOP_ON_COMPLETE disabled by design now)
STOP_ON_COMPLETE = False  # Forced off per option A

# Global flag to skip further Trends queries after a 429
TRENDS_RATE_LIMITED = False
TRENDS_MAX_RETRIES = int(os.getenv("TRENDS_MAX_RETRIES", "3"))  # new
TRENDS_BACKOFF_BASE = float(os.getenv("TRENDS_BACKOFF_BASE", "5"))  # new
_GLOBAL_TRENDS_CLIENT: Optional[TrendReq] = None  # new

# Track last completed job id
last_completed_job_id: Optional[str] = None


def purge_output_files(sources: List[str]):
    """Delete existing output files for the given sources (including temp files)."""
    output_dir = Path("./output")
    if not output_dir.exists():
        return
    for src in sources:
        for suffix in [".json", ".json.tmp"]:
            f = output_dir / f"{src}{suffix}"
            if f.exists():
                try:
                    f.unlink()
                    print(f"[purge] Removed {f}")
                except Exception as e:
                    print(f"[purge] Failed to remove {f}: {e}")


class WorkflowEntry(BaseModel):
    id: Optional[str] = None
    run_id: Optional[str] = None
    workflow: str
    platform: str
    popularity_metrics: Dict[str, Any]
    country: str = "unknown"
    source_id: str
    url: str
    collected_at: str
    score: Optional[float] = None
    schema_version: str = SCHEMA_VERSION


class JobStatus(BaseModel):
    job_id: str
    source_statuses: Dict[str, str]
    completed_at: Optional[str] = None
    counts: Dict[str, int]
    total_collected: int


class JobResponse(BaseModel):
    job_id: str
    started_at: str


def normalize_title(title: str) -> str:
    """Normalize title for deduplication by removing punctuation and converting to lowercase."""
    return re.sub(r"[^\w\s]", "", title.lower().strip())


def compute_entry_id(platform: str, source_id: str, title: str) -> str:
    h = hashlib.sha1()
    h.update(platform.encode())
    h.update(b"|")
    h.update(source_id.encode())
    h.update(b"|")
    h.update(normalize_title(title).encode())
    return h.hexdigest()


class DataCollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )
        self.seen_titles: Set[str] = set()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def collect_with_retry(self, func, *args, **kwargs):
        """Execute function with exponential backoff on 429/5xx errors."""
        for attempt in range(3):
            try:
                return await func(*args, **kwargs)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (429, 500, 502, 503, 504):
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2**attempt)

    def get_cached_or_fetch(self, cache_key: str, data: Any) -> Any:
        """Get from cache or store new data with TTL."""
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        cache.set(cache_key, data, expire=CACHE_TTL)
        return data

    async def collect_youtube_data(self, queries: List[str]) -> List[WorkflowEntry]:
        """Collect workflow data from YouTube API."""
        if RUN_MODE == "demo" or not YOUTUBE_API_KEY:
            return self._get_demo_youtube_data()

        results = []
        for query in queries:
            async with GLOBAL_SEMAPHORE:
                await RATE_LIMITERS["youtube"].acquire()

                cache_key = f"youtube_{SCHEMA_VERSION}_{query}"
                cached_data = cache.get(cache_key)
                if cached_data:
                    results.extend(cached_data)
                    continue

                try:
                    # Search for videos
                    search_url = "https://www.googleapis.com/youtube/v3/search"
                    search_params = {
                        "part": "snippet",
                        "q": query,
                        "type": "video",
                        "maxResults": 50,
                        "key": YOUTUBE_API_KEY,
                        "regionCode": "US",
                    }

                    search_response = await self.collect_with_retry(
                        self.client.get, search_url, params=search_params
                    )
                    search_response.raise_for_status()
                    search_data = search_response.json()

                    video_ids = [
                        item["id"]["videoId"] for item in search_data.get("items", [])
                    ]
                    if not video_ids:
                        continue

                    # Get video statistics
                    stats_url = "https://www.googleapis.com/youtube/v3/videos"
                    stats_params = {
                        "part": "statistics",
                        "id": ",".join(video_ids),
                        "key": YOUTUBE_API_KEY,
                    }

                    stats_response = await self.collect_with_retry(
                        self.client.get, stats_url, params=stats_params
                    )
                    stats_response.raise_for_status()
                    stats_data = stats_response.json()

                    # Combine search and stats data
                    stats_by_id = {
                        item["id"]: item["statistics"]
                        for item in stats_data.get("items", [])
                    }

                    query_results = []
                    for item in search_data.get("items", []):
                        video_id = item["id"]["videoId"]
                        title = item["snippet"]["title"]

                        # Deduplicate by normalized title
                        normalized = normalize_title(title)
                        if normalized in self.seen_titles:
                            continue
                        self.seen_titles.add(normalized)

                        stats = stats_by_id.get(video_id, {})
                        views = int(stats.get("viewCount", 0))
                        likes = (
                            int(stats.get("likeCount", 0))
                            if stats.get("likeCount")
                            else None
                        )
                        comments = (
                            int(stats.get("commentCount", 0))
                            if stats.get("commentCount")
                            else None
                        )

                        entry = WorkflowEntry(
                            workflow=title,
                            platform="YouTube",
                            popularity_metrics={
                                "views": views,
                                "likes": likes,
                                "comments": comments,
                                "upvotes": None,
                                "like_to_view_ratio": (
                                    likes / views if likes and views > 0 else None
                                ),
                            },
                            country="US",
                            source_id=video_id,
                            url=f"https://www.youtube.com/watch?v={video_id}",
                            collected_at=datetime.now(timezone.utc).isoformat(),
                            id=compute_entry_id("YouTube", video_id, title),
                        )
                        query_results.append(entry)

                    cache.set(cache_key, query_results, expire=CACHE_TTL)
                    results.extend(query_results)

                except Exception as e:
                    print(f"Error collecting YouTube data for query '{query}': {e}")
                    continue

        return results

    async def collect_github_data(self, queries: List[str]) -> List[WorkflowEntry]:
        """Collect workflow data from GitHub API."""
        if RUN_MODE == "demo":
            return self._get_demo_github_data()

        results = []
        headers = {}
        if GITHUB_TOKEN:
            headers["Authorization"] = f"token {GITHUB_TOKEN}"

        for query in queries:
            async with GLOBAL_SEMAPHORE:
                await RATE_LIMITERS["github"].acquire()

                cache_key = f"github_{query}"
                cached_data = cache.get(cache_key)
                if cached_data:
                    results.extend(cached_data)
                    continue

                try:
                    search_url = "https://api.github.com/search/repositories"
                    params = {
                        "q": f"{query} n8n workflow",
                        "sort": "stars",
                        "order": "desc",
                        "per_page": 50,
                    }

                    response = await self.collect_with_retry(
                        self.client.get, search_url, params=params, headers=headers
                    )
                    response.raise_for_status()
                    data = response.json()

                    query_results = []
                    for item in data.get("items", []):
                        title = (
                            item["name"] + " - " + (item.get("description", "") or "")
                        )

                        # Deduplicate by normalized title
                        normalized = normalize_title(title)
                        if normalized in self.seen_titles:
                            continue
                        self.seen_titles.add(normalized)

                        entry = WorkflowEntry(
                            workflow=title,
                            platform="GitHub",
                            popularity_metrics={
                                "views": None,
                                "likes": item.get("stargazers_count", 0),
                                "comments": None,
                                "upvotes": item.get("stargazers_count", 0),
                                "like_to_view_ratio": None,
                            },
                            country="unknown",
                            source_id=str(item["id"]),
                            url=item["html_url"],
                            collected_at=datetime.now(timezone.utc).isoformat(),
                            id=compute_entry_id("GitHub", str(item["id"]), title),
                        )
                        query_results.append(entry)

                    cache.set(cache_key, query_results, expire=CACHE_TTL)
                    results.extend(query_results)

                except Exception as e:
                    print(f"Error collecting GitHub data for query '{query}': {e}")
                    continue

        return results

    async def collect_trends_data(self, queries: List[str]) -> List[WorkflowEntry]:
        """Collect workflow data from Google Trends with retry/backoff."""
        results: List[WorkflowEntry] = []
        global TRENDS_RATE_LIMITED  # ensure declared before inner use
        for query in queries:
            if TRENDS_RATE_LIMITED:
                print(
                    "[trends] Skipping remaining queries due to prior rate-limit flag."
                )
                break
            async with GLOBAL_SEMAPHORE:
                await RATE_LIMITERS["trends"].acquire()
                cache_key = f"trends_{query}"
                cached_data = cache.get(cache_key)
                if cached_data:
                    results.extend(cached_data)
                    continue
                attempt = 0
                while attempt < TRENDS_MAX_RETRIES:
                    try:
                        loop = asyncio.get_event_loop()
                        trend_data = await loop.run_in_executor(
                            None, self._get_trends_data, query
                        )
                        if (
                            not trend_data
                            and (
                                RUN_MODE == "demo"
                                or os.getenv("TRENDS_ALLOW_DEMO", "0") == "1"
                            )
                            and attempt == TRENDS_MAX_RETRIES - 1
                        ):
                            ts = datetime.now(timezone.utc).isoformat()
                            fallback = WorkflowEntry(
                                workflow=f"Google Trends (fallback demo): {query}",
                                platform="Google Trends",
                                popularity_metrics={
                                    "views": 0,
                                    "likes": None,
                                    "comments": None,
                                    "upvotes": None,
                                    "like_to_view_ratio": None,
                                },
                                country="US",
                                source_id=f"trends_demo_{normalize_title(query)}",
                                url=f"https://trends.google.com/trends/explore?q={query}",
                                collected_at=ts,
                                id=compute_entry_id(
                                    "Google Trends",
                                    f"trends_demo_{normalize_title(query)}",
                                    query,
                                ),
                            )
                            trend_data = [fallback]
                        cache.set(cache_key, trend_data, expire=CACHE_TTL)
                        results.extend(trend_data)
                        break
                    except Exception as e:
                        attempt += 1
                        if "429" in str(e):
                            print(
                                f"[trends] 429 encountered at attempt {attempt} for '{query}'"
                            )
                            TRENDS_RATE_LIMITED = True
                            break
                        if attempt >= TRENDS_MAX_RETRIES:
                            print(
                                f"[trends] Failed '{query}' after {attempt} attempts: {e}"
                            )
                            break
                        sleep_for = TRENDS_BACKOFF_BASE * (
                            2 ** (attempt - 1)
                        ) + random.uniform(0, 2)
                        print(
                            f"[trends] attempt {attempt} err for '{query}': {e}; retry in {sleep_for:.1f}s"
                        )
                        await asyncio.sleep(sleep_for)
        return results

    def _get_trends_data(self, query: str) -> List[WorkflowEntry]:
        """Get Google Trends data synchronously."""
        try:
            global _GLOBAL_TRENDS_CLIENT
            if _GLOBAL_TRENDS_CLIENT is None:
                _GLOBAL_TRENDS_CLIENT = TrendReq(hl="en-US", tz=360)
            pytrends = _GLOBAL_TRENDS_CLIENT
            pytrends.build_payload(
                [query], cat=0, timeframe="today 12-m", geo="US", gprop=""
            )

            interest_data = pytrends.interest_over_time()
            if interest_data.empty:
                print(f"[trends] interest_over_time empty for query '{query}'")
                return []

            # Create a single entry representing the trend
            avg_interest = (
                int(interest_data[query].mean())
                if not interest_data[query].empty
                else 0
            )
            # Trend growth: (last - first)/first (normalized growth), guard division by zero
            first_val = (
                int(interest_data[query].iloc[0])
                if not interest_data[query].empty
                else 0
            )
            last_val = (
                int(interest_data[query].iloc[-1])
                if not interest_data[query].empty
                else 0
            )
            trend_growth = (last_val - first_val) / first_val if first_val > 0 else 0.0

            normalized = normalize_title(f"Google Trends: {query}")
            if normalized not in self.seen_titles:
                self.seen_titles.add(normalized)

                return [
                    WorkflowEntry(
                        workflow=f"Google Trends: {query}",
                        platform="Google Trends",
                        popularity_metrics={
                            "views": avg_interest,
                            "likes": None,
                            "comments": None,
                            "upvotes": None,
                            "like_to_view_ratio": None,
                            "trend_growth": trend_growth,
                        },
                        country="US",
                        source_id=f"trends_{query}",
                        url=f"https://trends.google.com/trends/explore?q={query}",
                        collected_at=datetime.now(timezone.utc).isoformat(),
                        id=compute_entry_id("Google Trends", f"trends_{query}", query),
                    )
                ]
            return []
        except Exception:
            import traceback

            tb = traceback.format_exc()
            print(f"[trends] Exception for query '{query}':\n{tb}")
            if "TooManyRequestsError" in tb or "429" in tb:
                global TRENDS_RATE_LIMITED
                TRENDS_RATE_LIMITED = True
            return []

    async def collect_reddit_data(self, queries: List[str]) -> List[WorkflowEntry]:
        """Collect workflow data from Reddit."""
        if RUN_MODE == "demo":
            return self._get_demo_reddit_data()

        results = []
        subreddits = ["n8n", "automation", "workflows"]

        for subreddit in subreddits:
            for query in queries:
                async with GLOBAL_SEMAPHORE:
                    await RATE_LIMITERS["reddit"].acquire()

                    cache_key = f"reddit_{subreddit}_{query}"
                    cached_data = cache.get(cache_key)
                    if cached_data:
                        results.extend(cached_data)
                        continue

                    try:
                        # Use Reddit JSON API (no auth required)
                        search_url = f"https://www.reddit.com/r/{subreddit}/search.json"
                        params = {
                            "q": query,
                            "sort": "top",
                            "limit": 25,
                            "restrict_sr": "true",
                        }

                        response = await self.collect_with_retry(
                            self.client.get,
                            search_url,
                            params=params,
                            headers={"User-Agent": "n8n-popularity-collector/1.0"},
                        )
                        response.raise_for_status()
                        data = response.json()

                        query_results = []
                        for item in data.get("data", {}).get("children", []):
                            post_data = item.get("data", {})
                            title = post_data.get("title", "")

                            # Deduplicate by normalized title
                            normalized = normalize_title(title)
                            if normalized in self.seen_titles:
                                continue
                            self.seen_titles.add(normalized)

                            entry = WorkflowEntry(
                                workflow=title,
                                platform="Reddit",
                                popularity_metrics={
                                    "views": None,
                                    "likes": None,
                                    "comments": post_data.get("num_comments", 0),
                                    "upvotes": post_data.get("ups", 0),
                                    "like_to_view_ratio": None,
                                },
                                country="unknown",
                                source_id=post_data.get("id", ""),
                                url=f"https://reddit.com{post_data.get('permalink', '')}",
                                collected_at=datetime.now(timezone.utc).isoformat(),
                                id=compute_entry_id(
                                    "Reddit", post_data.get("id", ""), title
                                ),
                            )
                            query_results.append(entry)

                        cache.set(cache_key, query_results, expire=CACHE_TTL)
                        results.extend(query_results)

                    except Exception as e:
                        print(
                            f"Error collecting Reddit data for subreddit '{subreddit}' query '{query}': {e}"
                        )
                        continue

        return results

    async def collect_forum_data(self, pages: int = 2) -> List[WorkflowEntry]:
        """Collect topics from n8n community forum (Discourse)."""
        base = "https://community.n8n.io/latest.json"
        out: List[WorkflowEntry] = []
        for p in range(pages):
            async with GLOBAL_SEMAPHORE:
                await RATE_LIMITERS["forum"].acquire()
                try:
                    resp = await self.client.get(
                        base,
                        params={"page": p},
                        headers={"User-Agent": "n8n-popularity-collector/1.0"},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    print(f"[forum] page {p} error: {e}")
                    break
                topics = (data.get("topic_list") or {}).get("topics", [])
                for t in topics:
                    title = t.get("title", "")
                    norm = normalize_title(title)
                    if norm in self.seen_titles:
                        continue
                    self.seen_titles.add(norm)
                    views = t.get("views", 0)
                    likes = t.get("like_count", 0)
                    replies = t.get("reply_count", 0)
                    entry = WorkflowEntry(
                        workflow=title,
                        platform="n8n Forum",
                        popularity_metrics={
                            "views": views,
                            "likes": likes,
                            "comments": replies,
                            "upvotes": likes,
                            "like_to_view_ratio": (likes / views) if views else None,
                        },
                        country="unknown",
                        source_id=str(t.get("id")),
                        url=f"https://community.n8n.io/t/{t.get('id')}",
                        collected_at=datetime.now(timezone.utc).isoformat(),
                        id=compute_entry_id("n8n Forum", str(t.get("id")), title),
                    )
                    out.append(entry)
        return out

    async def collect_search_data(
        self, queries: List[str], pages: int = 1
    ) -> List[WorkflowEntry]:
        """Collect DuckDuckGo search results (light HTML parsing)."""
        results: List[WorkflowEntry] = []
        headers = {"User-Agent": "Mozilla/5.0 (compatible; n8n-collector/1.0)"}
        for q in queries:
            for p in range(pages):
                async with GLOBAL_SEMAPHORE:
                    await RATE_LIMITERS["search"].acquire()
                    start = p * 50
                    url = f"https://duckduckgo.com/html/?q={url_quote(q)}&s={start}"
                    try:
                        resp = await self.client.get(url, headers=headers)
                        if resp.status_code != 200:
                            break
                        text = resp.text
                    except Exception as e:
                        print(f"[search] q='{q}' page={p} error: {e}")
                        break
                    for m in re.finditer(
                        r'<a[^>]+class="result__a"[^>]*href="(.*?)"[^>]*>(.*?)</a>',
                        text,
                    ):
                        href = m.group(1)
                        title_html = m.group(2)
                        title = re.sub(r"<.*?>", " ", title_html)
                        title = re.sub(r"\s+", " ", title).strip()
                        if not href or not title:
                            continue
                        norm = normalize_title(title)
                        if norm in self.seen_titles:
                            continue
                        self.seen_titles.add(norm)
                        sid = hashlib.sha1(href.encode()).hexdigest()
                        entry = WorkflowEntry(
                            workflow=title,
                            platform="Web Search",
                            popularity_metrics={
                                "views": None,
                                "likes": None,
                                "comments": None,
                                "upvotes": None,
                                "like_to_view_ratio": None,
                            },
                            country="unknown",
                            source_id=sid,
                            url=href,
                            collected_at=datetime.now(timezone.utc).isoformat(),
                            id=compute_entry_id("Web Search", sid, title),
                        )
                        results.append(entry)
        return results

    def _get_demo_youtube_data(self) -> List[WorkflowEntry]:
        """Return demo YouTube data when API key is not available."""
        return [
            WorkflowEntry(
                workflow="Demo: Google Sheets to Slack Automation",
                platform="YouTube",
                popularity_metrics={
                    "views": 1250,
                    "likes": 85,
                    "comments": 12,
                    "upvotes": None,
                    "like_to_view_ratio": 0.068,
                },
                country="US",
                source_id="demo_yt_1",
                url="https://youtube.com/watch?v=demo1",
                collected_at=datetime.now(timezone.utc).isoformat(),
            ),
            WorkflowEntry(
                workflow="Demo: n8n Workflow Tutorial",
                platform="YouTube",
                popularity_metrics={
                    "views": 890,
                    "likes": 45,
                    "comments": 8,
                    "upvotes": None,
                    "like_to_view_ratio": 0.051,
                },
                country="US",
                source_id="demo_yt_2",
                url="https://youtube.com/watch?v=demo2",
                collected_at=datetime.now(timezone.utc).isoformat(),
            ),
        ]

    def _get_demo_github_data(self) -> List[WorkflowEntry]:
        """Return demo GitHub data."""
        return [
            WorkflowEntry(
                workflow="Demo: n8n-workflow-examples - Collection of n8n workflows",
                platform="GitHub",
                popularity_metrics={
                    "views": None,
                    "likes": 156,
                    "comments": None,
                    "upvotes": 156,
                    "like_to_view_ratio": None,
                },
                country="unknown",
                source_id="demo_gh_1",
                url="https://github.com/demo/n8n-workflows",
                collected_at=datetime.now(timezone.utc).isoformat(),
            )
        ]

    def _get_demo_reddit_data(self) -> List[WorkflowEntry]:
        """Return demo Reddit data."""
        return [
            WorkflowEntry(
                workflow="Demo: Best n8n workflows for automation?",
                platform="Reddit",
                popularity_metrics={
                    "views": None,
                    "likes": None,
                    "comments": 23,
                    "upvotes": 45,
                    "like_to_view_ratio": None,
                },
                country="unknown",
                source_id="demo_reddit_1",
                url="https://reddit.com/r/n8n/comments/demo1",
                collected_at=datetime.now(timezone.utc).isoformat(),
            )
        ]


async def write_results_atomically(source: str, results: List[WorkflowEntry]):
    """Write results to JSON file atomically."""
    output_dir = Path("./output")
    output_dir.mkdir(exist_ok=True)

    temp_file = output_dir / f"{source}.json.tmp"
    final_file = output_dir / f"{source}.json"

    # Convert to JSON-serializable format (pydantic v1/v2 compatibility)
    data = []
    for entry in results:
        if hasattr(entry, "model_dump"):
            data.append(entry.model_dump())  # pydantic v2
        else:
            data.append(entry.dict())  # pydantic v1

    # Write to temporary file first
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Atomic rename
    if final_file.exists():
        try:
            final_file.unlink()
        except Exception as e:
            print(f"[write] Could not remove existing {final_file}: {e}")
    try:
        temp_file.rename(final_file)
    except Exception as e:
        print(f"[write] Rename failed for {source}: {e}")


async def collect_source_data(source: str, job_id: str):
    """Collect data for a specific source."""
    queries = [
        "n8n workflow automation",
        "n8n google sheets slack",
        "n8n workflow tutorial",
        "n8n integration",
        "n8n automation examples",
    ]

    try:
        jobs[job_id]["source_statuses"][source] = "running"

        async with DataCollector() as collector:
            if source == "youtube":
                results = await collector.collect_youtube_data(queries)
            elif source == "github":
                results = await collector.collect_github_data(queries)
            elif source == "trends":
                results = await collector.collect_trends_data(queries)
            elif source == "reddit":
                results = await collector.collect_reddit_data(queries)
            elif source == "forum":
                pages = int(os.getenv("FORUM_PAGES", "2"))
                results = await collector.collect_forum_data(pages=pages)
            elif source == "search":
                pages = int(os.getenv("SEARCH_PAGES", "1"))
                results = await collector.collect_search_data(queries, pages=pages)
            else:
                raise ValueError(f"Unknown source: {source}")

            # Compute per-source scores before writing
            try:
                compute_scores_for_source(source, results)
            except Exception as e:
                print(f"[scoring] Failed to compute scores for {source}: {e}")

            for r in results:
                if not r.id:
                    r.id = compute_entry_id(r.platform, r.source_id, r.workflow)
                r.run_id = job_id
            await write_results_atomically(source, results)
            if ENABLE_HISTORY:
                try:
                    append_history(results)
                except Exception as he:
                    print(f"[history] append failed for {source}: {he}")
            # Record per-source count for job summary
            try:
                jobs[job_id]["counts"][source] = len(results)
            except Exception:
                pass
            jobs[job_id]["source_statuses"][source] = "completed"

    except Exception as e:
        jobs[job_id]["source_statuses"][source] = f"failed: {str(e)}"
        print(f"Error collecting {source} data: {e}")


app = FastAPI(
    title="n8n Popularity Collector",
    description="Collect workflow mentions from multiple public sources",
    version="1.0.0",
)


@app.on_event("startup")
async def on_startup():
    auto_collect = os.getenv("AUTO_COLLECT_ON_START", "1") == "1"
    if not auto_collect:
        print("[startup] AUTO_COLLECT_ON_START disabled")
        return
    try:
        job_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        sources_to_collect = ALL_SOURCES
        if PURGE_ON_NEW_JOB:
            purge_output_files(sources_to_collect)
        jobs[job_id] = {
            "job_id": job_id,
            "started_at": started_at,
            "source_statuses": {s: "pending" for s in sources_to_collect},
            "counts": {s: 0 for s in sources_to_collect},
            "total_collected": 0,
            "completed_at": None,
        }

        async def run_auto_collection():
            print(f"[startup] Auto collection job {job_id} started for all sources")
            tasks = [collect_source_data(s, job_id) for s in sources_to_collect]
            await asyncio.gather(*tasks, return_exceptions=True)
            jobs[job_id]["total_collected"] = sum(jobs[job_id]["counts"].values())
            jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
            print(f"[startup] Auto collection job {job_id} completed")
            global last_completed_job_id
            last_completed_job_id = job_id

        asyncio.create_task(run_auto_collection())
    except Exception as e:
        print(f"[startup] Failed to start auto collection: {e}")


@app.on_event("shutdown")
async def on_shutdown():
    try:
        cache.close()
    except Exception:
        pass


# ---------------- Scoring Utilities ---------------- #
def _normalize(values: List[Optional[float]]) -> List[float]:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return [0.0 for _ in values]
    max_val = max(filtered)
    if max_val <= 0:
        return [0.0 for _ in values]
    return [(v / max_val) if v is not None else 0.0 for v in values]


def compute_scores_for_source(source: str, entries: List[WorkflowEntry]):
    """Attach a score to each entry based on source-specific weighting.

    Formulas (user provided / adapted):
      YouTube: 0.45*norm_views + 0.25*norm_likes + 0.15*norm_comments + 0.15*norm_like_to_view_ratio
      Reddit (forum adaptation - reweighed since only comments & upvotes are available):
          Original weights replies .40, likes .30, views .20, unique .10.
          Using only comments (replies) & upvotes (likes) -> rescale to 1.0:
              comments_weight = 0.40 / 0.70 ≈ 0.5714
              upvotes_weight  = 0.30 / 0.70 ≈ 0.4286
      Google Trends: 0.6*norm_search_interest + 0.4*norm_trend_growth
      GitHub: (only stars available) score = norm_stars
    """
    if not entries:
        return

    if source == "youtube":
        views = [e.popularity_metrics.get("views") for e in entries]
        likes = [e.popularity_metrics.get("likes") for e in entries]
        comments = [e.popularity_metrics.get("comments") for e in entries]
        ratios = [e.popularity_metrics.get("like_to_view_ratio") for e in entries]
        n_views = _normalize(views)
        n_likes = _normalize(likes)
        n_comments = _normalize(comments)
        n_ratios = _normalize(ratios)
        for i, e in enumerate(entries):
            e.score = round(
                0.45 * n_views[i]
                + 0.25 * n_likes[i]
                + 0.15 * n_comments[i]
                + 0.15 * n_ratios[i],
                6,
            )
    elif source == "reddit":
        comments = [e.popularity_metrics.get("comments") for e in entries]
        upvotes = [e.popularity_metrics.get("upvotes") for e in entries]
        n_comments = _normalize(comments)
        n_upvotes = _normalize(upvotes)
        for i, e in enumerate(entries):
            e.score = round(0.571428 * n_comments[i] + 0.428572 * n_upvotes[i], 6)
    elif source == "trends":
        interest = [
            e.popularity_metrics.get("views") for e in entries
        ]  # avg interest as proxy
        growth = [e.popularity_metrics.get("trend_growth") for e in entries]
        n_interest = _normalize(interest)
        n_growth = _normalize(growth)
        for i, e in enumerate(entries):
            e.score = round(0.6 * n_interest[i] + 0.4 * n_growth[i], 6)
    elif source == "github":
        stars = [e.popularity_metrics.get("likes") for e in entries]
        n_stars = _normalize(stars)
        for i, e in enumerate(entries):
            e.score = round(n_stars[i], 6)
    elif source == "forum":
        views = [e.popularity_metrics.get("views") for e in entries]
        likes = [e.popularity_metrics.get("likes") for e in entries]
        comments = [e.popularity_metrics.get("comments") for e in entries]
        n_views = _normalize(views)
        n_likes = _normalize(likes)
        n_comments = _normalize(comments)
        for i, e in enumerate(entries):
            e.score = round(
                0.4 * n_views[i] + 0.3 * n_likes[i] + 0.3 * n_comments[i], 6
            )
    elif source == "search":
        for e in entries:
            e.score = 0.0
    # else leave score None


@app.get("/collect", response_model=JobResponse)
async def collect_data(source: str = "all"):
    """Trigger data collection for specified source(s)."""
    valid_sources = ["all", "youtube", "trends", "github", "reddit", "forum", "search"]
    if source not in valid_sources:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {valid_sources}"
        )

    job_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    sources_to_collect = ALL_SOURCES if source == "all" else [source]

    # Purge existing output files when a new manual collection job starts
    if PURGE_ON_NEW_JOB:
        purge_output_files(sources_to_collect)

    jobs[job_id] = {
        "job_id": job_id,
        "started_at": started_at,
        "source_statuses": {s: "pending" for s in sources_to_collect},
        "counts": {s: 0 for s in sources_to_collect},
        "total_collected": 0,
        "completed_at": None,
    }

    # Start collection tasks
    async def run_collection():
        tasks = [collect_source_data(s, job_id) for s in sources_to_collect]
        await asyncio.gather(*tasks, return_exceptions=True)
        jobs[job_id]["total_collected"] = sum(jobs[job_id]["counts"].values())
        jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    global last_completed_job_id
    last_completed_job_id = job_id

    asyncio.create_task(run_collection())

    return JobResponse(job_id=job_id, started_at=started_at)


@app.get("/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get the status of a collection job."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatus(**jobs[job_id])


@app.get("/results/{source}")
async def get_results(source: str):
    """Get results for a specific source."""
    valid_sources = ["youtube", "trends", "github", "reddit", "forum", "search"]
    if source not in valid_sources:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {valid_sources}"
        )

    results_file = Path(f"./output/{source}.json")
    if not results_file.exists():
        raise HTTPException(
            status_code=404, detail=f"No results found for source: {source}"
        )

    try:
        with open(results_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading results: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with basic info."""
    return {
        "service": "n8n-popularity-collector",
        "version": "1.0.0",
        "mode": RUN_MODE,
        "endpoints": {
            "collect": "/collect?source=all|youtube|trends|github|reddit|forum|search",
            "status": "/status/{job_id}",
            "results": "/results/{source}",
            "rankings": "/rankings?source=all|youtube|trends|github|reddit|forum|search&limit=50",
            "trending": "/trending?source=all|youtube|trends|github|reddit|forum|search&limit=100",
            "export_json": "/export/json?history=0",
            "export_csv": "/export/csv?history=0",
        },
    }


@app.get("/jobs")
async def list_jobs():
    """List all jobs with brief status summary."""
    out = []
    for jid, data in jobs.items():
        out.append(
            {
                "job_id": jid,
                "completed_at": data.get("completed_at"),
                "total_collected": data.get("total_collected"),
                "source_statuses": data.get("source_statuses", {}),
            }
        )
    # Sort with most recent started_at first
    out.sort(key=lambda x: x.get("completed_at") or "", reverse=True)
    return out


@app.get("/last_completed")
async def last_completed():
    """Return summary of the last completed job."""
    if not last_completed_job_id or last_completed_job_id not in jobs:
        raise HTTPException(status_code=404, detail="No completed job yet")
    data = jobs[last_completed_job_id]
    return {
        "job_id": last_completed_job_id,
        "counts": data.get("counts", {}),
        "total_collected": data.get("total_collected"),
        "completed_at": data.get("completed_at"),
        "source_statuses": data.get("source_statuses", {}),
    }


@app.get("/rankings")
async def rankings(source: str = "all", limit: int = 50):
    """Return ranked entries by score for a given source or all sources combined.

    If source == all, aggregate all available outputs and sort by score.
    """
    valid_sources = ["all", "youtube", "trends", "github", "reddit", "forum", "search"]
    if source not in valid_sources:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {valid_sources}"
        )

    sources_to_load = ALL_SOURCES if source == "all" else [source]
    aggregated: List[Dict[str, Any]] = []
    for src in sources_to_load:
        f = Path(f"./output/{src}.json")
        if not f.exists():
            continue
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            # Ensure score exists (backward compatibility) by recomputing if absent
            if data and "score" not in data[0]:
                # Reconstruct WorkflowEntry objects minimally to reuse scoring
                entries = []
                for item in data:
                    entries.append(WorkflowEntry(**item))
                compute_scores_for_source(src, entries)
                data = [
                    e.model_dump() if hasattr(e, "model_dump") else e.dict()
                    for e in entries
                ]
            aggregated.extend(data)
        except Exception as e:
            print(f"[rankings] Failed to load {src} data: {e}")
            continue

    # Filter entries missing score (compute_scores may have left some None)
    aggregated = [a for a in aggregated if a.get("score") is not None]
    aggregated.sort(key=lambda x: x.get("score", 0), reverse=True)
    return aggregated[:limit]


# ---------------- History & Aggregation ---------------- #
def append_history(entries: Iterable[WorkflowEntry]):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        for e in entries:
            rec = e.model_dump() if hasattr(e, "model_dump") else e.dict()
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def load_current_snapshot() -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    for src in ALL_SOURCES:
        fp = Path(f"./output/{src}.json")
        if not fp.exists():
            continue
        try:
            with open(fp, "r", encoding="utf-8") as fh:
                part = json.load(fh)
            if isinstance(part, list):
                data.extend(part)
        except Exception as e:
            print(f"[aggregate] load failed {fp}: {e}")
    return data


def load_history_stream(limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []

    def gen():
        count = 0
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if limit is not None and count >= limit:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                    count += 1
                except Exception:
                    continue

    return gen()


ALIAS_MAP = {
    "yt": "youtube",
    "gh": "github",
    "git": "github",
    "forums": "forum",
    "web": "search",
}


@app.get("/trending")
async def trending(
    source: str = "all",
    q: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    sort: Optional[str] = None,
    history: int = 0,
):
    src = ALIAS_MAP.get(source.lower(), source.lower()) if source else "all"
    items = (
        list(load_history_stream())
        if history and ENABLE_HISTORY
        else load_current_snapshot()
    )
    if src != "all":
        items = [
            x for x in items if x.get("platform") and src in x.get("platform").lower()
        ]
    if q:
        ql = q.lower()
        items = [x for x in items if ql in (x.get("workflow") or "").lower()]
    if sort == "views":
        items.sort(
            key=lambda x: (x.get("popularity_metrics") or {}).get("views", 0) or 0,
            reverse=True,
        )
    elif sort == "likes":
        items.sort(
            key=lambda x: (x.get("popularity_metrics") or {}).get("likes", 0) or 0,
            reverse=True,
        )
    elif sort == "comments":
        items.sort(
            key=lambda x: (x.get("popularity_metrics") or {}).get("comments", 0) or 0,
            reverse=True,
        )
    elif sort == "upvotes":
        items.sort(
            key=lambda x: (x.get("popularity_metrics") or {}).get("upvotes", 0) or 0,
            reverse=True,
        )
    elif sort == "score":
        items.sort(key=lambda x: x.get("score", 0) or 0, reverse=True)
    elif sort == "date":
        items.sort(key=lambda x: x.get("collected_at", ""), reverse=True)
    elif sort == "random":
        import random as _r

        _r.shuffle(items)
    total = len(items)
    sliced = items[offset : offset + limit]
    return {"total": total, "data": sliced, "limit": limit, "offset": offset}


@app.get("/export/json")
async def export_json(history: int = 0):
    if history and ENABLE_HISTORY and HISTORY_FILE.exists():

        async def stream_hist():
            yield "["
            first = True
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if not first:
                        yield ","
                    else:
                        first = False
                    yield line
            yield "]"

        from fastapi.responses import StreamingResponse

        return StreamingResponse(stream_hist(), media_type="application/json")
    return load_current_snapshot()


@app.get("/export/csv")
async def export_csv(history: int = 0):
    rows: Iterable[Dict[str, Any]] = (
        load_history_stream()
        if (history and ENABLE_HISTORY)
        else load_current_snapshot()
    )
    from fastapi.responses import StreamingResponse

    def gen():
        metric_keys = set()
        buf = []
        for i, r in enumerate(rows):
            pm = r.get("popularity_metrics") or {}
            for k in pm.keys():
                metric_keys.add(k)
            buf.append(r)
            if i > 5000:
                break
        metric_cols = sorted(metric_keys)
        header = [
            "id",
            "run_id",
            "workflow",
            "platform",
            "source_id",
            "url",
            "collected_at",
            "score",
        ] + [f"metric_{m}" for m in metric_cols]
        yield ",".join(header) + "\n"
        for rec in buf:
            pm = rec.get("popularity_metrics") or {}
            row = [
                rec.get("id", ""),
                rec.get("run_id", ""),
                json.dumps(rec.get("workflow", ""))[1:-1],
                rec.get("platform", ""),
                rec.get("source_id", ""),
                rec.get("url", ""),
                rec.get("collected_at", ""),
                str(rec.get("score", "")),
            ]
            for m in metric_cols:
                row.append(str(pm.get(m, "")))
            yield ",".join(c.replace("\n", " ") for c in row) + "\n"

    return StreamingResponse(gen(), media_type="text/csv")


if __name__ == "__main__":
    import uvicorn

    print(f"Starting server with Pydantic {PYDANTIC_VERSION}")
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    try:
        uvicorn.run(app, host=host, port=port)
    except OSError as e:
        if getattr(e, "errno", None) == 10048:
            print(f"Port {port} in use. Set PORT env var to a free port, e.g. 8001.")
    # Removed stray raise to allow normal shutdown
