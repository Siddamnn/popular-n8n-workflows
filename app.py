import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
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

# Scaling / target distribution configuration (new)
TARGET_TOTAL_WORKFLOWS = int(os.getenv("TARGET_TOTAL_WORKFLOWS", "20000"))
YOUTUBE_TARGET_SHARE = float(os.getenv("YOUTUBE_TARGET_SHARE", "0.4"))
GITHUB_TARGET_SHARE = float(os.getenv("GITHUB_TARGET_SHARE", "0.3"))
REDDIT_TARGET_SHARE = float(os.getenv("REDDIT_TARGET_SHARE", "0.3"))
YOUTUBE_MAX_PAGES = int(
    os.getenv("YOUTUBE_MAX_PAGES", "10")
)  # each page up to 50 videos
GITHUB_MAX_PAGES = int(
    os.getenv("GITHUB_MAX_PAGES", "10")
)  # per_page=100 => up to 1000/query
REDDIT_MAX_PAGES = int(
    os.getenv("REDDIT_MAX_PAGES", "25")
)  # limit=100 => up to 2500/query
WIDE_COLLECTION_MODE = (
    os.getenv("WIDE_COLLECTION_MODE", "0") == "1"
)  # when true allows identical titles across platforms

# Generic retry/backoff configuration
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
RETRY_BACKOFF_JITTER = float(
    os.getenv("RETRY_BACKOFF_JITTER", "0.5")
)  # max random extra seconds

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
VALID_SOURCES = ["all", *ALL_SOURCES]

# Core keyword set for all collectors (user-defined for broader coverage)
KEYWORDS_CORE = [
    "n8n workflow",
    "n8n workflows",
    "n8n automation",
    "n8n tutorial",
    "n8n",
    "n8n webhook",
    "n8n slack integration",
    "n8n gmail automation",
    "n8n google sheets",
    "n8n whatsapp",
    "n8n ai agent",
    "n8n cron",
    "n8n http request",
    "n8n discord bot",
    "n8n notion",
    "n8n airtable",
    "n8n jira",
    "n8n zapier migration",
    "best n8n workflows",
    "popular n8n workflows",
]

# Forum collection configuration (new logic)
FORUM_BASE = os.getenv("FORUM_BASE", "https://community.n8n.io")
FORUM_PAGES_LATEST = int(os.getenv("FORUM_PAGES_LATEST", "2"))
FORUM_PAGES_PER_CATEGORY = int(os.getenv("FORUM_PAGES_PER_CATEGORY", "1"))
FORUM_CATEGORY_IDS = [
    cid.strip() for cid in os.getenv("FORUM_CATEGORY_IDS", "").split(",") if cid.strip()
]
# Worker related (async version uses gather; values kept for parity / future thread usage)
FORUM_MAX_WORKERS = int(os.getenv("FORUM_MAX_WORKERS", "5"))
PIPELINE_MAX_WORKERS = int(os.getenv("PIPELINE_MAX_WORKERS", "8"))

# Runtime flags (STOP_ON_COMPLETE disabled by design now)
STOP_ON_COMPLETE = False  # Forced off per option A

# Global flag to skip further Trends queries after a 429
TRENDS_RATE_LIMITED = False
TRENDS_MAX_RETRIES = int(os.getenv("TRENDS_MAX_RETRIES", "3"))  # new
TRENDS_BACKOFF_BASE = float(os.getenv("TRENDS_BACKOFF_BASE", "5"))  # new
_GLOBAL_TRENDS_CLIENT: Optional[TrendReq] = None  # new
# Global YouTube quota/forbidden stop flag
YOUTUBE_FORBIDDEN = False

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


def make_dedupe_key(platform: str, title: str) -> str:
    """Return dedupe key; in wide collection mode keep platform separation to allow similar titles across different sources."""
    base = normalize_title(title)
    if WIDE_COLLECTION_MODE:
        return f"{platform}:{base}"
    return base


def compute_source_targets() -> Dict[str, int]:
    """Compute target item counts per source based on shares.

    Only youtube/github/reddit are scaled; other sources collect opportunistically.
    """
    remaining = max(TARGET_TOTAL_WORKFLOWS, 0)
    yt = int(remaining * YOUTUBE_TARGET_SHARE)
    gh = int(remaining * GITHUB_TARGET_SHARE)
    rd = int(remaining * REDDIT_TARGET_SHARE)
    # Adjust rounding drift by assigning leftovers to youtube
    assigned = yt + gh + rd
    if assigned < remaining:
        yt += remaining - assigned
    return {"youtube": yt, "github": gh, "reddit": rd}


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
        """Execute HTTP call with exponential backoff + jitter for transient errors.

        Retries on status codes: 429, 500, 502, 503, 504, and 403 (sometimes quota / transient block) plus network errors.
        Controlled by RETRY_* environment variables.
        """
        attempt = 0
        while attempt < RETRY_MAX_ATTEMPTS:
            try:
                return await func(*args, **kwargs)
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code in (429, 500, 502, 503, 504, 403):
                    attempt += 1
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        raise
                    backoff = RETRY_BACKOFF_BASE * (
                        RETRY_BACKOFF_FACTOR ** (attempt - 1)
                    )
                    backoff += random.uniform(0, RETRY_BACKOFF_JITTER)
                    await asyncio.sleep(backoff)
                    continue
                raise
            except (httpx.TransportError, asyncio.TimeoutError) as e:
                attempt += 1
                if attempt >= RETRY_MAX_ATTEMPTS:
                    raise
                backoff = RETRY_BACKOFF_BASE * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
                backoff += random.uniform(0, RETRY_BACKOFF_JITTER)
                await asyncio.sleep(backoff)
            except Exception:
                # Non HTTP/network error: don't spin too hard; single retry path
                attempt += 1
                if attempt >= RETRY_MAX_ATTEMPTS:
                    raise
                await asyncio.sleep(
                    RETRY_BACKOFF_BASE + random.uniform(0, RETRY_BACKOFF_JITTER)
                )

    def get_cached_or_fetch(self, cache_key: str, data: Any) -> Any:
        """Get from cache or store new data with TTL."""
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        cache.set(cache_key, data, expire=CACHE_TTL)
        return data

    async def collect_youtube_data(
        self, queries: List[str], target_count: Optional[int] = None
    ) -> List[WorkflowEntry]:
        """Collect workflow data from YouTube API with pagination until target reached."""
        if RUN_MODE == "demo" or not YOUTUBE_API_KEY:
            return self._get_demo_youtube_data()

        results: List[WorkflowEntry] = []
        target = target_count or 0
        global YOUTUBE_FORBIDDEN
        for query in queries:
            if YOUTUBE_FORBIDDEN:
                print("[youtube] Skipping remaining queries due to prior 403 flag.")
                break
            next_page: Optional[str] = None
            page = 0
            while True:
                if target and len(results) >= target:
                    break
                if page >= YOUTUBE_MAX_PAGES:
                    break
                page += 1
                async with GLOBAL_SEMAPHORE:
                    await RATE_LIMITERS["youtube"].acquire()
                    cache_key = (
                        f"youtube_{SCHEMA_VERSION}_{query}_{next_page or 'first'}"
                    )
                    cached_data = cache.get(cache_key)
                    if cached_data:
                        results.extend(cached_data)
                        if len(results) >= target and target:
                            break
                        next_page = None  # don't attempt to chain after cached page
                        continue
                    try:
                        search_url = "https://www.googleapis.com/youtube/v3/search"
                        search_params = {
                            "part": "snippet",
                            "q": query,
                            "type": "video",
                            "maxResults": 50,
                            "key": YOUTUBE_API_KEY,
                            "regionCode": "US",
                        }
                        if next_page:
                            search_params["pageToken"] = next_page
                        search_response = await self.collect_with_retry(
                            self.client.get, search_url, params=search_params
                        )
                        search_response.raise_for_status()
                        search_data = search_response.json()
                        next_page = search_data.get("nextPageToken")
                        video_ids = [
                            item["id"]["videoId"]
                            for item in search_data.get("items", [])
                        ]
                        if not video_ids:
                            break
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
                        stats_by_id = {
                            item["id"]: item["statistics"]
                            for item in stats_data.get("items", [])
                        }
                        page_results: List[WorkflowEntry] = []
                        for item in search_data.get("items", []):
                            video_id = item["id"]["videoId"]
                            title = item["snippet"]["title"]
                            key = make_dedupe_key("YouTube", title)
                            if key in self.seen_titles:
                                continue
                            self.seen_titles.add(key)
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
                            page_results.append(entry)
                        cache.set(cache_key, page_results, expire=CACHE_TTL)
                        results.extend(page_results)
                        if not next_page:
                            break
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 403:
                            YOUTUBE_FORBIDDEN = True
                            print(
                                f"[youtube] 403 encountered for query '{query}' page {page}; setting stop flag."
                            )
                        print(
                            f"Error collecting YouTube data for query '{query}' page {page}: {e}"
                        )
                        break
                    except Exception as e:
                        print(
                            f"Error collecting YouTube data for query '{query}' page {page}: {e}"
                        )
                        break
        return results

    async def collect_github_data(
        self, queries: List[str], target_count: Optional[int] = None
    ) -> List[WorkflowEntry]:
        """Collect workflow data from GitHub API with pagination until target reached."""
        if RUN_MODE == "demo":
            return self._get_demo_github_data()
        results: List[WorkflowEntry] = []
        headers = {}
        if GITHUB_TOKEN:  # Ensure the GitHub token is used for authorization
            headers["Authorization"] = (
                f"token {GITHUB_TOKEN}"  # Added comment for clarity
            )
        target = target_count or 0
        for query in queries:
            page = 1
            while True:
                if target and len(results) >= target:
                    break
                # Respect configured max pages and GitHub 1000 result hard cap (page*per_page > 1000 triggers 422)
                if page > GITHUB_MAX_PAGES or page > 10:  # 10 * 100 = 1000 cap
                    break
                async with GLOBAL_SEMAPHORE:
                    await RATE_LIMITERS["github"].acquire()
                    params = {
                        "q": query,
                        "sort": "stars",
                        "order": "desc",
                        "per_page": 100,
                        "page": page,
                    }
                    try:
                        response = await self.collect_with_retry(
                            self.client.get,
                            "https://api.github.com/search/repositories",
                            params=params,
                            headers=headers,
                        )
                        response.raise_for_status()
                        data = response.json()
                        items = data.get("items", [])
                        page_results: List[WorkflowEntry] = []
                        for item in items:
                            title = (
                                item["name"]
                                + " - "
                                + (item.get("description", "") or "")
                            )
                            key = make_dedupe_key("GitHub", title)
                            if key in self.seen_titles:
                                continue
                            self.seen_titles.add(key)
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
                            page_results.append(entry)
                        results.extend(page_results)
                        if len(items) < 100:
                            break
                        page += 1
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 422:
                            print(
                                f"[github] Reached search cap for query '{query}' at page {page} (422). Stopping."
                            )
                            break
                        print(
                            f"Error collecting GitHub data for query '{query}' page {page}: {e}"
                        )
                        break
                    except Exception as e:
                        print(
                            f"Error collecting GitHub data for query '{query}' page {page}: {e}"
                        )
                        break
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

    async def collect_reddit_data(
        self, queries: List[str], target_count: Optional[int] = None
    ) -> List[WorkflowEntry]:
        """Collect workflow data from Reddit with pagination (after cursor)."""
        if RUN_MODE == "demo":
            return self._get_demo_reddit_data()
        results: List[WorkflowEntry] = []
        subreddits = ["n8n", "automation", "workflows"]
        target = target_count or 0
        for subreddit in subreddits:
            for query in queries:
                after: Optional[str] = None
                page = 0
                while True:
                    if target and len(results) >= target:
                        break
                    if page >= REDDIT_MAX_PAGES:
                        break
                    page += 1
                    async with GLOBAL_SEMAPHORE:
                        await RATE_LIMITERS["reddit"].acquire()
                        params = {
                            "q": query,
                            "sort": "top",
                            "limit": 100,
                            "restrict_sr": "true",
                        }
                        if after:
                            params["after"] = after
                        try:
                            response = await self.collect_with_retry(
                                self.client.get,
                                f"https://www.reddit.com/r/{subreddit}/search.json",
                                params=params,
                                headers={"User-Agent": "n8n-popularity-collector/1.0"},
                            )
                            response.raise_for_status()
                            data = response.json()
                            after = data.get("data", {}).get("after")
                            children = data.get("data", {}).get("children", [])
                            page_results: List[WorkflowEntry] = []
                            for item in children:
                                post_data = item.get("data", {})
                                title = post_data.get("title", "")
                                key = make_dedupe_key("Reddit", title)
                                if key in self.seen_titles:
                                    continue
                                self.seen_titles.add(key)
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
                                page_results.append(entry)
                            results.extend(page_results)
                            if not after:
                                break
                        except Exception as e:
                            print(
                                f"Error collecting Reddit data for subreddit '{subreddit}' query '{query}' page {page}: {e}"
                            )
                            break
        return results

    async def collect_forum_data(
        self,
        pages_latest: int = FORUM_PAGES_LATEST,
        pages_per_category: int = FORUM_PAGES_PER_CATEGORY,
        category_ids: Optional[List[str]] = None,
    ) -> List[WorkflowEntry]:
        """Collect forum topics using thread pool logic for higher volume (replicates provided logic)."""
        category_ids = category_ids if category_ids is not None else FORUM_CATEGORY_IDS
        import httpx as _hx

        headers_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/125.0",
        ]

        def fetch_discourse_page(url: str) -> Dict[str, Any]:
            ua = random.choice(headers_pool)
            try:
                with _hx.Client(timeout=20) as c:
                    resp = c.get(url, headers={"User-Agent": ua})
                if resp.status_code != 200:
                    return {}
                return resp.json()
            except Exception:
                return {}

        def page_to_entries(topics: List[Dict[str, Any]]) -> List[WorkflowEntry]:
            out_local: List[WorkflowEntry] = []
            for t in topics:
                title = t.get("title") or ""
                norm = normalize_title(title)
                if norm in self.seen_titles:
                    continue
                self.seen_titles.add(norm)
                views = t.get("views", 0)
                likes = t.get("like_count", 0)
                replies = t.get("reply_count", 0)
                posts = t.get("posts_count", 0)
                entry = WorkflowEntry(
                    workflow=title,
                    platform="n8n Forum",
                    popularity_metrics={
                        "views": views,
                        "likes": likes,
                        "comments": replies,
                        "upvotes": likes,
                        "posts": posts,
                        "like_to_view_ratio": (likes / views) if views else None,
                    },
                    country="unknown",
                    source_id=str(t.get("id")),
                    url=f"{FORUM_BASE}/t/{t.get('id')}",
                    collected_at=datetime.now(timezone.utc).isoformat(),
                    id=compute_entry_id("n8n Forum", str(t.get("id")), title),
                )
                out_local.append(entry)
            return out_local

        def collect_latest(pages: int) -> List[WorkflowEntry]:
            out_entries: List[WorkflowEntry] = []
            for p in range(pages):
                url = f"{FORUM_BASE}/latest.json?page={p}"
                data = fetch_discourse_page(url)
                topics = (data.get("topic_list") or {}).get("topics", [])
                out_entries.extend(page_to_entries(topics))
            return out_entries

        def collect_category(cid: str, pages: int) -> List[WorkflowEntry]:
            out_entries: List[WorkflowEntry] = []
            for p in range(pages):
                url = f"{FORUM_BASE}/c/{cid}.json?page={p}"
                data = fetch_discourse_page(url)
                topics = (data.get("topic_list") or {}).get("topics", [])
                out_entries.extend(page_to_entries(topics))
            return out_entries

        all_entries: List[WorkflowEntry] = []
        # Thread pool for parallel category + latest fetches
        max_workers = min(
            len(category_ids) + 1 if category_ids else 1, PIPELINE_MAX_WORKERS
        )
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            futures.append(ex.submit(collect_latest, pages_latest))
            for cid in category_ids:
                futures.append(ex.submit(collect_category, cid, pages_per_category))
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res:
                        all_entries.extend(res)
                except Exception as e:
                    print(f"[forum] thread error: {e}")
                    continue
        return all_entries

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
                        if "workflow" not in title.lower():
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
    # Refined queries: ensure every search term explicitly includes 'workflow' for higher relevance
    queries = KEYWORDS_CORE[:]

    # Allow partitioning of queries across processes when MULTIPROCESS_FANOUT is set
    fanout = int(os.getenv("MULTIPROCESS_FANOUT", "1"))
    worker_index = int(os.getenv("WORKER_INDEX", "0"))
    if fanout > 1:
        queries = [q for i, q in enumerate(queries) if i % fanout == worker_index]
        if not queries:
            # Nothing for this worker; mark completed quickly
            jobs[job_id]["source_statuses"][source] = "skipped-empty-partition"
            return

    try:
        jobs[job_id]["source_statuses"][source] = "running"

        async with DataCollector() as collector:
            targets = compute_source_targets()
            if source == "youtube":
                results = await collector.collect_youtube_data(
                    queries, target_count=targets.get("youtube")
                )
            elif source == "github":
                results = await collector.collect_github_data(
                    queries, target_count=targets.get("github")
                )
            elif source == "trends":
                results = await collector.collect_trends_data(queries)
            elif source == "reddit":
                results = await collector.collect_reddit_data(
                    queries, target_count=targets.get("reddit")
                )
            elif source == "forum":
                results = await collector.collect_forum_data()
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
            # Summary statistics
            counts = jobs[job_id]["counts"]
            print("[summary][auto] job", job_id)
            for src, cnt in counts.items():
                print(f"  - {src}: {cnt}")
            print(f"  Total: {jobs[job_id]['total_collected']}")
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


SCORE_CONFIG = {
    "youtube": {
        "views": 0.45,
        "likes": 0.25,
        "comments": 0.15,
        "like_to_view_ratio": 0.15,
    },
    "reddit": {"comments": 0.571428, "upvotes": 0.428572},
    "forum": {"views": 0.4, "likes": 0.3, "comments": 0.3},
}


def compute_scores_for_source(source: str, entries: List[WorkflowEntry]):
    if not entries:
        return
    if source in SCORE_CONFIG:
        weights = SCORE_CONFIG[source]
        normalized_vectors: Dict[str, List[float]] = {}
        for metric in weights.keys():
            normalized_vectors[metric] = _normalize(
                [e.popularity_metrics.get(metric) for e in entries]
            )
        for idx, e in enumerate(entries):
            e.score = round(
                sum(weights[m] * normalized_vectors[m][idx] for m in weights), 6
            )
        return
    if source == "trends":
        interest = _normalize([e.popularity_metrics.get("views") for e in entries])
        growth = _normalize([e.popularity_metrics.get("trend_growth") for e in entries])
        for i, e in enumerate(entries):
            e.score = round(0.6 * interest[i] + 0.4 * growth[i], 6)
        return
    if source == "github":
        stars = _normalize([e.popularity_metrics.get("likes") for e in entries])
        for i, e in enumerate(entries):
            e.score = round(stars[i], 6)
        return
    if source == "search":
        for e in entries:
            e.score = 0.0
        return


@app.get("/collect", response_model=JobResponse)
async def collect_data(source: str = "all"):
    """Trigger data collection for specified source(s)."""
    valid_sources = VALID_SOURCES
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
        counts = jobs[job_id]["counts"]
        print(f"[summary][manual] job {job_id}")
        for src, cnt in counts.items():
            print(f"  - {src}: {cnt}")
        print(f"  Total: {jobs[job_id]['total_collected']}")

    global last_completed_job_id
    last_completed_job_id = job_id

    asyncio.create_task(run_collection())

    return JobResponse(job_id=job_id, started_at=started_at)


# ---------------- Parallel Multi-Process Collection ---------------- #
def _spawn_worker(env_overrides: Dict[str, str]):
    """Spawn a worker process that partitions queries using env vars.

    Worker re-imports this module, disables auto start collection, and runs a manual collection.
    """

    def runner():
        import os as _os

        for k, v in env_overrides.items():
            _os.environ[k] = v
        _os.environ["AUTO_COLLECT_ON_START"] = "0"
        import asyncio as _a
        import app as _appmod  # noqa: F401

        async def _inner():
            job_id = str(uuid.uuid4())
            started_at = datetime.now(timezone.utc).isoformat()
            srcs = _os.environ.get("PARALLEL_SOURCES", "youtube,github,reddit").split(
                ","
            )
            srcs = [s.strip() for s in srcs if s.strip()]
            if PURGE_ON_NEW_JOB and _os.environ.get("PARALLEL_PURGE", "0") == "1":
                purge_output_files(srcs)
            jobs[job_id] = {
                "job_id": job_id,
                "started_at": started_at,
                "source_statuses": {s: "pending" for s in srcs},
                "counts": {s: 0 for s in srcs},
                "total_collected": 0,
                "completed_at": None,
            }
            await asyncio.gather(*[collect_source_data(s, job_id) for s in srcs])
            jobs[job_id]["total_collected"] = sum(jobs[job_id]["counts"].values())
            jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

        _a.run(_inner())

    p = multiprocessing.Process(target=runner)
    p.start()
    return p


@app.get("/collect_parallel")
async def collect_parallel(workers: int = 2, sources: str = "youtube,github,reddit"):
    """Spawn multiple OS processes to partition queries and speed up collection.

    Each worker receives a subset of the query list based on index modulo fanout.
    Set MULTIPROCESS_FANOUT automatically; worker index via WORKER_INDEX.
    NOTE: Output files are shared; atomic writes avoid corruption but last writer wins if overlaps.
    """
    if workers < 1 or workers > 16:
        raise HTTPException(status_code=400, detail="workers must be between 1 and 16")
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    job_group_id = str(uuid.uuid4())
    processes = []
    for idx in range(workers):
        env_overrides = {
            "MULTIPROCESS_FANOUT": str(workers),
            "WORKER_INDEX": str(idx),
            "PARALLEL_SOURCES": ",".join(src_list),
            "PARALLEL_PURGE": "1" if idx == 0 else "0",
        }
        p = _spawn_worker(env_overrides)
        processes.append({"pid": p.pid, "index": idx})
    return {
        "job_group_id": job_group_id,
        "workers": workers,
        "processes": processes,
        "sources": src_list,
    }


@app.get("/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get the status of a collection job."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatus(**jobs[job_id])


@app.get("/results/{source}")
async def get_results(source: str):
    """Get results for a specific source."""
    if source not in ALL_SOURCES:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {ALL_SOURCES}"
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
    if source not in VALID_SOURCES:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {VALID_SOURCES}"
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
    SORT_MAP = {
        "views": lambda x: (x.get("popularity_metrics") or {}).get("views", 0) or 0,
        "likes": lambda x: (x.get("popularity_metrics") or {}).get("likes", 0) or 0,
        "comments": lambda x: (x.get("popularity_metrics") or {}).get("comments", 0)
        or 0,
        "upvotes": lambda x: (x.get("popularity_metrics") or {}).get("upvotes", 0) or 0,
        "score": lambda x: x.get("score", 0) or 0,
        "date": lambda x: x.get("collected_at", ""),
    }
    if sort == "random":
        import random as _r

        _r.shuffle(items)
    elif sort in SORT_MAP:
        reverse = sort != "date"
        items.sort(key=SORT_MAP[sort], reverse=reverse)
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


@app.get("/export/forum/csv")
async def export_forum_csv(history: int = 0):
    """Export forum (n8n Forum) data only as CSV.

    If history=1 and history enabled, pulls all forum records from history stream.
    Otherwise uses current snapshot output/forum.json (if present).
    """
    from fastapi.responses import StreamingResponse

    if history and ENABLE_HISTORY:
        rows_iter: Iterable[Dict[str, Any]] = (
            r
            for r in load_history_stream()
            if (r.get("platform") or "").lower().startswith("n8n forum")
        )
    else:
        # Load current forum snapshot file only
        fp = Path("./output/forum.json")
        current: List[Dict[str, Any]] = []
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    current = json.load(f)
            except Exception as e:
                print(f"[export_forum_csv] failed to read forum.json: {e}")
        rows_iter = current

    def gen_forum():
        metric_keys: Set[str] = set()
        buffered: List[Dict[str, Any]] = []
        # Materialize (forum volumes are moderate) to discover metric columns
        for i, rec in enumerate(rows_iter):
            pm = rec.get("popularity_metrics") or {}
            for k in pm.keys():
                metric_keys.add(k)
            buffered.append(rec)
            if i > 50000:
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
        for rec in buffered:
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

    return StreamingResponse(gen_forum(), media_type="text/csv")


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
