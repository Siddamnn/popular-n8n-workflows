#!/usr/bin/env python3
"""
Simplified demo script for n8n-popularity-collector
This script demonstrates the core functionality without running the full FastAPI server.
"""

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

# Mock data for demonstration
demo_youtube_data = [
    {
        "workflow": "Google Sheets to Slack Automation with n8n",
        "platform": "YouTube",
        "popularity_metrics": {
            "views": 15240,
            "likes": 342,
            "comments": 28,
            "upvotes": None,
            "like_to_view_ratio": 0.0224,
        },
        "country": "US",
        "source_id": "dQw4w9WgXcQ",
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    },
    {
        "workflow": "Complete n8n Workflow Tutorial for Beginners",
        "platform": "YouTube",
        "popularity_metrics": {
            "views": 8750,
            "likes": 198,
            "comments": 45,
            "upvotes": None,
            "like_to_view_ratio": 0.0226,
        },
        "country": "US",
        "source_id": "abc123xyz",
        "url": "https://www.youtube.com/watch?v=abc123xyz",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    },
]

demo_github_data = [
    {
        "workflow": "n8n-workflow-examples - A collection of n8n workflow templates",
        "platform": "GitHub",
        "popularity_metrics": {
            "views": None,
            "likes": 1245,
            "comments": None,
            "upvotes": 1245,
            "like_to_view_ratio": None,
        },
        "country": "unknown",
        "source_id": "123456789",
        "url": "https://github.com/user/n8n-workflow-examples",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
]

demo_reddit_data = [
    {
        "workflow": "Best n8n workflows for small business automation?",
        "platform": "Reddit",
        "popularity_metrics": {
            "views": None,
            "likes": None,
            "comments": 34,
            "upvotes": 156,
            "like_to_view_ratio": None,
        },
        "country": "unknown",
        "source_id": "abc123def",
        "url": "https://reddit.com/r/n8n/comments/abc123def/best_n8n_workflows_for_small_business_automation/",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
]

demo_trends_data = [
    {
        "workflow": "Google Trends: n8n workflow automation",
        "platform": "Google Trends",
        "popularity_metrics": {
            "views": 68,
            "likes": None,
            "comments": None,
            "upvotes": None,
            "like_to_view_ratio": None,
        },
        "country": "US",
        "source_id": "trends_n8n workflow automation",
        "url": "https://trends.google.com/trends/explore?q=n8n workflow automation",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
]


def normalize_title(title: str) -> str:
    """Normalize title for deduplication by removing punctuation and converting to lowercase."""
    return re.sub(r"[^\w\s]", "", title.lower().strip())


async def write_results_atomically(source: str, results: list):
    """Write results to JSON file atomically."""
    output_dir = Path("./output")
    output_dir.mkdir(exist_ok=True)

    temp_file = output_dir / f"{source}.json.tmp"
    final_file = output_dir / f"{source}.json"

    # Write to temporary file first
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Atomic rename (handle Windows file locking)
    if final_file.exists():
        final_file.unlink()
    temp_file.rename(final_file)
    print(f"✓ Written {len(results)} entries to {final_file}")


async def simulate_collection(source: str):
    """Simulate data collection for a source with some delay."""
    print(f"🔄 Collecting {source} data...")

    # Simulate API call delay
    await asyncio.sleep(0.5)

    # Get demo data based on source
    if source == "youtube":
        data = demo_youtube_data
    elif source == "github":
        data = demo_github_data
    elif source == "reddit":
        data = demo_reddit_data
    elif source == "trends":
        data = demo_trends_data
    else:
        data = []

    # Write results
    await write_results_atomically(source, data)
    return len(data)


async def collect_all_sources():
    """Collect data from all sources concurrently."""
    sources = ["youtube", "github", "reddit", "trends"]

    print("🚀 Starting n8n popularity collection demo...")
    print(f"📊 Collecting from {len(sources)} sources: {', '.join(sources)}")
    print()

    start_time = time.time()

    # Run collections concurrently
    tasks = [simulate_collection(source) for source in sources]
    results = await asyncio.gather(*tasks)

    end_time = time.time()
    total_entries = sum(results)

    print()
    print("📈 Collection Summary:")
    print(f"   • Total entries collected: {total_entries}")
    print(f"   • Time taken: {end_time - start_time:.2f} seconds")
    print(f"   • Sources completed: {len(sources)}")
    print()

    # Show deduplication example
    print("🔍 Deduplication Example:")
    titles = [
        "Google Sheets to Slack Integration",
        "Google Sheets to Slack Integration!",
        "  google sheets to slack integration  ",
        "Different n8n Workflow",
    ]

    seen_titles = set()
    duplicates_found = 0

    for title in titles:
        normalized = normalize_title(title)
        if normalized in seen_titles:
            print(f"   ❌ Duplicate: '{title}' -> '{normalized}'")
            duplicates_found += 1
        else:
            print(f"   ✅ Unique: '{title}' -> '{normalized}'")
            seen_titles.add(normalized)

    print(f"   • Duplicates filtered: {duplicates_found}")
    print(f"   • Unique titles kept: {len(seen_titles)}")
    print()

    # List generated files
    print("📁 Generated Files:")
    output_dir = Path("./output")
    for source in sources:
        file_path = output_dir / f"{source}.json"
        if file_path.exists():
            file_size = file_path.stat().st_size
            print(f"   • {file_path} ({file_size:,} bytes)")

    print()
    print("✨ Demo completed successfully!")
    print()
    print("To run the full FastAPI server (after fixing dependencies):")
    print("   uvicorn app:app --host 127.0.0.1 --port 8000")
    print()
    print("API endpoints would be:")
    print("   GET /collect?source=all")
    print("   GET /status/{job_id}")
    print("   GET /results/youtube")


def main():
    """Run the demo."""
    print("=" * 60)
    print("  n8n Popularity Collector - Demo Mode")
    print("=" * 60)
    print()

    asyncio.run(collect_all_sources())


if __name__ == "__main__":
    main()
