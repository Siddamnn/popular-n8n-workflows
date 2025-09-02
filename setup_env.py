#!/usr/bin/env python3
"""
Environment Setup Helper for n8n Popularity Collector
This script helps you configure your .env file with API keys.
"""

import os
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # python-dotenv not installed, read .env file manually
    pass


def setup_environment():
    """Interactive setup for environment variables."""
    print("=" * 60)
    print("  n8n Popularity Collector - Environment Setup")
    print("=" * 60)
    print()

    env_file = Path(".env")
    env_example = Path(".env.example")

    print("📁 Environment files:")
    print(
        f"   • .env.example (template): {'✅ Found' if env_example.exists() else '❌ Missing'}"
    )
    print(
        f"   • .env (your config): {'✅ Found' if env_file.exists() else '❌ Missing'}"
    )
    print()

    if not env_file.exists():
        print("Creating .env file from template...")
        if env_example.exists():
            # Copy from example
            with open(env_example) as f:
                content = f.read()
            with open(env_file, "w") as f:
                f.write(content)
        print("✅ Created .env file")

    print("🔧 Current environment configuration:")

    # Read current .env values
    current_values = {}
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    current_values[key.strip()] = value.strip()

    # Show current values
    important_vars = [
        ("YOUTUBE_API_KEY", "YouTube Data API v3 Key"),
        ("GITHUB_TOKEN", "GitHub Personal Access Token"),
        ("RUN_MODE", "Run Mode (live/demo)"),
        ("CONCURRENCY", "Max Concurrent Operations"),
        ("CACHE_TTL", "Cache TTL (seconds)"),
    ]

    for var, description in important_vars:
        value = current_values.get(var, "NOT SET")
        if value and var in ["YOUTUBE_API_KEY", "GITHUB_TOKEN"]:
            # Mask API keys
            if len(value) > 8:
                masked_value = value[:4] + "*" * (len(value) - 8) + value[-4:]
            else:
                masked_value = "*" * len(value) if value else "NOT SET"
            print(f"   • {var}: {masked_value}")
        else:
            print(f"   • {var}: {value}")

    print()
    print("🔑 To add your API keys:")
    print("   1. Edit the .env file with a text editor")
    print("   2. Add your API keys after the = signs")
    print("   3. Save the file")
    print()

    print("📋 How to get API keys:")
    print()
    print("   🎥 YouTube API Key:")
    print("      1. Go to https://console.cloud.google.com/")
    print("      2. Create a new project or select existing")
    print("      3. Enable 'YouTube Data API v3'")
    print("      4. Go to 'Credentials' and create an API Key")
    print("      5. Copy the key to YOUTUBE_API_KEY in .env")
    print()

    print("   🐙 GitHub Token:")
    print("      1. Go to https://github.com/settings/tokens")
    print("      2. Generate new token (classic)")
    print("      3. Select 'public_repo' scope")
    print("      4. Copy the token to GITHUB_TOKEN in .env")
    print()

    print("   ⚙️ Configuration:")
    print("      • RUN_MODE=live (use real APIs) or demo (use mock data)")
    print("      • CONCURRENCY=10 (max concurrent requests)")
    print("      • CACHE_TTL=3600 (cache for 1 hour)")
    print()

    print("✨ After setup, run:")
    print("   python demo.py  (test functionality)")
    print("   OR")
    print("   uvicorn app:app --host 127.0.0.1 --port 8000  (start server)")
    print()

    # Test current environment
    print("🧪 Testing current environment:")
    youtube_key = os.getenv("YOUTUBE_API_KEY")
    github_token = os.getenv("GITHUB_TOKEN")
    run_mode = os.getenv("RUN_MODE", "demo")

    if youtube_key:
        print("   ✅ YouTube API key found")
    else:
        print("   ⚠️  YouTube API key not set (will use demo mode)")

    if github_token:
        print("   ✅ GitHub token found")
    else:
        print("   ⚠️  GitHub token not set (will use demo mode or lower rate limits)")

    print(f"   📊 Run mode: {run_mode}")

    if not youtube_key and not github_token:
        print("   💡 App will run in demo mode with sample data")
    elif run_mode == "demo":
        print("   💡 App will run in demo mode (change RUN_MODE=live to use APIs)")
    else:
        print("   🚀 App ready for live API mode!")


if __name__ == "__main__":
    setup_environment()
