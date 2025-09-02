# n8n Popularity Collector

FastAPI service aggregating popularity signals for n8n workflows across multiple public sources with scoring, history, and export APIs.

## ✨ Features

- Async multi-source collection: YouTube, GitHub, Reddit, Google Trends, n8n Forum, Web Search
- Per-source scoring + unified rankings & trending
- Disk TTL caching, rate limiting, retries & Trends backoff with jitter
- Dedup (normalized title) + stable hashed IDs + run IDs
- Atomic per-source JSON snapshots + optional purge
- Historical JSONL log (schema versioned) with export (JSON stream / CSV)
- Demo mode (works without API keys) + fallback for empty Trends

## 📦 Data Model (WorkflowEntry)

```
{
  id, run_id, workflow, platform, popularity_metrics{views,likes,comments,upvotes,like_to_view_ratio,trend_growth},
  country, source_id, url, collected_at, score?, schema_version
}
```

## 🧮 Scoring

| Source        | Formula                                                                        |
| ------------- | ------------------------------------------------------------------------------ |
| YouTube       | 0.45*views + 0.25*likes + 0.15*comments + 0.15*like_to_view_ratio (normalized) |
| Reddit        | 0.571428*comments + 0.428572*upvotes                                           |
| Google Trends | 0.6*avg_interest + 0.4*trend_growth                                            |
| GitHub        | normalized stars                                                               |
| n8n Forum     | 0.4*views + 0.3*likes + 0.3\*comments                                          |
| Web Search    | 0 (placeholder)                                                                |

## 🚀 Quick Start

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
python app.py
```

Browse http://127.0.0.1:8000

## 🔄 Manual Collection

```
GET /collect?source=all|youtube|github|reddit|trends|forum|search
```

## 🔌 Core Endpoints

| Endpoint               | Description                            |
| ---------------------- | -------------------------------------- |
| /                      | Metadata & endpoint map                |
| /collect               | Trigger job (source param)             |
| /status/{job_id}       | Job status                             |
| /jobs                  | All jobs summary                       |
| /last_completed        | Last completed job                     |
| /results/{source}      | Latest snapshot for source             |
| /rankings              | Ranked entries (score desc)            |
| /trending              | Filter/search/sort snapshot or history |
| /export/json?history=1 | Stream full history JSON               |
| /export/csv?history=1  | CSV export                             |

### /trending params

source (aliases: yt, gh, git, forums, web), q, limit, offset, sort (views|likes|comments|upvotes|score|date|random), history=0/1

## ⚙️ Environment Vars

| Var                   | Default       | Purpose                     |
| --------------------- | ------------- | --------------------------- |
| RUN_MODE              | demo          | demo/live mode              |
| YOUTUBE_API_KEY       | (unset)       | Real YouTube data           |
| GITHUB_TOKEN          | (unset)       | Higher GitHub rate limit    |
| CONCURRENCY           | 10            | Global semaphore            |
| CACHE_TTL             | 3600          | Cache seconds               |
| AUTO_COLLECT_ON_START | 1             | Auto job at startup         |
| ENABLE_HISTORY        | 1             | Append to history.jsonl     |
| HISTORY_FILE          | history.jsonl | History path                |
| PURGE_ON_NEW_JOB      | 1             | Clear snapshots before job  |
| FORUM_PAGES           | 2             | Forum pages to fetch        |
| SEARCH_PAGES          | 1             | DuckDuckGo pages            |
| TRENDS_MAX_RETRIES    | 3             | Trends retry attempts       |
| TRENDS_BACKOFF_BASE   | 5             | Trends backoff base seconds |

## 🧵 History

- Snapshots: output/<source>.json (atomic write)
- History: line-delimited JSON entries (run_id & schema_version)
- Use history=1 in /trending or exports for full set

## 🛡 Resilience

- Exponential retry (non-Trends) & specialized Trends backoff + global 429 halt
- Dedup across sources via normalized title and stable IDs

## 📦 Export

- JSON streaming (constant memory)
- CSV dynamic header (first 5001 rows currently)

## ♻️ Extend

1. Add collector method & rate limiter
2. Add to ALL_SOURCES
3. Branch in collect_source_data
4. Add scoring rule
5. Update root endpoint docs

## 🔮 Next Ideas

StackOverflow, Docker Hub, npm, historical score recompute, history rotation, Prometheus metrics, pagination for rankings.

## ❓ Troubleshooting

| Issue              | Fix                                               |
| ------------------ | ------------------------------------------------- |
| Empty rankings     | Wait job completion or trigger /collect           |
| Trends 429 early   | Increase TRENDS_BACKOFF_BASE / reduce queries     |
| YouTube quota      | Switch to demo or provide API key                 |
| Immediate shutdown | Ensure using current app.py (no lifespan context) |

## License

MIT (add LICENSE file if distributing)

---

This README consolidates previous docs (README_NEW.md removed).

### Prerequisites

- Python 3.11+
- pip

### Installation

1. Clone or create the project directory:

```bash
cd n8n2
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

**Note**: If you encounter version compatibility issues with FastAPI/Pydantic, you can run the demo script instead which shows the core functionality without the web server.

### Quick Demo

To see the core functionality without dependency issues:

```bash
python demo.py
```

This will demonstrate:

- Async data collection simulation
- Title normalization and deduplication
- Atomic file writing
- JSON output generation

### Configuration

#### Quick Setup

Run the environment setup helper:

```bash
python setup_env.py
```

This will:

- Create a `.env` file from the template
- Show current configuration
- Provide instructions for getting API keys

#### Manual Setup

Set environment variables by editing the `.env` file:

```bash
# YouTube Data API v3 Key (Optional)
YOUTUBE_API_KEY=your_youtube_api_key_here

# GitHub Personal Access Token (Optional)
GITHUB_TOKEN=your_github_token_here

# Run mode: 'live' or 'demo'
RUN_MODE=live

# Application settings
CONCURRENCY=10
CACHE_TTL=3600
```

**Note**: The service works in demo mode without any API keys, returning sample data.

### Getting API Keys (Optional)

#### YouTube API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable YouTube Data API v3
4. Create credentials (API Key)
5. Copy the API key

#### GitHub Token

1. Go to GitHub Settings → Developer settings → Personal access tokens
2. Generate new token with `public_repo` scope
3. Copy the token

## Running the Service

Start the server:

```bash
uvicorn app:app --host 127.0.0.1 --port 8000
```

The service will be available at: http://127.0.0.1:8000

## API Usage

### Start Data Collection

```bash
# Collect from all sources
curl "http://127.0.0.1:8000/collect?source=all"

# Collect from specific source
curl "http://127.0.0.1:8000/collect?source=youtube"
curl "http://127.0.0.1:8000/collect?source=github"
curl "http://127.0.0.1:8000/collect?source=reddit"
curl "http://127.0.0.1:8000/collect?source=trends"
```

Response:

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "started_at": "2025-09-02T10:30:00Z"
}
```

### Check Job Status

```bash
curl "http://127.0.0.1:8000/status/550e8400-e29b-41d4-a716-446655440000"
```

Response:

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_statuses": {
    "youtube": "completed",
    "github": "running",
    "reddit": "pending",
    "trends": "completed"
  },
  "completed_at": null
}
```

### Get Results

```bash
curl "http://127.0.0.1:8000/results/youtube"
```

Response: JSON array of workflow entries (see sample files in `./output/`)

### Service Info

```bash
curl "http://127.0.0.1:8000/"
```

## Output Format

Results are saved to `./output/<source>.json` with this schema:

```json
[
  {
    "workflow": "Google Sheets → Slack",
    "platform": "YouTube",
    "popularity_metrics": {
      "views": 1250,
      "likes": 85,
      "comments": 12,
      "upvotes": null,
      "like_to_view_ratio": 0.068
    },
    "country": "US",
    "source_id": "abc123",
    "url": "https://www.youtube.com/watch?v=abc123",
    "collected_at": "2025-09-02T10:30:00Z"
  }
]
```

## Testing

This slimmed repository version has tests removed. Re-add pytest and create tests if you need automated validation.

## Architecture

### Performance Design

- **Bounded Concurrency**: Global semaphore limits concurrent operations
- **Rate Limiting**: Per-API async rate limiters prevent API abuse
- **Caching**: Disk-based cache with TTL avoids redundant API calls
- **Deduplication**: In-memory set tracks normalized titles
- **Streaming**: Results written as collected to avoid memory growth
- **Atomic Writes**: Temporary files ensure data consistency

### Source-Specific Details

#### YouTube

- Uses Data API v3 (search.list + videos.list)
- Requires API key for live mode
- Rate limit: 100 requests per 100 seconds
- Demo mode provides sample data

#### GitHub

- Uses public REST API (search/repositories)
- Higher rate limits with authentication
- Rate limit: 60/hour (auth) or 10/hour (unauth)
- Searches for repositories and issues

#### Reddit

- Uses public JSON endpoints
- No authentication required
- Rate limit: 60 requests per minute
- Searches r/n8n, r/automation, r/workflows

#### Google Trends

- Uses pytrends library
- No API key required
- Rate limit: 1 request per 5 seconds (conservative)
- Provides trend data for keywords

## Project Structure

```
Project tree (excerpt):
```

app.py
setup_env.py
history.jsonl (if enabled)
output/ (snapshots)
requirements.txt
.env / .env.example

```

```

## Environment Variables

| Variable          | Default | Description                  |
| ----------------- | ------- | ---------------------------- |
| `YOUTUBE_API_KEY` | None    | YouTube Data API v3 key      |
| `GITHUB_TOKEN`    | None    | GitHub personal access token |
| `RUN_MODE`        | `demo`  | `live` or `demo` mode        |
| `CONCURRENCY`     | `10`    | Max concurrent operations    |
| `CACHE_TTL`       | `3600`  | Cache TTL in seconds         |

## Error Handling

- **Rate Limiting**: Automatic backoff on 429 errors
- **Network Errors**: Exponential backoff on 5xx errors
- **Missing Keys**: Graceful fallback to demo mode
- **Invalid Sources**: HTTP 400 with clear error message
- **File Errors**: HTTP 500 with error details

## Scaling Considerations

The service is designed to handle both 50 and 100,000 queries efficiently:

- **Memory**: Bounded by deduplication set and streaming writes
- **Disk**: Results streamed to avoid memory growth
- **Network**: Configurable concurrency and rate limiting
- **Cache**: TTL-based disk cache reduces redundant API calls
