# n8n Popularity Collector

FastAPI service that aggregates community & ecosystem popularity signals for n8n workflows across multiple public sources, computes per‑source scores, stores point‑in‑time snapshots, and exposes ranking / trending / export APIs.

## ✨ Key Capabilities

- Multi‑source async collection: YouTube, GitHub, Reddit, Google Trends, n8n Forum (Discourse), Web Search (DuckDuckGo HTML scrape lite)
- Per‑source adaptive scoring & unified ranking endpoint
- Rate limiting + exponential backoff + retry (Google Trends enhanced backoff & 429 global short‑circuit)
- Disk TTL caching (diskcache) to reduce duplicate fetches
- Title normalization + hashed stable IDs (dedupe across runs)
- Atomic per‑source JSON snapshot writes (Windows safe) + optional purge on new job
- Historical append‑only JSONL log with schema versioning & run IDs
- Query endpoints: rankings, trending (filter, search, pagination, sorting, history toggle)
- Export endpoints: JSON streaming & CSV (current snapshot or full history)
- Demo mode (works with zero API keys) + fallback demo entries for empty Trends

## 🗂 Data Model (WorkflowEntry)

```
{
  id: <sha1 stable>,
  run_id: <uuid job id>,
  workflow: <human title>,
  platform: "YouTube" | "GitHub" | "Reddit" | "Google Trends" | "n8n Forum" | "Web Search",
  popularity_metrics: {
    views?, likes?, comments?, upvotes?, like_to_view_ratio?, trend_growth?
  },
  country: "US" | "unknown" | ...,
  source_id: <upstream native identifier>,
  url: <canonical link>,
  collected_at: ISO8601 UTC timestamp,
  score?: float,              # per‑source computed
  schema_version: "3"
}
```

## 🧮 Scoring Summary

| Source        | Formula                                                                            |
| ------------- | ---------------------------------------------------------------------------------- |
| YouTube       | 0.45*views + 0.25*likes + 0.15*comments + 0.15*like_to_view_ratio (all normalized) |
| Reddit        | 0.571428*comments + 0.428572*upvotes                                               |
| Google Trends | 0.6*avg_interest + 0.4*trend_growth                                                |
| GitHub        | normalized stars (likes)                                                           |
| n8n Forum     | 0.4*views + 0.3*likes + 0.3\*comments                                              |
| Web Search    | 0 (placeholder – no intrinsic metrics)                                             |

All component metrics individually max‑normalized within the source batch.

## 📁 Project Layout

```
app.py           # FastAPI application & collectors
code.py          # (Reference legacy ideation script - not required to run)
setup_env.py     # Helper to create .env from template
history.jsonl    # Append-only historical log (if enabled)
output/          # Per-source latest snapshots (JSON)
  youtube.json
  github.json
  reddit.json
  trends.json
  forum.json (after first forum run)
  search.json (after first search run)
README.md        # (Original) - supersede with README_NEW.md or merge
README_NEW.md    # This comprehensive documentation
requirements.txt # Dependencies
.env / .env.example
```

## ⚙️ Environment Variables

| Variable              | Default       | Purpose                                             |
| --------------------- | ------------- | --------------------------------------------------- |
| RUN_MODE              | demo          | demo or live (demo supplies mock data where needed) |
| YOUTUBE_API_KEY       | (unset)       | Enables real YouTube collection                     |
| GITHUB_TOKEN          | (unset)       | Increases GitHub rate limits                        |
| CONCURRENCY           | 10            | Global async semaphore size                         |
| CACHE_TTL             | 3600          | Seconds for per‑query cache entries                 |
| AUTO_COLLECT_ON_START | 1             | Run a full collection job at startup                |
| ENABLE_HISTORY        | 1             | Append to history.jsonl                             |
| HISTORY_FILE          | history.jsonl | Path to history log                                 |
| PURGE_ON_NEW_JOB      | 1             | Remove existing snapshot JSON before a new job      |
| FORUM_PAGES           | 2             | Pages of forum /latest.json to request              |
| SEARCH_PAGES          | 1             | Pages of DuckDuckGo HTML to parse (50 results/page) |
| TRENDS_MAX_RETRIES    | 3             | Retry attempts per query (Google Trends)            |
| TRENDS_BACKOFF_BASE   | 5             | Base seconds for exponential + jitter backoff       |

## 🚀 Quick Start

```powershell
# (Windows PowerShell)
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env  # then edit if desired
python app.py  # uvicorn embedded
```

Browse: http://127.0.0.1:8000

Auto collection kicks off (unless AUTO_COLLECT_ON_START=0). Check job list:

```
GET /jobs
GET /last_completed
```

## 🔄 Manual Collection

```
GET /collect?source=all
GET /collect?source=youtube
GET /collect?source=forum
GET /collect?source=search
```

Sources: youtube | github | reddit | trends | forum | search | all

## 📊 Core Endpoints

| Endpoint                      | Description                                   |
| ----------------------------- | --------------------------------------------- |
| /                             | Service metadata & endpoint map               |
| /collect?source=...           | Trigger async job (returns job_id)            |
| /status/{job_id}              | Live status for a job                         |
| /jobs                         | All jobs summary                              |
| /last_completed               | Most recent finished job summary              |
| /results/{source}             | Latest per‑source snapshot JSON               |
| /rankings?source=all&limit=50 | Ranked entries (score descending)             |
| /trending                     | Filter/search/sort across snapshot or history |
| /export/json?history=1        | Stream JSON (snapshot or full history)        |
| /export/csv?history=1         | CSV export (includes popularity metrics)      |

### /trending Parameters

| Param   | Meaning                                             |
| ------- | --------------------------------------------------- | ----- | -------- | ------- | ----- | ---- | ------ |
| source  | all or specific (aliases: yt, gh, git, forums, web) |
| q       | substring filter on workflow title                  |
| limit   | page size (default 100)                             |
| offset  | pagination offset                                   |
| sort    | views                                               | likes | comments | upvotes | score | date | random |
| history | 1 = search full history.jsonl, 0 = snapshot         |

## 🧵 History & Snapshots

- Each collection job writes fresh JSON snapshots (one per source) atomically.
- If history enabled, every WorkflowEntry is appended as JSONL line with run_id + schema_version for future reprocessing.
- Use history=1 on /trending or export endpoints to query entire accumulated set.

## 🛡 Resilience & Backoff

- Unified retry helper for HTTP 429/5xx (non-Trends sources)
- Trends: exponential (base TRENDS_BACKOFF_BASE) + random jitter; global flag halts remaining queries after first 429 to avoid bans.

## 🧪 Demo Mode

Without keys the app still returns deterministic demo entries (YouTube, GitHub, Reddit) and fallback Trends entries (if enabled). Forum & search still hit public endpoints unless firewalled.

## ♻️ Extending

1. Add collector method (async) to DataCollector.
2. Add rate limiter key and expand ALL_SOURCES.
3. Add branch in collect_source_data.
4. Add scoring rule in compute_scores_for_source.
5. Update root endpoint doc string(s) if needed.

## 🔐 Notes

- DuckDuckGo HTML parsing is heuristic; for production consider official APIs or a licensed search API.
- Forum crawling limited to /latest.json to stay courteous.

## 📦 Exports

- JSON: streaming array for history (constant memory)
- CSV: dynamic header derived from all metric keys encountered (first 5001 rows to bound initial memory). Adjust easily if larger exports needed.

## 🧹 Cleanup & Quality

- Atomic writes avoid partial snapshots.
- Hash IDs join same content across runs even if ordering shifts.
- Schema version SCHEMA_VERSION guards downstream consumers; bump when adding/removing fields.

## 🔮 Potential Next Steps

- Add additional sources (StackOverflow, Docker Hub pulls, Twitter/X search, npm downloads)
- Introduce scoring recompute endpoint for historical replay
- Implement history compaction / rotation policy
- Add structured logging (JSON) & metrics (Prometheus)
- Add pagination / streaming for rankings endpoint
- Refactor search collector to an adapter pattern

## ⚖️ License

MIT (add a LICENSE file if distribution intended)

## ❓ Troubleshooting

| Issue               | Resolution                                              |
| ------------------- | ------------------------------------------------------- |
| Immediate shutdown  | Ensure lifespan context replaced (already fixed)        |
| YouTube quota empty | RUN_MODE=demo or reduce queries                         |
| Trends 429 quickly  | Increase TRENDS_BACKOFF_BASE, reduce query list         |
| Empty rankings      | Wait until job completes or trigger /collect            |
| CSV missing metrics | Metrics appear only if at least one entry supplies them |

---

Generated comprehensive README_NEW.md – merge or replace existing README.md as desired.
