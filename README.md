# Task Scheduler

A standalone, config-driven task scheduler that runs scheduled jobs using cron expressions. Runs as its own Docker Compose service.

## Tasks

| Task | Schedule | What it does |
|------|----------|-------------|
| `clear_red_watchlist` | `0 1 * * *` (1am daily) | Clears TradingView red watchlist tracker |
| `backup_teslamate` | `0 2 * * *` (2am daily) | pg_dump TeslaMate PostgreSQL → Google Drive |
| `backup_stocks` | `59 23 * * *` (11:59pm daily) | mysqldump stocks MySQL → Google Drive |

## Quick Start

```bash
docker compose up -d --build
docker logs -f task-scheduler
```

## Configuration

Tasks are defined in `tasks.json`. Two scheduling formats supported:

**Cron (preferred):**
```json
{
    "name": "my_task",
    "description": "What this task does",
    "schedule": "*/10 6-13 * * 1-5",
    "type": "command",
    "command": ["python3", "script.py"],
    "timeout": 120
}
```

**Legacy (backward compatible):**
```json
{
    "name": "my_task",
    "hour": 1,
    "minute": 0,
    "days": "daily",
    "command": ["python3", "script.py"],
    "timeout": 120
}
```

### Task Types

- **`command`** — runs a subprocess (default)
- **`http`** — makes an HTTP request to a URL

### HTTP Task Example

```json
{
    "name": "trigger_api",
    "schedule": "0 5 * * 1-5",
    "type": "http",
    "http": {
        "method": "POST",
        "url": "http://service:8500/endpoint",
        "body": {"key": "value"},
        "headers": {"Content-Type": "application/json"},
        "expected_status": [200]
    },
    "timeout": 120
}
```

## Architecture

```
task-scheduler
├── scheduler.py          # Main scheduler loop (polls every 30s, Pacific timezone)
├── backup.py             # Database backup script (pg_dump/mysqldump → Google Drive)
├── tasks.json            # Task definitions (mounted as volume for hot config)
├── docker-compose.yml    # Standalone Docker Compose service
├── Dockerfile            # Python 3.11 + curl + pg_dump + mysqldump
├── requirements.txt      # croniter, pytz, requests, google-api-python-client
└── test_scheduler.py     # 73 tests
```

## Dependencies

The container includes:
- **curl** — for TradingView API calls (watchlist scripts)
- **postgresql-client** — pg_dump for TeslaMate backups
- **default-mysql-client** — mysqldump for stocks backups
- **Google API Python client** — Google Drive uploads

## Volumes

| Mount | Purpose |
|-------|---------|
| `tasks.json` | Task config (read-only, edit without rebuild) |
| `/app/scripts` | mcp-chat-server scripts |
| `/var/log` | Scheduler logs |
| `credentials.json` | Google Drive OAuth credentials |
| `teslamate_token.json` | TeslaMate Google Drive token |
| `stocks_token.json` | Stocks Google Drive token |

## Environment

Loaded from `/home/sarvesh/backups/.env`:

| Variable | Purpose |
|----------|---------|
| `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE` | TeslaMate PostgreSQL |
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Stocks MySQL |
| `TESLAMATE_DRIVE_FOLDER_ID` | Google Drive folder for TeslaMate backups |
| `STOCKS_DRIVE_FOLDER_ID` | Google Drive folder for stocks backups |

## Tests

```bash
python3 -m pytest test_scheduler.py -v
```

## Adding a New Task

1. Edit `tasks.json` — add a new task entry
2. Restart: `docker compose restart task-scheduler`

No rebuild needed since `tasks.json` is mounted as a volume.

## Backup Script

`backup.py` supports running individual jobs:

```bash
# Run specific backup
python3 backup.py --job stocks
python3 backup.py --job teslamate

# Run all backups
python3 backup.py
```
