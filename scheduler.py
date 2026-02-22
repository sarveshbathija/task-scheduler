#!/usr/bin/env python3
"""
Task Scheduler - General-purpose scheduled task runner.

A standalone, config-driven scheduler that runs tasks at specified times.
Tasks are loaded from a JSON config file (default: /app/tasks.json), so any
container can mount its own task definitions without modifying this code.

Supports two scheduling formats:
  1. Cron expressions (preferred):
     "schedule": "*/10 6-13 * * 1-5"   # standard 5-field cron
  2. Legacy fixed-time (backward compatible):
     "hour": 1, "minute": 0, "days": "daily"

Supports two task types:
  1. "command" (default) - runs a subprocess
  2. "http" - makes an HTTP request to a URL

Config format (tasks.json):
[
    {
        "name": "clear_red_watchlist",
        "schedule": "0 1 * * *",
        "type": "command",
        "command": ["python3", "/app/scripts/clear_tv_watchlist.py", "--clear-tracker"],
        "timeout": 60
    },
    {
        "name": "extract_levels",
        "schedule": "*/10 6-13 * * 1-5",
        "type": "http",
        "http": {
            "method": "POST",
            "url": "http://backendv2:8500/tos-generate",
            "body": {"days": 1},
            "headers": {"Content-Type": "application/json"},
            "expected_status": [200]
        },
        "timeout": 120
    }
]
"""

import json
import os
import sys
import time
import logging
import subprocess
from datetime import datetime
from pathlib import Path

import pytz
import requests
from croniter import croniter

# Configure logging - log file path configurable for testing outside Docker
LOG_FILE = os.environ.get('TASK_SCHEDULER_LOG', '/var/log/task-scheduler.log')

handlers = [logging.StreamHandler(sys.stdout)]
try:
    handlers.append(logging.FileHandler(LOG_FILE))
except (PermissionError, FileNotFoundError):
    pass  # Skip file handler when running outside Docker (e.g. tests)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=handlers,
)
logger = logging.getLogger(__name__)

PACIFIC_TZ = pytz.timezone('America/Los_Angeles')

# Default config path; override with TASK_CONFIG env var
DEFAULT_CONFIG_PATH = '/app/tasks.json'

# Legacy format required fields
LEGACY_REQUIRED_FIELDS = {"name", "hour", "minute", "days", "command"}
VALID_DAYS = {"daily", "weekdays"}

# Valid task types
VALID_TASK_TYPES = {"command", "http"}


def load_tasks(config_path=None):
    """Load tasks from a JSON config file.

    Args:
        config_path: Path to the JSON config. Falls back to TASK_CONFIG env
                     var, then DEFAULT_CONFIG_PATH.

    Returns:
        List of validated task dicts.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config is invalid.
    """
    path = config_path or os.environ.get('TASK_CONFIG', DEFAULT_CONFIG_PATH)
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Task config not found: {path}")

    with open(path) as f:
        tasks = json.load(f)

    if not isinstance(tasks, list):
        raise ValueError(f"Task config must be a JSON array, got {type(tasks).__name__}")

    for task in tasks:
        validate_task(task)

    return tasks


def _is_cron_task(task):
    """Check if a task uses cron scheduling (vs legacy format)."""
    return "schedule" in task


def validate_task(task):
    """Validate a task configuration dict.

    Raises:
        ValueError: If task is missing required fields or has invalid values.
    """
    if not isinstance(task, dict):
        raise ValueError(f"Each task must be a dict, got {type(task).__name__}")

    if "name" not in task:
        raise ValueError("Task missing required field: name")

    task_type = task.get("type", "command")
    if task_type not in VALID_TASK_TYPES:
        raise ValueError(f"Task '{task['name']}' has invalid type: {task_type} (must be one of {VALID_TASK_TYPES})")

    if _is_cron_task(task):
        _validate_cron_task(task)
    else:
        _validate_legacy_task(task)

    if task_type == "http":
        _validate_http_task(task)
    elif task_type == "command":
        if "command" not in task:
            raise ValueError(f"Task '{task['name']}' of type 'command' requires a 'command' field")
        if not isinstance(task["command"], list) or not task["command"]:
            raise ValueError(f"Task '{task['name']}' command must be a non-empty list")


def _validate_cron_task(task):
    """Validate a task with cron schedule format."""
    schedule = task["schedule"]
    if not isinstance(schedule, str):
        raise ValueError(f"Task '{task['name']}' schedule must be a string")

    if not croniter.is_valid(schedule):
        raise ValueError(f"Task '{task['name']}' has invalid cron expression: {schedule}")


def _validate_legacy_task(task):
    """Validate a task with legacy hour/minute/days format."""
    missing = LEGACY_REQUIRED_FIELDS - set(task.keys())
    if missing:
        raise ValueError(f"Task '{task.get('name', '?')}' missing fields: {missing}")

    if task["days"] not in VALID_DAYS:
        raise ValueError(f"Task '{task['name']}' has invalid days: {task['days']} (must be {VALID_DAYS})")

    if not (0 <= task["hour"] <= 23):
        raise ValueError(f"Task '{task['name']}' has invalid hour: {task['hour']}")

    if not (0 <= task["minute"] <= 59):
        raise ValueError(f"Task '{task['name']}' has invalid minute: {task['minute']}")


def _validate_http_task(task):
    """Validate HTTP-specific task configuration."""
    if "http" not in task:
        raise ValueError(f"Task '{task['name']}' of type 'http' requires an 'http' config object")

    http = task["http"]
    if not isinstance(http, dict):
        raise ValueError(f"Task '{task['name']}' http config must be a dict")

    if "url" not in http:
        raise ValueError(f"Task '{task['name']}' http config missing required field: url")

    method = http.get("method", "GET").upper()
    if method not in {"GET", "POST", "PUT", "DELETE", "PATCH"}:
        raise ValueError(f"Task '{task['name']}' has invalid HTTP method: {method}")


def should_task_run(task, now, last_runs):
    """Check if a task should run at the given time.

    Routes to cron or legacy check based on task config.

    Args:
        task: Task configuration dict.
        now: Current datetime (timezone-aware, Pacific).
        last_runs: Dict mapping task name -> last run key string.

    Returns:
        True if the task should run now.
    """
    if _is_cron_task(task):
        return _should_run_cron(task, now, last_runs)
    else:
        return _should_run_legacy(task, now, last_runs)


def _should_run_cron(task, now, last_runs):
    """Check if a cron-scheduled task should run now.

    Uses croniter to check if the current minute matches the cron expression.
    Dedup key uses minute-level precision (YYYY-MM-DD HH:MM).
    """
    cron = croniter(task["schedule"], now)
    prev_match = cron.get_prev(datetime)

    # Check if prev_match falls within the current minute
    diff = (now - prev_match).total_seconds()
    if diff >= 60:
        return False

    # Dedup: prevent running twice in the same minute
    run_key = now.strftime("%Y-%m-%d %H:%M")
    if last_runs.get(task["name"]) == run_key:
        return False

    return True


def _should_run_legacy(task, now, last_runs):
    """Check if a legacy-scheduled task should run now.

    Original logic: checks hour, minute, and day-of-week.
    Dedup key uses hour-level precision (YYYY-MM-DD HH).
    """
    if task["days"] == "weekdays" and now.weekday() > 4:
        return False

    if now.hour != task["hour"] or now.minute != task["minute"]:
        return False

    run_key = now.strftime("%Y-%m-%d %H")
    if last_runs.get(task["name"]) == run_key:
        return False

    return True


def run_task(task):
    """Execute a command-type scheduled task.

    Args:
        task: Task configuration dict with name, command, timeout.

    Returns:
        Return code (0 for success).
    """
    logger.info(f"[{task['name']}] Executing: {' '.join(task['command'])}")

    try:
        result = subprocess.run(
            task["command"],
            capture_output=True,
            text=True,
            timeout=task.get("timeout", 120),
        )

        if result.returncode == 0:
            logger.info(f"[{task['name']}] Completed successfully")
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    logger.info(f"[{task['name']}] {line}")
        else:
            logger.error(f"[{task['name']}] Failed with exit code {result.returncode}")
            if result.stderr:
                for line in result.stderr.strip().split('\n'):
                    logger.error(f"[{task['name']}] {line}")

        return result.returncode

    except subprocess.TimeoutExpired:
        logger.error(f"[{task['name']}] Timed out after {task.get('timeout', 120)}s")
        return 1
    except Exception as e:
        logger.error(f"[{task['name']}] Error: {e}")
        return 1


def run_http_task(task):
    """Execute an HTTP-type scheduled task.

    Args:
        task: Task configuration dict with name, http config, timeout.

    Returns:
        0 on success (status in expected_status list), 1 on failure.
    """
    http = task["http"]
    method = http.get("method", "GET").upper()
    url = http["url"]
    headers = http.get("headers", {})
    body = http.get("body", None)
    expected_status = http.get("expected_status", [200])
    timeout = task.get("timeout", 120)

    logger.info(f"[{task['name']}] HTTP {method} {url}")

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=body if body else None,
            timeout=timeout,
        )

        if response.status_code in expected_status:
            logger.info(f"[{task['name']}] HTTP {response.status_code} OK")
            preview = response.text[:200] if response.text else ""
            if preview:
                logger.info(f"[{task['name']}] Response: {preview}")
            return 0
        else:
            logger.error(f"[{task['name']}] HTTP {response.status_code} (expected {expected_status})")
            preview = response.text[:500] if response.text else ""
            if preview:
                logger.error(f"[{task['name']}] Response: {preview}")
            return 1

    except requests.Timeout:
        logger.error(f"[{task['name']}] HTTP request timed out after {timeout}s")
        return 1
    except requests.ConnectionError as e:
        logger.error(f"[{task['name']}] HTTP connection error: {e}")
        return 1
    except Exception as e:
        logger.error(f"[{task['name']}] HTTP error: {e}")
        return 1


def _get_dedup_key(task, now):
    """Get the deduplication key for a task run.

    Cron tasks use minute-level precision, legacy tasks use hour-level.
    """
    if _is_cron_task(task):
        return now.strftime("%Y-%m-%d %H:%M")
    else:
        return now.strftime("%Y-%m-%d %H")


def _format_schedule(task):
    """Format a task's schedule for logging."""
    if _is_cron_task(task):
        return f"cron({task['schedule']})"
    else:
        days_label = "every day" if task["days"] == "daily" else "weekdays only"
        return f"{task['hour']:02d}:{task['minute']:02d} Pacific, {days_label}"


def main():
    """Main scheduler loop."""
    logger.info("Task Scheduler started")

    tasks = load_tasks()

    logger.info(f"Loaded {len(tasks)} task(s):")
    for task in tasks:
        task_type = task.get("type", "command")
        desc = f" — {task['description']}" if task.get("description") else ""
        logger.info(f"  - {task['name']}: {_format_schedule(task)} [{task_type}]{desc}")

    last_runs = {}

    while True:
        try:
            now = datetime.now(PACIFIC_TZ)

            for task in tasks:
                if should_task_run(task, now, last_runs):
                    logger.info(f"Triggering '{task['name']}' at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

                    task_type = task.get("type", "command")
                    if task_type == "http":
                        run_http_task(task)
                    else:
                        run_task(task)

                    last_runs[task["name"]] = _get_dedup_key(task, now)

            # Log status at the top of every hour
            if now.minute == 0 and now.second < 30:
                logger.info(f"Status: {now.strftime('%Y-%m-%d %H:%M %Z')} - {len(tasks)} tasks registered")

            time.sleep(30)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)
            time.sleep(60)


if __name__ == '__main__':
    main()
