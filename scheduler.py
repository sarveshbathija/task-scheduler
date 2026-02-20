#!/usr/bin/env python3
"""
Task Scheduler - General-purpose scheduled task runner.

A standalone, config-driven scheduler that runs tasks at specified times.
Tasks are loaded from a JSON config file (default: /app/tasks.json), so any
container can mount its own task definitions without modifying this code.

Config format (tasks.json):
[
    {
        "name": "clear_red_watchlist",
        "hour": 1,
        "minute": 0,
        "days": "daily",
        "command": ["python3", "/app/scripts/clear_tv_watchlist.py", "--clear-tracker"],
        "timeout": 60
    }
]

Fields:
    name     - Unique task identifier (used for duplicate-run tracking)
    hour     - Hour to run (0-23, Pacific Time)
    minute   - Minute to run (0-59)
    days     - "daily" or "weekdays" (Mon-Fri)
    command  - Command as list of strings
    timeout  - Max seconds before killing the process (default: 120)
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

# Required fields for each task
REQUIRED_FIELDS = {"name", "hour", "minute", "days", "command"}
VALID_DAYS = {"daily", "weekdays"}


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


def validate_task(task):
    """Validate a task configuration dict.

    Raises:
        ValueError: If task is missing required fields or has invalid values.
    """
    if not isinstance(task, dict):
        raise ValueError(f"Each task must be a dict, got {type(task).__name__}")

    missing = REQUIRED_FIELDS - set(task.keys())
    if missing:
        raise ValueError(f"Task '{task.get('name', '?')}' missing fields: {missing}")

    if task["days"] not in VALID_DAYS:
        raise ValueError(f"Task '{task['name']}' has invalid days: {task['days']} (must be {VALID_DAYS})")

    if not (0 <= task["hour"] <= 23):
        raise ValueError(f"Task '{task['name']}' has invalid hour: {task['hour']}")

    if not (0 <= task["minute"] <= 59):
        raise ValueError(f"Task '{task['name']}' has invalid minute: {task['minute']}")

    if not isinstance(task["command"], list) or not task["command"]:
        raise ValueError(f"Task '{task['name']}' command must be a non-empty list")


def should_task_run(task, now, last_runs):
    """Check if a task should run at the given time.

    Args:
        task: Task configuration dict with hour, minute, days.
        now: Current datetime (timezone-aware, Pacific).
        last_runs: Dict mapping task name -> last run date string (YYYY-MM-DD HH).

    Returns:
        True if the task should run now.
    """
    # Check day-of-week constraint
    if task["days"] == "weekdays" and now.weekday() > 4:
        return False

    # Check hour and minute
    if now.hour != task["hour"] or now.minute != task["minute"]:
        return False

    # Check if already ran this hour (prevent duplicate runs)
    run_key = now.strftime("%Y-%m-%d %H")
    if last_runs.get(task["name"]) == run_key:
        return False

    return True


def run_task(task):
    """Execute a scheduled task.

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


def main():
    """Main scheduler loop."""
    logger.info("Task Scheduler started")

    tasks = load_tasks()

    logger.info(f"Loaded {len(tasks)} task(s):")
    for task in tasks:
        days_label = "every day" if task["days"] == "daily" else "weekdays only"
        logger.info(f"  - {task['name']}: {task['hour']:02d}:{task['minute']:02d} Pacific, {days_label}")

    last_runs = {}

    while True:
        try:
            now = datetime.now(PACIFIC_TZ)

            for task in tasks:
                if should_task_run(task, now, last_runs):
                    logger.info(f"Triggering '{task['name']}' at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    run_task(task)
                    last_runs[task["name"]] = now.strftime("%Y-%m-%d %H")

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
