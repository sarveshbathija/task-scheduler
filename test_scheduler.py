#!/usr/bin/env python3
"""Tests for task-scheduler/scheduler.py"""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import pytz

from scheduler import (
    should_task_run,
    run_task,
    load_tasks,
    validate_task,
    PACIFIC_TZ,
)


# --- Fixtures ---

@pytest.fixture
def sample_task():
    return {
        "name": "test_task",
        "hour": 1,
        "minute": 0,
        "days": "daily",
        "command": ["echo", "hello"],
        "timeout": 10,
    }


@pytest.fixture
def weekday_task():
    return {
        "name": "weekday_only",
        "hour": 9,
        "minute": 30,
        "days": "weekdays",
        "command": ["echo", "weekday"],
        "timeout": 10,
    }


@pytest.fixture
def tasks_json(tmp_path):
    """Write a tasks.json to a temp dir and return the path."""
    def _write(tasks):
        path = tmp_path / "tasks.json"
        path.write_text(json.dumps(tasks))
        return str(path)
    return _write


def make_pacific_time(year=2026, month=2, day=19, hour=1, minute=0):
    """Create a timezone-aware Pacific datetime."""
    dt = PACIFIC_TZ.localize(datetime(year, month, day, hour, minute, 0))
    return dt


# --- should_task_run tests ---

class TestShouldTaskRun:
    def test_runs_at_correct_time(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        assert should_task_run(sample_task, now, {}) is True

    def test_does_not_run_at_wrong_hour(self, sample_task):
        now = make_pacific_time(hour=2, minute=0)
        assert should_task_run(sample_task, now, {}) is False

    def test_does_not_run_at_wrong_minute(self, sample_task):
        now = make_pacific_time(hour=1, minute=5)
        assert should_task_run(sample_task, now, {}) is False

    def test_prevents_duplicate_run_same_hour(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        last_runs = {"test_task": "2026-02-19 01"}
        assert should_task_run(sample_task, now, last_runs) is False

    def test_allows_run_different_hour(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        last_runs = {"test_task": "2026-02-19 00"}
        assert should_task_run(sample_task, now, last_runs) is True

    def test_allows_run_different_day(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        last_runs = {"test_task": "2026-02-18 01"}
        assert should_task_run(sample_task, now, last_runs) is True

    def test_daily_runs_on_weekends(self, sample_task):
        # Feb 21, 2026 is a Saturday
        now = make_pacific_time(month=2, day=21, hour=1, minute=0)
        assert should_task_run(sample_task, now, {}) is True

    def test_weekday_task_runs_on_weekday(self, weekday_task):
        # Feb 19, 2026 is a Thursday
        now = make_pacific_time(hour=9, minute=30)
        assert should_task_run(weekday_task, now, {}) is True

    def test_weekday_task_skips_saturday(self, weekday_task):
        # Feb 21, 2026 is a Saturday
        now = make_pacific_time(month=2, day=21, hour=9, minute=30)
        assert should_task_run(weekday_task, now, {}) is False

    def test_weekday_task_skips_sunday(self, weekday_task):
        # Feb 22, 2026 is a Sunday
        now = make_pacific_time(month=2, day=22, hour=9, minute=30)
        assert should_task_run(weekday_task, now, {}) is False

    def test_unrelated_task_last_run_does_not_affect(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        last_runs = {"other_task": "2026-02-19 01"}
        assert should_task_run(sample_task, now, last_runs) is True


# --- run_task tests ---

class TestRunTask:
    @patch('scheduler.subprocess.run')
    def test_successful_execution(self, mock_run, sample_task):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Task completed\n",
            stderr=""
        )
        result = run_task(sample_task)
        assert result == 0
        mock_run.assert_called_once_with(
            ["echo", "hello"],
            capture_output=True,
            text=True,
            timeout=10,
        )

    @patch('scheduler.subprocess.run')
    def test_failed_execution(self, mock_run, sample_task):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Something went wrong\n"
        )
        result = run_task(sample_task)
        assert result == 1

    @patch('scheduler.subprocess.run')
    def test_timeout_handling(self, mock_run, sample_task):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="echo", timeout=10)
        result = run_task(sample_task)
        assert result == 1

    @patch('scheduler.subprocess.run')
    def test_exception_handling(self, mock_run, sample_task):
        mock_run.side_effect = OSError("Command not found")
        result = run_task(sample_task)
        assert result == 1

    @patch('scheduler.subprocess.run')
    def test_uses_default_timeout(self, mock_run):
        task = {
            "name": "no_timeout",
            "command": ["echo", "test"],
        }
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        run_task(task)
        mock_run.assert_called_once_with(
            ["echo", "test"],
            capture_output=True,
            text=True,
            timeout=120,
        )


# --- load_tasks tests ---

class TestLoadTasks:
    def test_loads_valid_config(self, tasks_json):
        tasks = [
            {"name": "t1", "hour": 0, "minute": 0, "days": "daily", "command": ["echo", "hi"]},
        ]
        path = tasks_json(tasks)
        loaded = load_tasks(path)
        assert len(loaded) == 1
        assert loaded[0]["name"] == "t1"

    def test_loads_multiple_tasks(self, tasks_json):
        tasks = [
            {"name": "t1", "hour": 0, "minute": 0, "days": "daily", "command": ["echo", "1"]},
            {"name": "t2", "hour": 12, "minute": 30, "days": "weekdays", "command": ["echo", "2"]},
        ]
        path = tasks_json(tasks)
        loaded = load_tasks(path)
        assert len(loaded) == 2

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_tasks("/nonexistent/tasks.json")

    def test_non_array_raises(self, tasks_json):
        path = tasks_json({"name": "not_an_array"})
        with pytest.raises(ValueError, match="JSON array"):
            load_tasks(path)

    def test_loads_from_env_var(self, tasks_json, monkeypatch):
        tasks = [
            {"name": "env_task", "hour": 5, "minute": 0, "days": "daily", "command": ["echo"]},
        ]
        path = tasks_json(tasks)
        monkeypatch.setenv("TASK_CONFIG", path)
        loaded = load_tasks()
        assert loaded[0]["name"] == "env_task"

    def test_loads_bundled_tasks_json(self):
        """The bundled tasks.json in the repo should be valid."""
        bundled = Path(__file__).parent / "tasks.json"
        if bundled.exists():
            loaded = load_tasks(str(bundled))
            assert len(loaded) >= 1


# --- validate_task tests ---

class TestValidateTask:
    def test_valid_task_passes(self):
        task = {"name": "t", "hour": 0, "minute": 0, "days": "daily", "command": ["echo"]}
        validate_task(task)  # Should not raise

    def test_missing_name_raises(self):
        task = {"hour": 0, "minute": 0, "days": "daily", "command": ["echo"]}
        with pytest.raises(ValueError, match="missing fields"):
            validate_task(task)

    def test_invalid_days_raises(self):
        task = {"name": "t", "hour": 0, "minute": 0, "days": "monthly", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid days"):
            validate_task(task)

    def test_invalid_hour_raises(self):
        task = {"name": "t", "hour": 25, "minute": 0, "days": "daily", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid hour"):
            validate_task(task)

    def test_invalid_minute_raises(self):
        task = {"name": "t", "hour": 0, "minute": 61, "days": "daily", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid minute"):
            validate_task(task)

    def test_empty_command_raises(self):
        task = {"name": "t", "hour": 0, "minute": 0, "days": "daily", "command": []}
        with pytest.raises(ValueError, match="non-empty list"):
            validate_task(task)

    def test_string_command_raises(self):
        task = {"name": "t", "hour": 0, "minute": 0, "days": "daily", "command": "echo hi"}
        with pytest.raises(ValueError, match="non-empty list"):
            validate_task(task)

    def test_non_dict_raises(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_task("not a dict")
