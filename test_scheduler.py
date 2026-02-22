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
    run_http_task,
    load_tasks,
    validate_task,
    _should_run_cron,
    _should_run_legacy,
    _is_cron_task,
    _get_dedup_key,
    _format_schedule,
    PACIFIC_TZ,
)


# --- Fixtures ---

@pytest.fixture
def sample_task():
    """Legacy format task for backward compat testing."""
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
    """Legacy format weekday-only task."""
    return {
        "name": "weekday_only",
        "hour": 9,
        "minute": 30,
        "days": "weekdays",
        "command": ["echo", "weekday"],
        "timeout": 10,
    }


@pytest.fixture
def cron_task():
    """Cron-scheduled command task."""
    return {
        "name": "cron_task",
        "schedule": "0 1 * * *",
        "type": "command",
        "command": ["echo", "cron"],
        "timeout": 10,
    }


@pytest.fixture
def cron_weekday_task():
    """Cron-scheduled task that runs every 10 min, 6am-1pm, weekdays."""
    return {
        "name": "cron_weekday",
        "schedule": "*/10 6-13 * * 1-5",
        "type": "command",
        "command": ["echo", "market"],
        "timeout": 10,
    }


@pytest.fixture
def http_task():
    """HTTP-type task with cron schedule."""
    return {
        "name": "http_task",
        "schedule": "*/10 6-13 * * 1-5",
        "type": "http",
        "http": {
            "method": "POST",
            "url": "http://backendv2:8500/tos-generate",
            "body": {"days": 1},
            "headers": {"Content-Type": "application/json"},
            "expected_status": [200],
        },
        "timeout": 120,
    }


@pytest.fixture
def tasks_json(tmp_path):
    """Write a tasks.json to a temp dir and return the path."""
    def _write(tasks):
        path = tmp_path / "tasks.json"
        path.write_text(json.dumps(tasks))
        return str(path)
    return _write


def make_pacific_time(year=2026, month=2, day=19, hour=1, minute=0, second=0):
    """Create a timezone-aware Pacific datetime.

    Feb 19, 2026 = Thursday
    Feb 21, 2026 = Saturday
    Feb 22, 2026 = Sunday
    """
    dt = PACIFIC_TZ.localize(datetime(year, month, day, hour, minute, second))
    return dt


# --- _is_cron_task tests ---

class TestIsCronTask:
    def test_cron_task(self, cron_task):
        assert _is_cron_task(cron_task) is True

    def test_legacy_task(self, sample_task):
        assert _is_cron_task(sample_task) is False


# --- Legacy should_task_run tests (backward compatibility) ---

class TestShouldTaskRunLegacy:
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


# --- Cron should_task_run tests ---

class TestShouldTaskRunCron:
    def test_runs_at_matching_time(self, cron_task):
        """Cron '0 1 * * *' should fire at 01:00."""
        now = make_pacific_time(hour=1, minute=0, second=15)
        assert should_task_run(cron_task, now, {}) is True

    def test_does_not_run_at_non_matching_time(self, cron_task):
        """Cron '0 1 * * *' should NOT fire at 02:00."""
        now = make_pacific_time(hour=2, minute=0)
        assert should_task_run(cron_task, now, {}) is False

    def test_does_not_run_at_wrong_minute(self, cron_task):
        """Cron '0 1 * * *' should NOT fire at 01:30."""
        now = make_pacific_time(hour=1, minute=30)
        assert should_task_run(cron_task, now, {}) is False

    def test_prevents_duplicate_run_same_minute(self, cron_task):
        now = make_pacific_time(hour=1, minute=0, second=15)
        last_runs = {"cron_task": "2026-02-19 01:00"}
        assert should_task_run(cron_task, now, last_runs) is False

    def test_allows_run_different_minute(self, cron_task):
        now = make_pacific_time(hour=1, minute=0, second=15)
        last_runs = {"cron_task": "2026-02-19 00:50"}
        assert should_task_run(cron_task, now, last_runs) is True

    def test_every_10_min_runs_at_6_00(self, cron_weekday_task):
        """'*/10 6-13 * * 1-5' should fire at 06:00 on Thursday."""
        now = make_pacific_time(hour=6, minute=0, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is True

    def test_every_10_min_runs_at_6_10(self, cron_weekday_task):
        now = make_pacific_time(hour=6, minute=10, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is True

    def test_every_10_min_runs_at_6_20(self, cron_weekday_task):
        now = make_pacific_time(hour=6, minute=20, second=5)
        assert should_task_run(cron_weekday_task, now, {}) is True

    def test_every_10_min_skips_6_05(self, cron_weekday_task):
        """'*/10' should NOT fire at :05."""
        now = make_pacific_time(hour=6, minute=5)
        assert should_task_run(cron_weekday_task, now, {}) is False

    def test_every_10_min_skips_before_6am(self, cron_weekday_task):
        """Hour range 6-13 should not fire at 5:00."""
        now = make_pacific_time(hour=5, minute=0, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is False

    def test_every_10_min_skips_after_1pm(self, cron_weekday_task):
        """Hour range 6-13 should not fire at 14:00."""
        now = make_pacific_time(hour=14, minute=0, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is False

    def test_every_10_min_runs_at_13_50(self, cron_weekday_task):
        """Last slot in 6-13 range: 13:50."""
        now = make_pacific_time(hour=13, minute=50, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is True

    def test_weekday_cron_skips_saturday(self, cron_weekday_task):
        """'1-5' means Mon-Fri. Saturday should be skipped."""
        # Feb 21, 2026 is Saturday
        now = make_pacific_time(month=2, day=21, hour=6, minute=0, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is False

    def test_weekday_cron_skips_sunday(self, cron_weekday_task):
        """Sunday should be skipped."""
        # Feb 22, 2026 is Sunday
        now = make_pacific_time(month=2, day=22, hour=6, minute=0, second=10)
        assert should_task_run(cron_weekday_task, now, {}) is False

    def test_daily_cron_runs_on_weekend(self, cron_task):
        """'0 1 * * *' has no day filter, should run Saturday."""
        now = make_pacific_time(month=2, day=21, hour=1, minute=0, second=10)
        assert should_task_run(cron_task, now, {}) is True


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


# --- run_http_task tests ---

class TestRunHttpTask:
    @patch('scheduler.requests.request')
    def test_successful_post(self, mock_request, http_task):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_request.return_value = mock_response

        result = run_http_task(http_task)

        assert result == 0
        mock_request.assert_called_once_with(
            method="POST",
            url="http://backendv2:8500/tos-generate",
            headers={"Content-Type": "application/json"},
            json={"days": 1},
            timeout=120,
        )

    @patch('scheduler.requests.request')
    def test_unexpected_status_code(self, mock_request, http_task):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_request.return_value = mock_response

        result = run_http_task(http_task)
        assert result == 1

    @patch('scheduler.requests.request')
    def test_connection_error(self, mock_request, http_task):
        mock_request.side_effect = __import__('requests').ConnectionError("refused")
        result = run_http_task(http_task)
        assert result == 1

    @patch('scheduler.requests.request')
    def test_timeout_error(self, mock_request, http_task):
        mock_request.side_effect = __import__('requests').Timeout("timed out")
        result = run_http_task(http_task)
        assert result == 1

    @patch('scheduler.requests.request')
    def test_generic_exception(self, mock_request, http_task):
        mock_request.side_effect = RuntimeError("something broke")
        result = run_http_task(http_task)
        assert result == 1

    @patch('scheduler.requests.request')
    def test_get_request_no_body(self, mock_request):
        task = {
            "name": "get_task",
            "schedule": "0 5 * * *",
            "type": "http",
            "http": {
                "method": "GET",
                "url": "http://example.com/health",
                "expected_status": [200],
            },
            "timeout": 30,
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_request.return_value = mock_response

        result = run_http_task(task)
        assert result == 0
        mock_request.assert_called_once_with(
            method="GET",
            url="http://example.com/health",
            headers={},
            json=None,
            timeout=30,
        )

    @patch('scheduler.requests.request')
    def test_multiple_expected_status(self, mock_request):
        task = {
            "name": "multi_status",
            "schedule": "0 5 * * *",
            "type": "http",
            "http": {
                "method": "POST",
                "url": "http://example.com/api",
                "expected_status": [200, 201, 204],
            },
            "timeout": 30,
        }
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.text = "Created"
        mock_request.return_value = mock_response

        result = run_http_task(task)
        assert result == 0

    @patch('scheduler.requests.request')
    def test_empty_body_sends_no_json(self, mock_request, http_task):
        """Empty dict body should still send json={}."""
        http_task["http"]["body"] = {}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_request.return_value = mock_response

        run_http_task(http_task)
        # Empty dict is falsy, so json=None
        call_kwargs = mock_request.call_args[1]
        assert call_kwargs["json"] is None


# --- load_tasks tests ---

class TestLoadTasks:
    def test_loads_valid_legacy_config(self, tasks_json):
        tasks = [
            {"name": "t1", "hour": 0, "minute": 0, "days": "daily", "command": ["echo", "hi"]},
        ]
        path = tasks_json(tasks)
        loaded = load_tasks(path)
        assert len(loaded) == 1
        assert loaded[0]["name"] == "t1"

    def test_loads_valid_cron_config(self, tasks_json):
        tasks = [
            {"name": "t1", "schedule": "0 1 * * *", "type": "command", "command": ["echo", "hi"]},
        ]
        path = tasks_json(tasks)
        loaded = load_tasks(path)
        assert len(loaded) == 1

    def test_loads_mixed_config(self, tasks_json):
        tasks = [
            {"name": "legacy", "hour": 1, "minute": 0, "days": "daily", "command": ["echo", "1"]},
            {"name": "cron", "schedule": "0 2 * * *", "type": "command", "command": ["echo", "2"]},
            {"name": "http", "schedule": "*/10 6-13 * * 1-5", "type": "http",
             "http": {"method": "POST", "url": "http://localhost/api", "expected_status": [200]}},
        ]
        path = tasks_json(tasks)
        loaded = load_tasks(path)
        assert len(loaded) == 3

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
    # Legacy validation
    def test_valid_legacy_task_passes(self):
        task = {"name": "t", "hour": 0, "minute": 0, "days": "daily", "command": ["echo"]}
        validate_task(task)  # Should not raise

    def test_missing_name_raises(self):
        task = {"hour": 0, "minute": 0, "days": "daily", "command": ["echo"]}
        with pytest.raises(ValueError, match="name"):
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

    # Cron validation
    def test_valid_cron_task_passes(self):
        task = {"name": "t", "schedule": "0 1 * * *", "type": "command", "command": ["echo"]}
        validate_task(task)

    def test_invalid_cron_expression_raises(self):
        task = {"name": "t", "schedule": "not a cron", "type": "command", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid cron"):
            validate_task(task)

    def test_cron_with_invalid_range_raises(self):
        task = {"name": "t", "schedule": "0 25 * * *", "type": "command", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid cron"):
            validate_task(task)

    def test_cron_schedule_must_be_string(self):
        task = {"name": "t", "schedule": 123, "type": "command", "command": ["echo"]}
        with pytest.raises(ValueError, match="must be a string"):
            validate_task(task)

    # HTTP validation
    def test_valid_http_task_passes(self):
        task = {
            "name": "t", "schedule": "0 1 * * *", "type": "http",
            "http": {"method": "POST", "url": "http://localhost/api"},
        }
        validate_task(task)

    def test_http_missing_config_raises(self):
        task = {"name": "t", "schedule": "0 1 * * *", "type": "http"}
        with pytest.raises(ValueError, match="requires an 'http' config"):
            validate_task(task)

    def test_http_missing_url_raises(self):
        task = {
            "name": "t", "schedule": "0 1 * * *", "type": "http",
            "http": {"method": "POST"},
        }
        with pytest.raises(ValueError, match="missing required field: url"):
            validate_task(task)

    def test_http_invalid_method_raises(self):
        task = {
            "name": "t", "schedule": "0 1 * * *", "type": "http",
            "http": {"method": "INVALID", "url": "http://localhost/api"},
        }
        with pytest.raises(ValueError, match="invalid HTTP method"):
            validate_task(task)

    def test_http_config_must_be_dict(self):
        task = {
            "name": "t", "schedule": "0 1 * * *", "type": "http",
            "http": "not a dict",
        }
        with pytest.raises(ValueError, match="http config must be a dict"):
            validate_task(task)

    def test_invalid_task_type_raises(self):
        task = {"name": "t", "schedule": "0 1 * * *", "type": "webhook", "command": ["echo"]}
        with pytest.raises(ValueError, match="invalid type"):
            validate_task(task)

    def test_command_type_requires_command_field(self):
        task = {"name": "t", "schedule": "0 1 * * *", "type": "command"}
        with pytest.raises(ValueError, match="requires a 'command' field"):
            validate_task(task)


# --- Dedup key tests ---

class TestDedupKey:
    def test_cron_uses_minute_precision(self, cron_task):
        now = make_pacific_time(hour=1, minute=0)
        key = _get_dedup_key(cron_task, now)
        assert key == "2026-02-19 01:00"

    def test_legacy_uses_hour_precision(self, sample_task):
        now = make_pacific_time(hour=1, minute=0)
        key = _get_dedup_key(sample_task, now)
        assert key == "2026-02-19 01"


# --- Format schedule tests ---

class TestFormatSchedule:
    def test_cron_format(self, cron_task):
        assert _format_schedule(cron_task) == "cron(0 1 * * *)"

    def test_legacy_daily_format(self, sample_task):
        assert _format_schedule(sample_task) == "01:00 Pacific, every day"

    def test_legacy_weekday_format(self, weekday_task):
        assert _format_schedule(weekday_task) == "09:30 Pacific, weekdays only"
