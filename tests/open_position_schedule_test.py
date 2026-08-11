from datetime import datetime

from quant_platform.live_engine.schedule import build_schedules_from_env


def test_open_position_runs_at_930_without_precompute(monkeypatch):
    monkeypatch.setenv("COMPUTE_SCHEDULES", "open_position_precompute,open_position")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_TIME", "09:30")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_DEADLINE", "09:35")

    schedules = {schedule.name: schedule for schedule in build_schedules_from_env()}
    open_position = schedules["open_position"]

    assert "open_position_precompute" not in schedules
    assert open_position.should_run(
        0.0, datetime(2026, 8, 7, 9, 25, 0), "20260807") is False
    assert open_position.should_run(
        0.0, datetime(2026, 8, 7, 9, 30, 0), "20260807") is True


def test_open_position_does_not_trigger_at_or_after_deadline(monkeypatch):
    monkeypatch.setenv("COMPUTE_SCHEDULES", "open_position")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_TIME", "09:30")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_DEADLINE", "09:35")

    before_deadline = build_schedules_from_env()[0]
    at_deadline = build_schedules_from_env()[0]
    after_deadline = build_schedules_from_env()[0]

    assert before_deadline.should_run(
        0.0, datetime(2026, 8, 11, 9, 34, 59), "20260811") is True
    assert at_deadline.should_run(
        0.0, datetime(2026, 8, 11, 9, 35, 0), "20260811") is False
    assert after_deadline.should_run(
        0.0, datetime(2026, 8, 11, 13, 0, 0), "20260811") is False


def test_minute_schedule_skips_930_and_is_eligible_at_931(monkeypatch):
    monkeypatch.setenv("COMPUTE_SCHEDULES", "minute,open_position")
    monkeypatch.setenv("MINUTE_SKIP_TIMES", "09:30")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_TIME", "09:30")

    schedules = {schedule.name: schedule for schedule in build_schedules_from_env()}
    minute = schedules["minute"]
    open_position = schedules["open_position"]

    assert minute.should_run(
        120.0, datetime(2026, 8, 11, 9, 30, 0), "20260811") is False
    assert open_position.should_run(
        120.0, datetime(2026, 8, 11, 9, 30, 0), "20260811") is True
    assert minute.should_run(
        180.0, datetime(2026, 8, 11, 9, 31, 0), "20260811") is True
