from datetime import datetime

from quant_platform.live_engine.schedule import build_schedules_from_env


def test_call_auction_precompute_has_bounded_trigger_window(monkeypatch):
    monkeypatch.setenv(
        "COMPUTE_SCHEDULES", "open_position_precompute,open_position")
    monkeypatch.setenv("OPEN_POSITION_PRECOMPUTE_TRIGGER_TIME", "09:25")
    monkeypatch.setenv("OPEN_POSITION_TRIGGER_TIME", "09:30")

    schedules = {schedule.name: schedule for schedule in build_schedules_from_env()}
    precompute = schedules["open_position_precompute"]
    open_position = schedules["open_position"]

    assert precompute.should_run(
        0.0, datetime(2026, 8, 7, 9, 25, 0), "20260807") is True
    assert open_position.should_run(
        0.0, datetime(2026, 8, 7, 9, 25, 0), "20260807") is False

    fresh = {
        schedule.name: schedule for schedule in build_schedules_from_env()
    }["open_position_precompute"]
    assert fresh.should_run(
        0.0, datetime(2026, 8, 7, 9, 30, 0), "20260807") is False
