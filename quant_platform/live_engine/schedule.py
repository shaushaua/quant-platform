# -*- coding: utf-8 -*-
"""
Computation schedules for NativeEngine.

Defines when and how factor computation is triggered — minute-level interval
or time-based triggers (e.g. daily at 15:10).

The engine reads COMPUTE_SCHEDULES env var and builds the active schedule list.
The factor_calculation function is the same regardless of schedule.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import os

logger = logging.getLogger(__name__)


def _parse_trigger_time(raw: str, default: Tuple[int, int],
                        env_var: str) -> Tuple[int, int]:
    """Parse "HH:MM" into (H, M); fall back to default on malformed input.

    Tolerates extra parts (e.g. "14:50:00") and logs a warning on fallback so
    a typo in k8s env does not crash engine startup.
    """
    parts = raw.split(":")
    if len(parts) < 2:
        logger.warning("[schedule] %s=%r malformed (expected HH:MM); using default %02d:%02d",
                       env_var, raw, default[0], default[1])
        return default
    try:
        h, m = int(parts[0]), int(parts[1])
    except ValueError:
        logger.warning("[schedule] %s=%r non-integer; using default %02d:%02d",
                       env_var, raw, default[0], default[1])
        return default
    if not (0 <= h < 24 and 0 <= m < 60):
        logger.warning("[schedule] %s=%r out of range; using default %02d:%02d",
                       env_var, raw, default[0], default[1])
        return default
    return (h, m)


@dataclass
class ComputationSchedule:
    """A single computation frequency definition."""

    name: str                                   # "minute" | "daily" | future "hourly"
    schedule_type: str                          # "interval" | "time_trigger"

    # interval mode
    interval_seconds: int = 60
    active_hours: Tuple[Tuple[int, int], Tuple[int, int]] = ((9, 15), (15, 5))
    active_sessions: Optional[List[Tuple[Tuple[int, int], Tuple[int, int]]]] = None

    # time_trigger mode
    trigger_times: Optional[List[Tuple[int, int]]] = None  # [(15, 10)]

    # behavior flags
    run_inference: bool = True
    is_daily_result: bool = False               # result becomes next day's prev_day
    use_daily_factor_module: bool = False       # select daily_factor_module + all stocks + single-code call + end_time=''
    result_label: str = ""                      # storage label (csv/oss key); empty -> end_time
    skip_factor_compute: bool = False           # open_position: skip shm scan + factor calc, use prev_day_factors directly

    # runtime state (managed by engine)
    _last_run_ts: float = field(default=0.0, repr=False)
    _last_run_day: str = field(default="", repr=False)

    def should_run(self, now_ts: float, now_dt: datetime,
                   trading_day: str) -> bool:
        """Return True if this schedule should fire now."""
        if self.schedule_type == "interval":
            if now_ts - self._last_run_ts < self.interval_seconds:
                return False
            h, m = now_dt.hour, now_dt.minute
            now_min = h * 60 + m
            if self.active_sessions:
                return any(
                    (sh * 60 + sm) <= now_min <= (eh * 60 + em)
                    for (sh, sm), (eh, em) in self.active_sessions
                )
            (sh, sm), (eh, em) = self.active_hours
            return (sh * 60 + sm) <= now_min <= (eh * 60 + em)

        elif self.schedule_type == "time_trigger":
            if self._last_run_day == trading_day:
                return False  # already triggered today
            # Fire once the first trigger time has been reached today. Using a
            # ">=" window (rather than exact hour/minute match) tolerates the
            # main loop missing the exact minute while busy (e.g. GIL held by a
            # long minute compute); as long as the loop samples any later time
            # in the same trading_day, the daily run still fires once.
            now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
            for th, tm in (self.trigger_times or []):
                if now_sec >= th * 3600 + tm * 60:
                    return True
            return False

        return False

    def mark_run(self, now_ts: float, trading_day: str) -> None:
        """Record that this schedule has fired."""
        self._last_run_ts = now_ts
        self._last_run_day = trading_day

    def unmark_run(self) -> None:
        """Roll back a mark_run() if dispatch failed.

        Restores the not-yet-run state so the schedule can fire again on the
        next main-loop tick. Without this, a failed thread start would leave
        a daily schedule permanently marked as run for the day.
        """
        self._last_run_ts = 0.0
        self._last_run_day = ""

    @property
    def end_time_label(self) -> str:
        """The end_time value passed to factor_calculation.

        minute:  actual clock HHMMSS (computed at dispatch time)
        daily:   "daily"
        """
        if self.name == "daily":
            return "daily"
        return ""  # interval — filled at dispatch with actual time


def build_schedules_from_env(factor_info: Optional[Dict] = None) -> List[ComputationSchedule]:
    """Build the active schedule list from environment variables.

    Env vars:
        COMPUTE_SCHEDULES:          comma-separated names, default "minute"
        COMPUTE_INTERVAL:           minute interval seconds, default 60
        DAILY_FACTOR_TRIGGER_TIME:  HH:MM, default "15:10"

    factor_info override:
        compute_interval:           strategy-defined interval (takes precedence over env)
    """
    enabled = [s.strip() for s in os.environ.get("COMPUTE_SCHEDULES", "minute").split(",")]
    # factor_info.compute_interval 优先，env var 次之
    fi = factor_info or {}
    interval = int(fi.get("compute_interval", 0)) or int(os.environ.get("COMPUTE_INTERVAL", "60"))
    schedules: List[ComputationSchedule] = []

    if "minute" in enabled:
        # MINUTE_RUN_INFERENCE=false: keep computing factors (and syncing tick
        # state) but skip inference → no order push. Used when only daily
        # frequency orders are desired (e.g. sim day with only open_position).
        minute_run_inference = os.environ.get("MINUTE_RUN_INFERENCE", "true").lower() in {
            "1", "true", "yes", "y", "on"
        }
        schedules.append(ComputationSchedule(
            name="minute",
            schedule_type="interval",
            interval_seconds=interval,
            active_hours=((9, 15), (15, 0)),
            active_sessions=[((9, 15), (11, 30)), ((13, 0), (15, 0))],
            run_inference=minute_run_inference,
            is_daily_result=False,
        ))

    if "daily" in enabled:
        t = _parse_trigger_time(
            os.environ.get("DAILY_FACTOR_TRIGGER_TIME", "15:10"),
            (15, 10), "DAILY_FACTOR_TRIGGER_TIME")
        schedules.append(ComputationSchedule(
            name="daily",
            schedule_type="time_trigger",
            trigger_times=[t],
            run_inference=False,
            is_daily_result=True,
        ))

    if "daily_position" in enabled:
        t = _parse_trigger_time(
            os.environ.get("DAILY_POSITION_TRIGGER_TIME", "14:50"),
            (14, 50), "DAILY_POSITION_TRIGGER_TIME")
        schedules.append(ComputationSchedule(
            name="daily_position",
            schedule_type="time_trigger",
            trigger_times=[t],
            run_inference=True,
            is_daily_result=False,
            use_daily_factor_module=True,
            result_label=f"{t[0]:02d}{t[1]:02d}00",
        ))

    if "open_position" in enabled:
        t = _parse_trigger_time(
            os.environ.get("OPEN_POSITION_TRIGGER_TIME", "09:30"),
            (9, 30), "OPEN_POSITION_TRIGGER_TIME")
        schedules.append(ComputationSchedule(
            name="open_position",
            schedule_type="time_trigger",
            trigger_times=[t],
            run_inference=True,
            is_daily_result=False,
            use_daily_factor_module=False,
            result_label=f"{t[0]:02d}{t[1]:02d}00",
            skip_factor_compute=True,
        ))

    return schedules
