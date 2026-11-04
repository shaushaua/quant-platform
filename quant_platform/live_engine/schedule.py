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
from typing import Dict, List, Optional, Set, Tuple
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
    excluded_minutes: Set[Tuple[int, int]] = field(default_factory=set)

    # time_trigger mode
    trigger_times: Optional[List[Tuple[int, int]]] = None  # [(15, 10)]
    trigger_deadline: Optional[Tuple[int, int]] = None     # optional exclusive HH:MM deadline

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
            if (h, m) in self.excluded_minutes:
                return False
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
            if self.trigger_deadline is not None:
                dh, dm = self.trigger_deadline
                if now_sec >= dh * 3600 + dm * 60:
                    return False
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
        MINUTE_SKIP_TIMES:          comma-separated HH:MM values, default empty
        DAILY_FACTOR_TRIGGER_TIME:  HH:MM, default "15:10"

    factor_info override:
        compute_interval:           strategy-defined interval (takes precedence over env)
    """
    enabled = [s.strip() for s in os.environ.get("COMPUTE_SCHEDULES", "minute").split(",")]
    # factor_info.compute_interval 优先，env var 次之
    fi = factor_info or {}
    interval = int(fi.get("compute_interval", 0)) or int(os.environ.get("COMPUTE_INTERVAL", "60"))
    schedules: List[ComputationSchedule] = []

    # MINUTE_SKIP_TIMES 由 minute 与 minute_daily 共用(同一批跳过分钟)
    excluded_minutes: Set[Tuple[int, int]] = set()
    for raw in os.environ.get("MINUTE_SKIP_TIMES", "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split(":")
        try:
            h, m = int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            logger.warning(
                "[schedule] ignoring malformed MINUTE_SKIP_TIMES value: %r", raw)
            continue
        if not (0 <= h < 24 and 0 <= m < 60):
            logger.warning(
                "[schedule] ignoring out-of-range MINUTE_SKIP_TIMES value: %r", raw)
            continue
        excluded_minutes.add((h, m))

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
            excluded_minutes=excluded_minutes,
            run_inference=minute_run_inference,
            is_daily_result=False,
        ))

    if "minute_daily" in enabled:
        # 第二套分钟级: 日频模块(DAILY_FACTOR_MODULE, v2)的分钟频率滚动计算。
        # 与 "minute"(老 min 模块) 每分钟各算一轮, 经 _compute_lock 自动串行。
        # 产出与 15:10 正式日频同 schema 的 F_ 特征(截至当分钟的滚动近似),
        # 落盘 daily-rolling/ 子目录 + OSS minutes-daily-feature/。
        # run_inference=False: 本阶段只算因子(先测试正确性与耗时), 交易后续接。
        interval_daily = int(os.environ.get(
            "MINUTE_DAILY_INTERVAL", str(interval)))
        schedules.append(ComputationSchedule(
            name="minute_daily",
            schedule_type="interval",
            interval_seconds=interval_daily,
            active_sessions=[((9, 15), (11, 30)), ((13, 0), (15, 0))],
            excluded_minutes=set(excluded_minutes),
            run_inference=False,
            is_daily_result=False,
            use_daily_factor_module=True,
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
        deadline = _parse_trigger_time(
            os.environ.get("OPEN_POSITION_TRIGGER_DEADLINE", "09:35"),
            (9, 35), "OPEN_POSITION_TRIGGER_DEADLINE")
        schedules.append(ComputationSchedule(
            name="open_position",
            schedule_type="time_trigger",
            trigger_times=[t],
            trigger_deadline=deadline,
            run_inference=True,
            is_daily_result=False,
            use_daily_factor_module=False,
            result_label=f"{t[0]:02d}{t[1]:02d}00",
            skip_factor_compute=True,
        ))

    if "daily_basic_refresh" in enabled:
        # Post-close trigger to upload T-0 daily_basic + composition to OSS.
        # MySQL mkt_equd is written at ~15:07 (A-share close + clearing),
        # so 15:30 is the earliest safe fire time. Without this, OSS path
        # returns no T-0 rows and DailyBasicCache falls back to MySQL /
        # T-1 latest_date (see _load_daily_basic).
        t = _parse_trigger_time(
            os.environ.get("DAILY_BASIC_REFRESH_TRIGGER_TIME", "15:30"),
            (15, 30), "DAILY_BASIC_REFRESH_TRIGGER_TIME")
        schedules.append(ComputationSchedule(
            name="daily_basic_refresh",
            schedule_type="time_trigger",
            trigger_times=[t],
            run_inference=False,
            is_daily_result=False,
        ))

    if "csi2000_signal" in enabled:
        # 中证2000独立信号: 15:12 触发(引擎 daily 因子 15:10 产出之后),
        # 产出当天 targets/orders/positions 到 /data/factors/csi2000/。
        # 与全指信号共用因子与模型, 池/基准/资金独立, 虚拟盘不下单。
        t = _parse_trigger_time(
            os.environ.get("CSI2000_SIGNAL_TRIGGER_TIME", "15:12"),
            (15, 12), "CSI2000_SIGNAL_TRIGGER_TIME")
        schedules.append(ComputationSchedule(
            name="csi2000_signal",
            schedule_type="time_trigger",
            trigger_times=[t],
            run_inference=False,
            is_daily_result=False,
        ))

    if "csi2000_open" in enabled:
        # 中证2000实盘执行: 09:30 用昨日 targets 在银河账户建仓/调仓
        # (真实持仓/资金 sizing → 皓兴 VWAP 205), 与全指 open_position 对称。
        t = _parse_trigger_time(
            os.environ.get("CSI2000_OPEN_TRIGGER_TIME", "09:30"),
            (9, 30), "CSI2000_OPEN_TRIGGER_TIME")
        deadline = _parse_trigger_time(
            os.environ.get("CSI2000_OPEN_DEADLINE", "09:45"),
            (9, 45), "CSI2000_OPEN_DEADLINE")
        schedules.append(ComputationSchedule(
            name="csi2000_open",
            schedule_type="time_trigger",
            trigger_times=[t],
            trigger_deadline=deadline,
            run_inference=False,
            is_daily_result=False,
        ))

    if "csi2000_position" in enabled:
        # 中证2000 14:50 盘中调仓: 与全指 daily_position 同频同口径。
        # 因子 = 引擎当天现算的 {td}_145000.csv(handler 内轮询等待
        # daily_position 产出, 不依赖 15:10 才上传的 OSS daily 因子);
        # 执行 = 与 09:30 csi2000_open 同一实盘链(银河账户, 皓兴 VWAP)。
        t = _parse_trigger_time(
            os.environ.get("CSI2000_POSITION_TRIGGER_TIME", "14:50"),
            (14, 50), "CSI2000_POSITION_TRIGGER_TIME")
        deadline = _parse_trigger_time(
            os.environ.get("CSI2000_POSITION_DEADLINE", "15:00"),
            (15, 0), "CSI2000_POSITION_DEADLINE")
        schedules.append(ComputationSchedule(
            name="csi2000_position",
            schedule_type="time_trigger",
            trigger_times=[t],
            trigger_deadline=deadline,
            run_inference=False,
            is_daily_result=False,
        ))

    return schedules
