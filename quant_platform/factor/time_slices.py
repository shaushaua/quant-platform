"""Trading-session time slice helpers for factor calculation."""

from __future__ import annotations


def generate_intraday_end_times(interval_seconds: int) -> list[str]:
    """Generate A-share intraday end_time labels.

    Sessions are right-open: 09:25-11:30 and 13:00-15:00.
    For interval_seconds=60 this returns 245 labels:
    092500, 092600, ..., 145900.
    """
    if interval_seconds <= 0:
        return [""]

    times: list[str] = []

    def append_range(start: int, end: int) -> None:
        t = start
        while t < end:
            h, rem = divmod(t, 3600)
            m, s = divmod(rem, 60)
            times.append(f"{h:02d}{m:02d}{s:02d}")
            t += interval_seconds

    append_range(9 * 3600 + 25 * 60, 11 * 3600 + 30 * 60)
    append_range(13 * 3600, 15 * 3600)
    return times
