# -*- coding: utf-8 -*-
"""Configuration for the MDL SDK collector."""

from dataclasses import dataclass
import os
from typing import List, Tuple


Subscription = Tuple[int, int]


@dataclass(frozen=True)
class SDKCollectorConfig:
    # Local mode: connect to feeder_client sidecar on localhost
    use_local_client: bool = False
    # Remote MDL cloud: token-based auth, no password needed
    # SZ L2 server (19012 port)
    server: str = "mdl-cloud-sh.datayes.com:19012"
    # SH L2 server (different from SZ)
    server_sh: str = "mdl-sse01.datayes.com:19010"
    token: str = ""
    io_threads: int = 4
    callback_multithread: bool = True
    encoding: int = 7        # 7=compressed (public network), 1=uncompressed (LAN)
    enable_merge: bool = True # Enable group encoding for public network
    heartbeat_interval: int = 10
    heartbeat_timeout: int = 30
    flush_interval_ms: int = 10
    batch_size: int = 2000
    queue_warn_size: int = 50000
    queue_hard_limit: int = 0
    subs: Tuple[Subscription, ...] = ((4, 4), (4, 24), (6, 28), (6, 33), (6, 36))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _parse_subs(raw: str) -> Tuple[Subscription, ...]:
    result: List[Subscription] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(".")
        if len(parts) != 2:
            raise ValueError(f"invalid MDL subscription: {item!r}")
        result.append((int(parts[0]), int(parts[1])))
    return tuple(result)


def load_config() -> SDKCollectorConfig:
    default_subs = "4.4,4.24,6.28,6.33,6.36"
    use_local = _env_bool("MDL_USE_LOCAL_CLIENT", False)

    if use_local:
        # Local mode: feeder_client sidecar on localhost, no compression/token needed
        defaults = dict(
            use_local_client=True,
            server="127.0.0.1:9012",
            server_sh="127.0.0.1:9012",
            token="",
            encoding=1,
            enable_merge=False,
        )
    else:
        defaults = dict(
            use_local_client=False,
            server=os.getenv("MDL_SERVER", "mdl-cloud-sh.datayes.com:19012"),
            server_sh=os.getenv("MDL_SERVER_SH", "mdl-sse01.datayes.com:19010"),
            token=os.getenv("MDL_TOKEN", ""),
            encoding=int(os.getenv("MDL_ENCODING", "7")),
            enable_merge=_env_bool("MDL_ENABLE_MERGE", True),
        )

    return SDKCollectorConfig(
        **defaults,
        io_threads=int(os.getenv("MDL_IO_THREADS", "4")),
        callback_multithread=_env_bool("MDL_CALLBACK_MULTITHREAD", True),
        heartbeat_interval=int(os.getenv("MDL_HEARTBEAT_INTERVAL", "10")),
        heartbeat_timeout=int(os.getenv("MDL_HEARTBEAT_TIMEOUT", "30")),
        flush_interval_ms=int(os.getenv("MDL_FLUSH_INTERVAL_MS", "10")),
        batch_size=int(os.getenv("MDL_BATCH_SIZE", "2000")),
        queue_warn_size=int(os.getenv("MDL_QUEUE_WARN_SIZE", "50000")),
        queue_hard_limit=int(os.getenv("MDL_QUEUE_HARD_LIMIT", "0")),
        subs=_parse_subs(os.getenv("MDL_SUBS", default_subs)),
    )
