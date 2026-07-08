#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-off post-close raw archive snapshot.

The default order is order -> deal -> tick so the heaviest stream does not block
order/deal recovery.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path


def _repo_root() -> Path:
    if os.environ.get("REPO_ROOT"):
        return Path(os.environ["REPO_ROOT"]).resolve()
    if Path("/app/quant_platform").exists():
        return Path("/app")
    return Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--kinds", default="order,deal,tick")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--mark-done", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    root = _repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from quant_platform.live_engine.native_engine import (  # noqa: WPS433
        NativeEngine,
        _DEFAULT_POST_CLOSE_ARCHIVE_KINDS,
    )

    engine = NativeEngine()
    engine.trading_day = args.date
    kinds = engine._parse_archive_kinds(args.kinds, _DEFAULT_POST_CLOSE_ARCHIVE_KINDS)
    os.environ.setdefault("RAW_ARCHIVE_EXIT_UPLOAD_SHM", "false")

    files_by_code = engine._scan_shm_files()
    if not files_by_code:
        logging.error("no SHM files found under %s", engine.shm_dir)
        return 2

    logging.info("snapshot starting date=%s kinds=%s codes=%d",
                 args.date, ",".join(kinds), len(files_by_code))
    if not engine._archive_native_incremental(files_by_code, kinds=kinds):
        logging.error("snapshot incomplete date=%s kinds=%s", args.date, ",".join(kinds))
        return 4
    logging.info("snapshot finished date=%s kinds=%s", args.date, ",".join(kinds))

    if args.upload:
        logging.info("upload starting date=%s kinds=%s", args.date, ",".join(kinds))
        if not engine._upload_raw_day_to_oss(kinds=kinds):
            logging.error("upload failed or no chunks uploaded")
            return 3
        logging.info("upload finished date=%s kinds=%s", args.date, ",".join(kinds))

    if args.mark_done:
        marker_suffix = "_".join(kinds)
        engine._mark_archive_done(f"post_close_{marker_suffix}_snapshot")
        if args.upload:
            engine._mark_archive_done(f"post_close_{marker_suffix}_upload")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
