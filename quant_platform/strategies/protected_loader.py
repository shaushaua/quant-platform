# -*- coding: utf-8 -*-
"""Strategy loading helpers for generated protected single-file wrappers."""

from __future__ import annotations

import ast
import importlib
import importlib.util
import json
import logging
import re
import sys
import threading
from pathlib import Path
from types import ModuleType
from typing import Iterable, List

logger = logging.getLogger(__name__)

_PROTECTED_MARKER = "Generated single-file loader for a protected strategy bundle"
_LOAD_LOCK = threading.RLock()


def _protected_modules_from_source(path: str | None) -> List[str]:
    if not path:
        return []

    source_path = Path(path)
    if not source_path.is_file():
        return []

    try:
        head = source_path.read_text(encoding="utf-8", errors="ignore")[:4096]
    except OSError:
        return []
    if _PROTECTED_MARKER not in head:
        return []

    modules = ["strategy_core"]
    try:
        text = source_path.read_text(encoding="utf-8", errors="ignore")
        match = re.search(r"_PAYLOAD\s*=\s*('(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")", text)
        if match:
            payload = ast.literal_eval(match.group(1))
            for item in json.loads(payload):
                module_name = item.get("module")
                if module_name:
                    modules.append(str(module_name))
    except Exception as exc:
        logger.warning("[strategy-loader] failed to parse protected payload from %s: %s", path, exc)

    return list(dict.fromkeys(modules))


def _clear_modules(module_names: Iterable[str], label: str) -> None:
    cleared = []
    for module_name in module_names:
        if sys.modules.pop(module_name, None) is not None:
            cleared.append(module_name)
    if cleared:
        logger.info("[strategy-loader] isolated %s by clearing modules: %s", label, ",".join(cleared))


def import_strategy_module(module_path: str) -> ModuleType:
    """Import a strategy module, isolating generated protected wrappers.

    Trading strategy interfaces remain unchanged. This only clears stale
    top-level names used internally by generated protected wrappers before the
    wrapper executes, so multiple protected strategies can coexist in one Python
    process without reusing the wrong ``strategy_core`` module.
    """
    spec = importlib.util.find_spec(module_path)
    origin = getattr(spec, "origin", None) if spec is not None else None
    with _LOAD_LOCK:
        _clear_modules(_protected_modules_from_source(origin), module_path)
        return importlib.import_module(module_path)


def load_strategy_file(path: str, module_name: str = "strategy") -> ModuleType:
    """Load a strategy by file path with the same protected-wrapper isolation."""
    with _LOAD_LOCK:
        _clear_modules(_protected_modules_from_source(path), path)
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load strategy from {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
