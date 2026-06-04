#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify Rust MDL parser assumptions against the generated Python SDK.

The parser is intentionally hand-written for speed. This script guards the
small set of binary layout constants that the hot path depends on.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import tarfile
import tempfile
from pathlib import Path


DEFAULT_SDK = Path(
    os.environ.get(
        "PYMDL_SDK_TAR",
        "/Users/zhangyang/Downloads/MDL Python SDK客户端/pymdl-2.13.232-py3.tar.gz",
    )
)


EXPECTED = {
    "mdl_shl2_msg.py": {
        "MDLMID_SHL2MarketData": 4,
        "MDLMID_NGTSTick": 24,
        "SHL2MarketData.msg_size": 248,
        "SHL2MarketData.BidLevelsItem.msg_size": 28,
        "SHL2MarketData.SellLevelsItem.msg_size": 28,
        "NGTSTick.msg_size": 70,
    },
    "mdl_szl2_msg.py": {
        "MDLMID_Snapshot300111_v2": 28,
        "MDLMID_Order300192_v2": 33,
        "MDLMID_Transaction300191_v2": 36,
        "Snapshot300111_v2.msg_size": 224,
        "Snapshot300111_v2.BidPriceLevelItem.msg_size": 28,
        "Snapshot300111_v2.AskPriceLevelItem.msg_size": 28,
        "Order300192_v2.msg_size": 58,
        "Transaction300191_v2.msg_size": 70,
    },
}


def _extract_sdk(sdk_tar: Path, out_dir: Path) -> dict[str, Path]:
    wanted = {
        "mdl_shl2_msg.py": "pymdl/api/mdl_shl2_msg.py",
        "mdl_szl2_msg.py": "pymdl/api/mdl_szl2_msg.py",
    }
    result = {}
    with tarfile.open(sdk_tar, "r:gz") as tf:
        members = {m.name: m for m in tf.getmembers()}
        for filename, suffix in wanted.items():
            member = next((m for n, m in members.items() if n.endswith(suffix)), None)
            if member is None:
                raise FileNotFoundError(f"{suffix} not found in {sdk_tar}")
            target = out_dir / filename
            src = tf.extractfile(member)
            if src is None:
                raise FileNotFoundError(f"{member.name} cannot be read from {sdk_tar}")
            target.write_bytes(src.read())
            result[filename] = target
    return result


def _class_node(tree: ast.AST, dotted: str) -> ast.ClassDef:
    node: ast.AST = tree
    for name in dotted.split("."):
        for child in getattr(node, "body", []):
            if isinstance(child, ast.ClassDef) and child.name == name:
                node = child
                break
        else:
            raise KeyError(f"class {dotted} not found")
    return node  # type: ignore[return-value]


def _msg_size(tree: ast.AST, dotted_class: str) -> int:
    cls = _class_node(tree, dotted_class)
    init = next(
        child for child in cls.body
        if isinstance(child, ast.FunctionDef) and child.name == "__init__"
    )
    for stmt in init.body:
        if not isinstance(stmt, ast.Assign):
            continue
        for target in stmt.targets:
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr == "msg_size"
                and isinstance(stmt.value, ast.Constant)
            ):
                return int(stmt.value.value)
    raise KeyError(f"{dotted_class}.msg_size not found")


def _module_const(text: str, name: str) -> int:
    m = re.search(rf"^{re.escape(name)}\s*=\s*(\d+)\s*$", text, re.MULTILINE)
    if not m:
        raise KeyError(f"{name} not found")
    return int(m.group(1))


def verify(sdk_tar: Path) -> list[str]:
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="mdl-sdk-layout-") as td:
        files = _extract_sdk(sdk_tar, Path(td))
        for filename, checks in EXPECTED.items():
            path = files[filename]
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
            for key, expected in checks.items():
                actual = _module_const(text, key) if key.startswith("MDLMID_") else _msg_size(tree, key[:-9])
                if actual != expected:
                    failures.append(f"{filename}: {key} expected {expected}, got {actual}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sdk", type=Path, default=DEFAULT_SDK, help="Path to pymdl py3 tar.gz")
    args = parser.parse_args()

    failures = verify(args.sdk)
    if failures:
        print("MDL layout verification failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print(f"MDL layout verification passed: {args.sdk}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
