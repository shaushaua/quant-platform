"""
fast_csv: Rust 高性能 CSV 预过滤解析器的 Python ctypes 绑定

用法:
    from quant_platform.rust_ext.fast_csv import parse_csv_chunk
    new_offset, header, rows = parse_csv_chunk(
        path="/path/to/file.csv",
        offset=0,
        max_bytes=50*1024*1024,
        sid_col=5,
        sid_prefixes=["0", "3"],
        header_first_col="ChannelNo",
    )
"""

import ctypes
import os
from pathlib import Path
from typing import List, Optional, Tuple

# 定位 .so 文件
_LIB_DIR = Path(__file__).parent
_LIB_PATH = _LIB_DIR / "libfast_csv.so"

if not _LIB_PATH.exists():
    # 也尝试从项目根目录找
    _ALT_PATH = Path(__file__).parent.parent.parent / "libfast_csv.so"
    if _ALT_PATH.exists():
        _LIB_PATH = _ALT_PATH

_lib = None


def _get_lib():
    global _lib
    if _lib is not None:
        return _lib

    if not _LIB_PATH.exists():
        raise FileNotFoundError(
            f"Rust 扩展未找到: {_LIB_PATH}\n"
            f"请先编译: cd quant_platform/rust_ext && cargo build --release\n"
            f"然后复制: cp target/release/libfast_csv.so {_LIB_DIR}/"
        )

    _lib = ctypes.CDLL(str(_LIB_PATH))

    # 设置函数签名
    _lib.fast_csv_parse.restype = _ParseResult
    _lib.fast_csv_parse.argtypes = [
        ctypes.c_char_p,  # path
        ctypes.c_uint64,  # offset
        ctypes.c_size_t,  # max_bytes
        ctypes.c_size_t,  # sid_col
        ctypes.c_char_p,  # sid_prefixes
        ctypes.c_char_p,  # header_first_col
    ]

    _lib.fast_csv_free.argtypes = [ctypes.POINTER(_ParseResult)]
    _lib.fast_csv_free.restype = None

    return _lib


class _ParseResult(ctypes.Structure):
    _fields_ = [
        ("new_offset", ctypes.c_uint64),
        ("row_count", ctypes.c_uint32),
        ("col_count", ctypes.c_uint32),
        ("has_header", ctypes.c_uint8),
        ("data_ptr", ctypes.c_char_p),
        ("data_len", ctypes.c_uint64),
    ]


def parse_csv_chunk(
    path: str,
    offset: int,
    max_bytes: int,
    sid_col: int,
    sid_prefixes: List[str],
    header_first_col: str = "",
) -> Tuple[int, Optional[List[str]], List[List[str]]]:
    """
    用 Rust 解析 CSV chunk，预过滤股票行。

    Args:
        path: CSV 文件路径
        offset: 当前文件偏移
        max_bytes: 最大读取字节数
        sid_col: SecurityID 所在列索引（0-based）
        sid_prefixes: 有效的 SecurityID 前缀列表，如 ["0", "3"]
        header_first_col: header 第一列的列名（用于识别 header 行），空字符串表示不识别

    Returns:
        (new_offset, header, rows)
        - new_offset: 新的文件偏移
        - header: 列名列表（如果 chunk 中包含 header），否则 None
        - rows: 过滤后的行数据列表
    """
    lib = _get_lib()

    # 准备参数
    c_path = path.encode("utf-8")
    c_prefixes = ",".join(sid_prefixes).encode("utf-8")
    c_header_col = header_first_col.encode("utf-8") if header_first_col else b""

    # 调用 Rust 函数
    result = lib.fast_csv_parse(
        c_path,
        offset,
        max_bytes,
        sid_col,
        c_prefixes,
        c_header_col,
    )

    try:
        new_offset = result.new_offset
        row_count = result.row_count
        col_count = result.col_count
        has_header = result.has_header

        if row_count == 0 or not result.data_ptr:
            return new_offset, None, []

        # 解析连续内存：所有字段以 \0 分隔
        data = result.data_ptr[:result.data_len]
        fields = data.split(b'\0')
        # 去掉最后一个空元素（因为数据以 \0 结尾）
        if fields and fields[-1] == b'':
            fields = fields[:-1]

        # 按 col_count 分组为行
        rows: List[List[str]] = []
        header: Optional[List[str]] = None

        start = 0
        for i in range(row_count):
            row_fields = fields[start:start + col_count]
            row_str = [f.decode("utf-8", errors="replace") for f in row_fields]

            if has_header and i == 0:
                header = row_str
            else:
                rows.append(row_str)

            start += col_count

        return new_offset, header, rows

    finally:
        # 释放 Rust 分配的内存
        lib.fast_csv_free(ctypes.byref(result))


def is_available() -> bool:
    """检查 Rust 扩展是否可用。"""
    try:
        _get_lib()
        return True
    except (FileNotFoundError, OSError):
        return False
