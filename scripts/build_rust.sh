#!/bin/bash
# 构建 Rust 快速 CSV 解析器
# 用法: ./scripts/build_rust.sh
# 产出: quant_platform/rust_ext/libfast_csv.so

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RUST_DIR="$PROJECT_DIR/quant_platform/rust_ext"
OUTPUT_DIR="$PROJECT_DIR/quant_platform/rust_ext"

echo "=== 编译 Rust 快速 CSV 解析器 ==="

cd "$RUST_DIR"

# 检查 Rust 工具链
if ! command -v cargo &>/dev/null; then
    echo "安装 Rust 工具链..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

# 编译 release 版本
echo "编译中..."
cargo build --release

# 复制 .so 到 Python 包目录
cp target/release/libfast_csv.so "$OUTPUT_DIR/"
echo "已复制到: $OUTPUT_DIR/libfast_csv.so"

# 验证
python3 -c "
from quant_platform.rust_ext.fast_csv import is_available
print(f'Rust 扩展可用: {is_available()}')
"

echo "=== 完成 ==="
