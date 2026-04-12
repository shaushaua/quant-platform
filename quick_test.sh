#!/bin/bash
# 快速测试脚本

set -e

echo "============================================"
echo "🚀 快速测试工具"
echo "============================================"
echo ""

# 检查环境变量
if [ -z "$OSS_ACCESS_KEY_ID" ]; then
    echo "❌ 错误: 缺少环境变量 OSS_ACCESS_KEY_ID"
    echo ""
    echo "请先设置 OSS 环境变量："
    echo ""
    echo "方式 1: 手动设置"
    echo "  export OSS_ACCESS_KEY_ID='your_key'"
    echo "  export OSS_ACCESS_KEY_SECRET='your_secret'"
    echo "  # OSS_ENDPOINT 可选，会自动检测网络环境"
    echo ""
    echo "方式 2: 使用配置文件"
    echo "  cp .env.local.example .env.local"
    echo "  # 编辑 .env.local 填入你的凭证"
    echo "  source .env.local"
    echo ""
    exit 1
fi

# 检测网络环境
if [ -z "$OSS_ENDPOINT" ]; then
    echo "ℹ️  OSS_ENDPOINT 未设置，将自动检测网络环境..."
fi

# 默认参数
STRATEGY="${1:-example_simple_strategy.py}"
DATE="${2:-20250106}"
CODES="${3:-000001.SZ,000002.SZ}"

echo "📝 策略文件: $STRATEGY"
echo "📅 测试日期: $DATE"
echo "📈 测试股票: $CODES"
echo ""

# 检查策略文件
if [ ! -f "$STRATEGY" ]; then
    echo "❌ 错误: 策略文件不存在: $STRATEGY"
    exit 1
fi

# 运行测试
echo "⚙️  开始测试..."
echo ""

python3 local_test.py "$STRATEGY" --date "$DATE" --codes "$CODES" -v

echo ""
echo "============================================"
echo "✅ 测试完成"
echo "============================================"
echo ""
echo "💡 提示："
echo "  - 结果已保存到 ./local_test_result.json"
echo "  - 如果测试通过，可以提交到集群："
echo "    python3 submit_taq_batches.py"
echo ""
