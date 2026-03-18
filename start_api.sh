#!/bin/bash
# API 宿主机启动脚本
# 用法: ./start_api.sh

set -e

# 加载环境变量
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# 检查必要环境变量
required_vars=(
    "OSS_ACCESS_KEY_ID"
    "OSS_ACCESS_KEY_SECRET"
    "OSS_BUCKET_NAME"
    "OSS_ENDPOINT"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "错误: 缺少环境变量 $var"
        exit 1
    fi
done

# 创建必要目录
mkdir -p /data /results /logs

echo "Starting API server on port 8000..."
python -m uvicorn qlib_factor_platform.api:app --host 0.0.0.0 --port 8000 --reload
