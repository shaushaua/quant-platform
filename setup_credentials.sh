#!/bin/bash
# OSS 凭证配置向导

echo "============================================"
echo "🔐 OSS 凭证配置向导"
echo "============================================"
echo ""
echo "本地测试需要 OSS 凭证来访问数据"
echo ""

# 读取凭证
read -p "请输入 OSS_ACCESS_KEY_ID: " ACCESS_KEY_ID
read -p "请输入 OSS_ACCESS_KEY_SECRET: " ACCESS_KEY_SECRET

# 写入配置文件
cat > .env.local <<EOF
# OSS 凭证配置
export OSS_ACCESS_KEY_ID='$ACCESS_KEY_ID'
export OSS_ACCESS_KEY_SECRET='$ACCESS_KEY_SECRET'
export OSS_DATA_BUCKET='quant-mdl-data'

# endpoint 会自动检测，如需手动指定取消注释：
# 公网（本地开发）：
# export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'
# 内网（VPN/集群）：
# export OSS_ENDPOINT='https://oss-cn-hangzhou-internal.aliyuncs.com'
EOF

echo ""
echo "✅ 配置已保存到 .env.local"
echo ""
echo "使用方法："
echo "  source .env.local"
echo "  python local_test.py strategy.py --date 20250106 --codes \"000001.SZ\""
echo ""
