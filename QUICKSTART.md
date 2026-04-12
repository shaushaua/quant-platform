# 🚀 快速开始 - 5 分钟上手本地测试

## 第一步：配置 OSS 凭证（只需一次）

### 方式 1：使用配置向导（推荐）

```bash
cd /Users/zhangyang/Desktop/lianghua/quant-platform
./setup_credentials.sh
```

按提示输入你的 AccessKey 即可。

### 方式 2：手动配置

```bash
# 复制配置模板
cp .env.local.example .env.local

# 编辑配置文件
vim .env.local

# 填入你的 AccessKey：
# export OSS_ACCESS_KEY_ID='你的AccessKey'
# export OSS_ACCESS_KEY_SECRET='你的SecretKey'
```

## 第二步：加载配置

```bash
source .env.local
```

## 第三步：运行第一个测试

### 测试示例策略（最简单）

```bash
# 测试 1 只股票，1 天（约 2 元流量费，耗时 1-2 分钟）
python local_test.py example_simple_strategy.py \
    --date 20250106 \
    --codes "000001.SZ"
```

**预期输出**：

```
============================================================
🧪 本地策略测试
============================================================

📝 加载策略: example_simple_strategy.py
   ✓ factor_info: {'market_count': 1, ...}
   ✓ securities: 2 只股票
   ✓ end_times: ['']

📅 测试日期: 20250106 ~ 20250106
   测试 1 只股票: ['000001.SZ']

🔌 连接 OSS: https://oss-cn-hangzhou.aliyuncs.com
   Bucket: quant-mdl-data
   ✓ OSS 连接成功

⚙️  开始计算因子...
   (这可能需要一些时间，取决于数据量)

[INFO] 000001.SZ 20250106: tick=5009 deal=95370
   ✓ 20250106 : 1 条记录

✅ 计算完成

📊 结果统计:
   总记录数: 1
   字段数: 5
   字段名: ['code', 'date', 'tick_count', 'deal_count', 'total_volume']

   ✓ 所有字段都有有效值（前10条检查）

📋 样本数据（第1条）:
      code: 000001.SZ
      date: 20250106
      tick_count: 5009
      deal_count: 95370
      total_volume: 1234567890
      avg_price: 12.3456

💾 结果已保存到: ./local_test_result.json

============================================================
✅ 本地测试完成
============================================================
```

## 第四步：测试你的 TAQ 策略

```bash
# 测试 TAQ 策略（2 只股票，1 天）
python local_test.py taq_generator_strategy.py \
    --date 20250106 \
    --codes "000001.SZ,000002.SZ" \
    -v
```

## 第五步：查看结果

```bash
# 查看结果文件
cat local_test_result.json | python -m json.tool | head -50

# 或使用 jq（如果安装了）
cat local_test_result.json | jq '.[0]'
```

## 常见问题

### Q1: OSS 连接失败

```bash
# 检查环境变量
echo $OSS_ACCESS_KEY_ID
echo $OSS_ENDPOINT

# 手动指定公网 endpoint
export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'
```

### Q2: 数据全是 NaN

检查策略代码中的属性名：

```python
# ❌ 错误
tick = stock_data.tick
deal = stock_data.deal

# ✅ 正确
tick = stock_data.l1_tick
deal = stock_data.l2_deal
```

### Q3: 测试太慢

- 第一次测试建议只用 1 天数据
- 确认逻辑正确后再测试多天

### Q4: 忘记设置环境变量

每次打开新终端都需要重新加载：

```bash
source .env.local
```

或者写入 `~/.bashrc` 或 `~/.zshrc`：

```bash
echo "source /Users/zhangyang/Desktop/lianghua/quant-platform/.env.local" >> ~/.zshrc
```

## 完整工作流示例

```bash
# 1. 一次性配置
./setup_credentials.sh
source .env.local

# 2. 测试示例策略（验证环境）
python local_test.py example_simple_strategy.py --date 20250106 --codes "000001.SZ"

# 3. 测试你的策略（单只股票，单日）
python local_test.py taq_generator_strategy.py --date 20250106 --codes "000001.SZ"

# 4. 测试多只股票
python local_test.py taq_generator_strategy.py --date 20250106 --codes "000001.SZ,000002.SZ"

# 5. 测试多天（验证稳定性）
python local_test.py taq_generator_strategy.py --date 20250106-20250108 --codes "000001.SZ,000002.SZ"

# 6. 确认无误，提交集群
python submit_taq_batches.py
```

## 下一步

- 📖 阅读详细文档：`docs/LOCAL_TEST_GUIDE.md`
- 🌐 了解网络配置：`docs/NETWORK_CONFIG.md`
- 📝 查看项目信息：`CLAUDE.md`

## 获取帮助

```bash
# 查看帮助信息
python local_test.py --help

# 查看演示说明
./demo_test.sh
```
