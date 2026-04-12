# 本地测试环境配置指南

## 网络环境说明

### 集群环境（内网）
- **Endpoint**: `https://oss-cn-hangzhou-internal.aliyuncs.com`
- **特点**: 快速、免流量费
- **适用**: K8s 集群内的 Pod

### 本地开发环境（公网）
- **Endpoint**: `https://oss-cn-hangzhou.aliyuncs.com`
- **特点**: 较慢、有流量费（约 0.5元/GB）
- **适用**: 本地电脑、无 VPN 环境

## 配置方法

### 方式 1: 自动检测（推荐）

工具会自动检测网络环境并选择合适的 endpoint：

```bash
# 只需设置 AK/SK，endpoint 会自动选择
export OSS_ACCESS_KEY_ID='your_access_key_id'
export OSS_ACCESS_KEY_SECRET='your_access_key_secret'

# 运行测试（会自动检测网络并使用合适的 endpoint）
python local_test.py strategy.py --date 20250106 --codes "000001.SZ"
```

**输出示例**：
```
ℹ️  检测到公网环境，使用公网 endpoint（会产生流量费用）
🔌 连接 OSS: https://oss-cn-hangzhou.aliyuncs.com
```

### 方式 2: 手动指定 endpoint

```bash
export OSS_ACCESS_KEY_ID='your_access_key_id'
export OSS_ACCESS_KEY_SECRET='your_access_key_secret'

# 本地开发（公网）
export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'

# 或通过 VPN 访问内网
# export OSS_ENDPOINT='https://oss-cn-hangzhou-internal.aliyuncs.com'
```

### 方式 3: 使用配置文件

```bash
# 1. 复制示例配置
cp .env.local.example .env.local

# 2. 编辑配置文件
vim .env.local

# 填入你的 AccessKey
# 选择合适的 OSS_ENDPOINT

# 3. 加载配置
source .env.local

# 4. 运行测试
python local_test.py strategy.py --date 20250106 --codes "000001.SZ"
```

## 流量费用说明

### 本地测试的流量消耗

| 测试规模 | 下载数据量 | 预估费用 |
|---------|-----------|---------|
| 1 只股票，1 天 | ~4 GB | ~2 元 |
| 2 只股票，1 天 | ~4 GB | ~2 元 |
| 2 只股票，5 天 | ~20 GB | ~10 元 |
| 10 只股票，1 天 | ~4 GB | ~2 元 |

**说明**：
- DuckDB 虽然只加载需要的数据到内存，但**整个文件仍需要下载**
- tick 文件 ~1.4 GB，deal 文件 ~2.5 GB，每天总计 ~4 GB
- 多只股票不会增加下载量（都在同一个文件中）
- 多天会线性增加下载量

### 降低费用的方法

#### 方法 1: 减少测试天数

```bash
# ❌ 测试 1 个月（26 个交易日）~100 GB，~50 元
python local_test.py strategy.py --date 20250106-20250131 --codes "000001.SZ"

# ✅ 先测试 1 天
python local_test.py strategy.py --date 20250106 --codes "000001.SZ"

# ✅ 然后测试 2-3 天验证稳定性
python local_test.py strategy.py --date 20250106-20250108 --codes "000001.SZ"

# ✅ 确认无误后提交集群（集群内网免费）
python submit_taq_batches.py
```

#### 方法 2: 使用 VPN 连接内网

如果公司提供 VPN 连接到阿里云 VPC：

```bash
# 1. 连接 VPN
# 2. 使用内网 endpoint
export OSS_ENDPOINT='https://oss-cn-hangzhou-internal.aliyuncs.com'

# 3. 测试（免流量费）
python local_test.py strategy.py --date 20250106 --codes "000001.SZ"
```

#### 方法 3: 在集群内测试

如果数据量大，可以在集群内运行测试脚本：

```bash
# 在集群内启动一个临时 Pod
kubectl run local-test --rm -it \
  --image=registry.cn-hangzhou.aliyuncs.com/lianghua/quant-platform:latest \
  -n quant -- bash

# 在 Pod 内运行本地测试（使用内网，免费）
python local_test.py strategy.py --date 20250106-20250110 --codes "000001.SZ"
```

#### 方法 4: 缓存数据（未来功能）

可以考虑添加本地缓存功能：

```python
# 未来可以添加
# 第一次下载后缓存到本地
# 第二次直接使用缓存
export OSS_CACHE_DIR="$HOME/.oss_cache"
```

## 推荐的测试策略

### 本地测试（公网）

**目标**：快速验证策略逻辑

```bash
# 阶段 1: 验证基本逻辑（1 只股票，1 天）
# 数据量: ~4 GB，费用: ~2 元
python local_test.py strategy.py --date 20250106 --codes "000001.SZ"

# 阶段 2: 验证多股票（2-3 只股票，1 天）
# 数据量: ~4 GB，费用: ~2 元
python local_test.py strategy.py --date 20250106 --codes "000001.SZ,000002.SZ"

# 阶段 3: 验证多日期（2 只股票，2-3 天）
# 数据量: ~8-12 GB，费用: ~4-6 元
python local_test.py strategy.py --date 20250106-20250108 --codes "000001.SZ,000002.SZ"
```

**总费用**：约 8-10 元

### 集群测试（内网，免费）

**目标**：大规模验证

```bash
# 提交小规模任务（5-10 只股票，5-10 天）
# 免费，验证集群环境
python submit_taq_batches.py  # 修改为小规模

# 确认无误后，提交完整批次
python submit_taq_batches.py  # 完整规模
```

## 故障排查

### 问题 1: 连接超时

```
❌ 错误: OSS 连接失败: timeout
```

**原因**: 网络不稳定或 endpoint 选择错误

**解决**:
```bash
# 检查网络
ping oss-cn-hangzhou.aliyuncs.com

# 手动指定公网 endpoint
export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'
```

### 问题 2: 访问被拒绝

```
❌ 错误: Access Denied
```

**原因**: AccessKey 权限不足或错误

**解决**:
- 检查 AccessKey 是否正确
- 确认 AccessKey 有 OSS 读取权限

### 问题 3: 下载速度慢

```
下载完成: ... (耗时 5 分钟)
```

**原因**: 公网带宽限制

**解决**:
- 使用 VPN 连接内网
- 或在集群内运行测试
- 或减少测试数据量

## 总结

| 场景 | Endpoint | 费用 | 速度 | 推荐场景 |
|------|---------|------|------|---------|
| 本地电脑 | 公网 | 有 | 慢 | 快速验证逻辑（1-3天数据）|
| 通过 VPN | 内网 | 无 | 快 | 如果有 VPN，优先使用 |
| 集群内 Pod | 内网 | 无 | 快 | 大规模测试 |

**推荐工作流**：
1. 本地测试（公网）：验证基本逻辑（1-3 天，费用 2-6 元）
2. 集群测试（内网）：大规模验证（免费）
3. 集群运行（内网）：正式计算（免费）
