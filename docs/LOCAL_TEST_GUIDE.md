# 本地策略测试操作手册

> 面向交易员：在提交集群任务之前，先在本地验证策略代码是否正确。

---

## 一、为什么要本地测试？

每次提交集群任务的代价：
- 等待镜像构建：5~10 分钟
- 等待 Pod 调度：1~2 分钟
- 如果代码写错，重头再来

本地测试可以在 **几秒钟** 内发现问题，修改后立刻重跑，不浪费集群资源。

---

## 二、一次性环境配置

### 第一步：安装 Python 依赖

```bash
cd /path/to/quant-platform
pip install -r requirements.txt
pip install duckdb   # 如果尚未安装
```

### 第二步：配置 OSS 凭证

**方式 A（推荐）：使用配置向导**

```bash
./setup_credentials.sh
```

按提示输入 AccessKey ID 和 AccessKey Secret，配置会保存到 `.env.local`。

**方式 B：手动编辑**

```bash
cp .env.local.example .env.local
# 然后编辑 .env.local，填入你的 AK/SK
```

`.env.local` 内容示例：

```bash
export OSS_ACCESS_KEY_ID='LTAI5tXXXXXXXXXXXXXXXXXX'
export OSS_ACCESS_KEY_SECRET='XXXXXXXXXXXXXXXXXXXXXXXXXX'
export OSS_DATA_BUCKET='quant-mdl-data'
# endpoint 会自动检测，也可以手动指定：
# export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'  # 公网
```

### 第三步：每次开终端加载配置

```bash
source .env.local
```

> 如果不想每次都手动 source，可以写入 `~/.zshrc`：
> ```bash
> echo "source /path/to/quant-platform/.env.local" >> ~/.zshrc
> ```

---

## 三、使用方法

### 基本格式

```bash
python local_test.py <策略文件> --date <日期> --codes "<股票代码>"
```

### 常用命令

```bash
# 测试单只股票、单天
python local_test.py taq_generator_strategy.py \
    --date 20250106 \
    --codes "000001.SZ"

# 测试多只股票、单天
python local_test.py taq_generator_strategy.py \
    --date 20250106 \
    --codes "000001.SZ,000002.SZ,600000.SH"

# 测试多天（连续日期范围）
python local_test.py taq_generator_strategy.py \
    --date 20250106-20250108 \
    --codes "000001.SZ,000002.SZ"

# 显示详细日志（推荐调试时使用）
python local_test.py taq_generator_strategy.py \
    --date 20250106 \
    --codes "000001.SZ" -v

# 指定结果输出路径
python local_test.py taq_generator_strategy.py \
    --date 20250106 \
    --codes "000001.SZ" \
    --output ./my_result.json
```

---

## 四、看懂输出

### 正常输出示例

```
============================================================
🧪 本地策略测试
============================================================

📝 加载策略: taq_generator_strategy.py
   ✓ factor_info: {'market_count': 1, 'need_l1_tick': True, 'need_l2_deal': True}
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
   字段数: 6
   字段名: ['code', 'date', 'tick_count', 'deal_count', ...]

   ✓ 所有字段都有有效值（前10条检查）

📋 样本数据（第1条）:
      code: 000001.SZ
      date: 20250106
      tick_count: 5009
      deal_count: 95370

💾 结果已保存到: ./local_test_result.json

============================================================
✅ 本地测试完成
============================================================
```

**关键检查点：**

| 输出内容 | 含义 | 正常值 |
|---------|------|--------|
| `tick=5009` | 当天 tick 条数 | 几千条，不应为 0 |
| `deal=95370` | 当天成交条数 | 几万条，不应为 0 |
| `所有字段都有有效值` | 无 NaN | 必须出现 |
| `总记录数` | 计算出的股票数 | 等于 --codes 中的股票数量 |

### 如果出现 NaN 警告

```
⚠️  警告: 发现 NaN 值的字段:
      ask_twap_1m_0947: 10 条记录
```

说明策略代码里数据读取有问题，参见第六节排查。

---

## 五、关于下载速度和缓存

### 首次运行

第一次测试某一天的数据，需要从 OSS 下载两个大文件：

| 文件 | 大小 | 公网下载时间 |
|-----|------|------------|
| deal.parquet | ~2.5 GB | 约 25~40 分钟 |
| tick.parquet | ~1.4 GB | 约 15~20 分钟 |

**首次运行总耗时约 45 分钟**，这是正常的。

### 第二次起（有缓存）

文件下载后会缓存在本地 `~/.oss_cache/` 目录。**第二次运行同一天的数据只需 3~5 秒**。

```
[INFO] 命中本地缓存，跳过下载: deal.parquet
[INFO] 命中本地缓存，跳过下载: tick.parquet
```

### 缓存管理

```bash
# 查看缓存目录大小
du -sh ~/.oss_cache/

# 查看缓存文件列表
ls -lh ~/.oss_cache/

# 清空缓存（需要重新下载）
rm -rf ~/.oss_cache/

# 自定义缓存目录（默认 ~/.oss_cache）
export OSS_LOCAL_CACHE_DIR='/data/oss_cache'
```

> 每多测试一天，缓存增加约 4 GB。注意磁盘空间。

---

## 六、常见问题排查

### 问题 1：tick=0 deal=0，结果全是 NaN

**原因**：策略代码中的属性名写错了。

```python
# ❌ 错误写法
tick_data = data.tick
deal_data = data.deal

# ✅ 正确写法
tick_data = data.l1_tick
deal_data = data.l2_deal
```

### 问题 2：结果条数不对（应该 2 条只有 1 条）

检查：
- 该股票在当天是否有数据（可能停牌）
- 股票代码格式是否正确（需要带交易所后缀：`.SZ` / `.SH`）

### 问题 3：OSS 连接失败

```
❌ 错误: 缺少环境变量: OSS_ACCESS_KEY_ID
```

```bash
# 检查环境变量是否加载
echo $OSS_ACCESS_KEY_ID

# 重新加载
source .env.local
```

```
❌ OSS 连接失败: timeout
```

网络问题，公网访问 OSS 需要能连接到 `oss-cn-hangzhou.aliyuncs.com`：

```bash
ping oss-cn-hangzhou.aliyuncs.com
```

### 问题 4：duckdb 未安装

```
[WARNING] duckdb 未安装，大文件（tick/order/deal）将不可用
```

```bash
pip install duckdb
```

### 问题 5：策略加载失败

```
❌ 加载策略失败: ...
```

检查策略文件：
1. Python 语法是否正确（`python -c "import ast; ast.parse(open('strategy.py').read())"`)
2. 是否定义了 `factor_calculation` 函数
3. 是否定义了 `factor_info` 字典

---

## 七、策略文件的正确写法

参考 `example_simple_strategy.py`：

```python
# 必须定义：告诉引擎需要哪些数据
factor_info = {
    "market_count": 1,       # 需要几日 daily_basic 历史
    "need_l1_tick": True,    # 是否需要 L1 tick 数据
    "need_l2_deal": True,    # 是否需要 L2 成交数据
    "need_l2_order": False,  # 是否需要 L2 委托数据
}

# 可选：指定股票列表（空列表表示全市场）
securities = ["000001.SZ", "000002.SZ"]

# 可选：时间切片（空字符串表示全天只算一次）
end_times = [""]

# 必须定义：因子计算函数
def factor_calculation(data, code, date, end_time):
    """
    Args:
        data: StockData 对象，包含当天数据
        code: 股票代码，如 "000001.SZ"
        date: 日期字符串，如 "20250106"
        end_time: 时间切片，如 "093000"

    Returns:
        dict: 计算结果，必须包含 "code" 和 "date" 字段
    """
    tick = data.l1_tick   # ← 注意：l1_tick，不是 tick
    deal = data.l2_deal   # ← 注意：l2_deal，不是 deal

    if tick.empty or deal.empty:
        return None  # 返回 None 表示跳过该股票

    return {
        "code": code,
        "date": date,
        "tick_count": len(tick),
        "deal_count": len(deal),
        "total_volume": deal["Volume"].sum() if "Volume" in deal.columns else 0,
    }
```

---

## 八、推荐工作流程

```
① 写策略代码
      ↓
② 本地测试（1只股票，1天）     ← 验证逻辑是否正确，有无 NaN
      ↓
③ 本地测试（2~3只股票，2~3天） ← 验证稳定性
      ↓
④ 提交集群（小规模）           ← 5~10 只股票，3~5 天
      ↓
⑤ 提交集群（完整批次）         ← 全量数据
```

**费用参考**（公网本地测试，每天约 4GB 数据）：

| 测试规模 | 流量 | 费用 | 首次耗时 | 缓存后耗时 |
|---------|------|------|---------|----------|
| 1只股票，1天 | ~4 GB | ~2 元 | ~45 分钟 | ~5 秒 |
| 2只股票，1天 | ~4 GB | ~2 元 | ~45 分钟 | ~5 秒 |
| 2只股票，3天 | ~12 GB | ~6 元 | ~135 分钟 | ~15 秒 |

> 多只股票不会增加下载量，因为所有股票都在同一个文件里。
> 多天会线性增加下载量（每天约 4 GB）。

---

## 九、查看计算结果

```bash
# 查看结果文件（格式化输出）
cat local_test_result.json | python -m json.tool

# 只看第一条
cat local_test_result.json | python -m json.tool | head -30

# 如果安装了 jq
cat local_test_result.json | jq '.[0]'
cat local_test_result.json | jq 'length'         # 总条数
cat local_test_result.json | jq '.[].code'       # 所有股票代码
```

结果文件默认保存在：`./local_test_result.json`（项目根目录）

---

## 十、确认无误后提交集群

```bash
# 提交完整批次任务
python submit_taq_batches.py
```

---

## 相关文件说明

| 文件 | 用途 |
|-----|------|
| `local_test.py` | 本地测试工具主程序 |
| `example_simple_strategy.py` | 最简单的策略示例，可以直接跑 |
| `taq_generator_strategy.py` | TAQ 因子策略 |
| `submit_taq_batches.py` | 提交集群批次任务 |
| `setup_credentials.sh` | OSS 凭证配置向导 |
| `.env.local` | 本地凭证配置文件（不提交到 git） |
| `~/.oss_cache/` | 大文件本地缓存目录 |
| `./local_test_result.json` | 本地测试计算结果 |
