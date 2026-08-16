# local_test 使用文档

`local_test.py` 用于在本地读取 OSS 历史行情并验证因子策略。它支持指定股票、全市场、单日、日期范围、单进程和与线上 worker 一致的多进程批量调用。

## 前置条件

在 `quant-platform` 根目录执行命令，并配置 OSS 凭据：

```bash
export OSS_ACCESS_KEY_ID='your_access_key'
export OSS_ACCESS_KEY_SECRET='your_secret_key'
```

可选环境变量：

```bash
export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'
export OSS_DATA_BUCKET='quant-mdl-data'
```

未设置 `OSS_ENDPOINT` 时，程序会尝试自动选择杭州内网或公网地址。

## 命令格式

```bash
python local_test.py STRATEGY \
  --date DATE_OR_RANGE \
  (--codes CODE_LIST | --all) \
  [--output OUTPUT] \
  [--format csv|parquet|json] \
  [--processes N] \
  [--verbose]
```

## 参数说明

| 参数 | 必需 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `STRATEGY` | 是 | - | 策略 Python 文件路径 |
| `--date` | 是 | - | 单日 `YYYYMMDD` 或范围 `YYYYMMDD-YYYYMMDD` |
| `--codes` | 二选一 | - | 指定股票，多个代码使用逗号分隔 |
| `--all` | 二选一 | false | 使用全市场模式 |
| `--output` | 否 | `./local_test_result` | 输出文件路径，不含扩展名 |
| `--format` | 否 | `csv` | 输出格式：`csv`、`parquet` 或 `json` |
| `--processes` | 否 | `1` | 计算进程数；大于1时启用线上批量路径 |
| `--verbose`, `-v` | 否 | false | 输出详细运行日志 |

## 股票范围规则

当策略的 `securities` 为空时，`--codes` 和 `--all` 至少要提供一个。仅省略 `--codes` 不会自动切换到全市场；只有策略本身提供了非空 `securities` 时，才可以省略这两个参数。

具体行为如下：

| 输入 | 策略 `securities` | 实际行为 |
| --- | --- | --- |
| `--codes "000001.SZ,000002.SZ"` | 任意 | 只计算参数指定的股票 |
| `--all` | 空列表 | 引擎加载并计算全市场 |
| `--all` | 非空列表 | 最多计算策略列表中的前10只 |
| 两者都不传 | 空列表 | 报错并退出 |
| 两者都不传 | 非空列表 | 最多计算策略列表中的前10只 |

对于 `securities=[]` 的全市场策略，必须使用 `--all` 才会计算全市场：

```bash
python local_test.py strategy.py --date 20240102 --all
```

## 调用模式

### 单进程

`--processes 1` 是默认模式。引擎一次处理一只股票，但仍使用统一批量签名调用策略：

```python
factor_calculation(
    {code: stock_data},
    [code],
    date,
    [end_time],
)
```

此时字典和代码列表中只有一只股票。该模式适合断点调试和检查单票结果。

### 多进程批量

`--processes` 大于1时，程序启用与线上 worker 一致的 streaming 和多进程批量路径：

```python
factor_calculation(
    {code_1: stock_data_1, code_2: stock_data_2, ...},
    [code_1, code_2, ...],
    date,
    [end_time_1, end_time_2, ...],
)
```

一次策略调用收到多少只股票由 `FACTOR_TASK_CODE_BATCH_SIZE` 控制。

## 批次环境变量

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_BATCH_SIZE` | `200` | 每次从行情文件加载的股票数 |
| `FACTOR_BATCH_SIZE` | `100` | 一个内存计算批次包含的股票数 |
| `FACTOR_TASK_CODE_BATCH_SIZE` | 等于 `FACTOR_BATCH_SIZE` | 一次策略调用收到的股票数 |

批次大小满足以下约束：

```text
FACTOR_TASK_CODE_BATCH_SIZE <= FACTOR_BATCH_SIZE <= DATA_BATCH_SIZE
```

当前线上回测使用：

```bash
DATA_BATCH_SIZE=200
FACTOR_BATCH_SIZE=100
FACTOR_TASK_CODE_BATCH_SIZE=10
```

要在本地复现线上策略调用粒度，应显式设置这些参数并使用多进程。

## 使用示例

### 单票调试

```bash
python local_test.py strategy.py \
  --date 20240102 \
  --codes "000001.SZ" \
  --output local_test_result \
  --format csv \
  --processes 1 \
  --verbose
```

### 多票单进程

```bash
python local_test.py strategy.py \
  --date 20240102 \
  --codes "000001.SZ,000002.SZ,600000.SH" \
  --processes 1
```

虽然指定了多只股票，单进程模式仍然逐票调用策略。

### 多票批量调用

```bash
FACTOR_TASK_CODE_BATCH_SIZE=10 \
python local_test.py strategy.py \
  --date 20240102 \
  --codes "000001.SZ,000002.SZ,000004.SZ,000005.SZ" \
  --output local_test_result \
  --format parquet \
  --processes 8 \
  --verbose
```

### 全市场并复现线上批次

```bash
DATA_BATCH_SIZE=200 \
FACTOR_BATCH_SIZE=100 \
FACTOR_TASK_CODE_BATCH_SIZE=10 \
python local_test.py strategy.py \
  --date 20240102 \
  --all \
  --output local_test_result_all \
  --format parquet \
  --processes 8 \
  --verbose
```

### 日期范围

```bash
DATA_BATCH_SIZE=200 \
FACTOR_BATCH_SIZE=100 \
FACTOR_TASK_CODE_BATCH_SIZE=10 \
python local_test.py strategy.py \
  --date 20240102-20240105 \
  --all \
  --output local_test_result_range \
  --format parquet \
  --processes 8
```

## 输出

程序根据 `--format` 在 `--output` 后添加扩展名：

```text
local_test_result.csv
local_test_result.parquet
local_test_result.json
```

策略无结果时不会产生有效因子记录。批量测试时应同时检查日志中的以下信息：

```text
[ForkWorker] codes=10 ... results=10
日期计算完成 ... records=...
```

如果出现 `task failed`、`Traceback` 或最终记录数为0，应先修复策略错误，不应将快速结束视为性能提升。

## 常见问题

### 不传 `--codes` 是否自动全量？

不会。全市场必须显式传入 `--all`。如果策略的 `securities` 也是空列表，两个参数都不传会直接报错。

### `--all` 是否只计算前10只？

取决于策略的 `securities`：

- `securities=[]`：计算全市场。
- `securities` 非空：本地测试最多取策略列表前10只。

### `--processes 8` 是否意味着一次给策略8只股票？

不是。`--processes` 控制进程数，一次策略调用的股票数由 `FACTOR_TASK_CODE_BATCH_SIZE` 控制。

### 如何完全复现当前线上调用方式？

使用：

```bash
DATA_BATCH_SIZE=200 \
FACTOR_BATCH_SIZE=100 \
FACTOR_TASK_CODE_BATCH_SIZE=10 \
python local_test.py strategy.py --date 20240102 --all --processes 8 --verbose
```
