# 推理模块接口规范

## 概述

引擎（实盘 / 历史回测）每轮因子计算完成后，会自动调用推理模块生成仓位。

推理模块是一个标准 Python 模块，需实现 `inference()` 函数。跟因子策略模块实现 `factor_calculation()` 完全同构——引擎通过 `importlib` 动态加载，调用约定函数。

## 接口定义

### 必须实现

```python
def inference(date_str: str, end_time: str,
              prev_day_factors_df: pd.DataFrame,
              intraday_factors_df: pd.DataFrame,
              daily_basic_df: pd.DataFrame,
              portfolio_context: PortfolioContext | None = None) -> pd.DataFrame:
    """
    引擎每轮调用一次。

    参数:
        date_str:             交易日, 如 "20260605"
        end_time:             当前时间切片, 如 "093000"
        prev_day_factors_df:  前一交易日因子结果（日频特征）
        intraday_factors_df:  当轮因子计算结果（分钟级特征）
        daily_basic_df:       daily_basic 市场数据
                              (risk factors, industry factors, daily features)
        portfolio_context:    当前账户上下文。实盘由引擎注入；回测/无配置时为 None。
                              老版本 inference 不声明该参数也兼容。

    返回:
        DataFrame, 至少包含以下列:
            code      str    股票代码, 如 "000001.SZ"
            pred      float  模型预测值
            position  float  仓位权重 (>0 做多, <0 做空, 0 无仓位)

    异常:
        函数内部自行 try/except, 不要抛出未捕获异常。
        如果推理失败, 返回空 DataFrame 即可。
    """
```

## 四份输入数据

### 1. prev_day_factors_df — 前一交易日日频因子

| 属性 | 说明 |
|------|------|
| 来源 | 前一个交易日的因子计算结果 |
| 频率 | 日频（每天一份） |
| 内容 | 与 `intraday_factors_df` 列名相同 |
| 首日 | 可能为 `None`（回测第一天、实盘启动日无历史） |

实盘：引擎启动时从 `FACTOR_OUTPUT_PATH` 加载前一交易日的因子 CSV。
回测：引擎在日期循环中保留上一日的 `test` DataFrame。

### 2. intraday_factors_df — 当轮分钟级因子

| 属性 | 说明 |
|------|------|
| 来源 | 当轮因子计算结果（`factor_calculation()` 的合并输出） |
| 频率 | 分钟级（每 COMPUTE_INTERVAL 秒一轮） |
| 内容 | 每行一只股票，列为策略返回的各因子 |

```
   date      code       factor_a  factor_b  factor_c  ...  data_latency_ms
0  20260605  000001.SZ  1.23      0.45      -0.12     ...  150.2
1  20260605  000002.SZ  0.87      1.34      0.56      ...  148.7
...
```

### 3. daily_basic_df — 市场数据

| 属性 | 说明 |
|------|------|
| 来源 | 引擎已加载的 daily_basic |
| 内容 | risk factors、industry factors、daily features |

常见列：

| 列 | 说明 |
|----|------|
| `ID_QI` | 6 位股票代码, 如 "000001" |
| `SECURITY_ID` | 整数 ID |
| `BETA`, `MOMENTUM`, `SIZE`, ... | 10 个 risk factors |
| `Agriculture`, `Banks`, `Steel`, ... | 行业分类因子 |
| `open`, `high`, `low`, `close`, `volume`, ... | 日线特征 |

### 4. portfolio_context — 当前账户上下文

该参数用于把 QMT 当前持仓、资金、委托、成交传给交易员推理模块。引擎负责从
QMT 数据导出文件 / SFTP / 后续 broker adapter 获取账户快照，交易员模块只消费
这个只读对象，不直接读 QMT 文件、不直接下单。

`portfolio_context` 类型为 `quant_platform.inference.interface.PortfolioContext`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `account_id` | str | 资金账号 |
| `broker` | str | 来源，如 `qmt` |
| `account_type` | str | 账号类型，如 `2` 股票、`3` 信用 |
| `as_of` | str | 快照时间，如 `20260607 09:35:00` |
| `source` | str | 快照来源，如 `sftp://.../exports` |
| `positions` | DataFrame | 当前持仓 |
| `account` | DataFrame | 当前资金/资产 |
| `orders` | DataFrame | 当日委托 |
| `deals` | DataFrame | 当日成交 |
| `meta` | dict | 额外元信息，如文件 mtime、延迟、校验状态 |

建议标准化后的 `positions` 至少包含：

| 列名 | 类型 | 说明 |
|------|------|------|
| `code` | str | 标准股票代码，如 `600000.SH` |
| `current_volume` | int | 当前持仓数量 |
| `available_volume` | int | 当前可卖数量 |
| `market_value` | float | 当前市值 |
| `cost_price` | float | 成本价，可选 |
| `last_price` | float | 最新价，可选 |

建议标准化后的 `account` 至少包含：

| 列名 | 类型 | 说明 |
|------|------|------|
| `account_id` | str | 资金账号 |
| `total_asset` | float | 总资产 |
| `available_cash` | float | 可用资金 |
| `market_value` | float | 持仓市值 |

交易员模块可以用它做当前仓位感知，例如避免重复买入、限制卖出数量、根据现金
调整目标仓位。但最终仍建议返回“目标仓位/目标股数”，由引擎或 QMT adapter
统一转换成 QMT 下单文件。

## 输出 DataFrame 格式

| 列名 | 类型 | 必须 | 说明 |
|------|------|------|------|
| `code` | str | 是 | 股票代码 |
| `pred` | float | 是 | 模型预测值 |
| `position` | float | 是 | 仓位权重 |

可以额外返回任意列，引擎不会丢弃。

### position 权重约定

- `position > 0`: 做多
- `position < 0`: 做空
- `position == 0`: 无仓位

常见做法（等权）：
```
top 10% 预测值 → position = +1.0/k
bottom 10% 预测值 → position = -1.0/k
其余 → 不返回
```

## CLI 脚本适配指南

交易员现有 CLI 推理脚本通常长这样：

```python
def main():
    args = parse_args()
    model, features, medians, feature_transform = load_model_artifact(Path(args.model))
    factors = load_factor_results(...)   # OSS 加载因子
    market = load_market_frames(...)     # OSS 加载市场数据
    df = merge_market_features(factors, market)
    x = build_feature_matrix(df, features, medians, feature_transform)
    pred = model.predict(x)
    positions = make_tail_positions(pred, ...)
```

适配为推理模块，3 步：

### 1. 模型预加载

```python
_MODEL = None

def _get_model():
    global _MODEL
    if _MODEL is None:
        model_path = Path(__file__).parent / "model.joblib"
        _MODEL = joblib.load(model_path)
    return _MODEL
```

### 2. 实现 inference()

```python
def inference(date_str, end_time, prev_day_factors_df, intraday_factors_df,
              daily_basic_df, portfolio_context=None):
    artifact = _get_model()
    model = artifact["model"]
    features = artifact["features"]
    medians = artifact["medians"]
    feature_transform = artifact.get("feature_transform")

    # 用 intraday_factors_df 作为主要输入
    df = normalize_factor_keys(intraday_factors_df)

    # 如需合并前一日因子
    if prev_day_factors_df is not None and not prev_day_factors_df.empty:
        # ... merge logic ...

    # 如需合并市场数据
    if daily_basic_df is not None and not daily_basic_df.empty:
        # ... merge logic ...

    x = build_feature_matrix(df, features, medians, feature_transform)
    df["pred"] = model.predict(x)
    df["position"] = make_tail_positions(df["pred"], df["_date"], tail_frac=0.10)

    if portfolio_context is not None and not portfolio_context.positions.empty:
        current_pos = portfolio_context.positions[["code", "current_volume", "available_volume"]]
        df = df.merge(current_pos, on="code", how="left")

    return df[df["position"] != 0.0][["code", "pred", "position"]]
```

### 3. 去掉不需要的代码

| 原有代码 | 替代方式 |
|----------|----------|
| `load_factor_results()` | `intraday_factors_df` 直接传入 |
| `load_market_frames()` | `daily_basic_df` 直接传入 |
| 前一天因子加载 | `prev_day_factors_df` 直接传入 |
| CLI 参数、OSS 下载 | 引擎处理 |

## 文件组织

```
quant_platform/inference/examples/
  my_inference.py       ← 推理模块, 实现 inference()
  model.joblib          ← 模型文件
```

## 部署

**实盘**：K8s 环境变量
```yaml
- name: INFERENCE_MODULE
  value: "quant_platform.inference.examples.my_inference"
- name: PORTFOLIO_CONTEXT_MODULE
  value: "quant_platform.broker.qmt_context"
```

`PORTFOLIO_CONTEXT_MODULE` 可选。配置后模块需实现：

```python
def get_portfolio_context(date_str: str, end_time: str) -> PortfolioContext:
    ...
```

如果不配置，`portfolio_context` 为 `None`，旧交易员模块不受影响。

**回测**：代码调用
```python
calc_factors_by_date_range(
    ...,
    inference_handler=inference,  # 传入 inference 函数
)
```

## 完整样例

参见 `example_ols_inference.py`。

## 注意事项

1. **不要在 `inference()` 里读写文件** — 引擎已处理因子、市场数据和账户上下文
2. **推理模块应自包含** — 不依赖 `quant_platform` 其他模块
3. **异常自行捕获** — 返回空 DataFrame，不要抛异常
4. **性能** — 推理在 fork 子进程中执行（实盘），尽量 < 5 秒
5. **模型文件** — bake 进 Docker 镜像，避免运行时下载
6. **不要在推理模块直接下单** — 返回目标，由引擎/QMT adapter 统一转换
