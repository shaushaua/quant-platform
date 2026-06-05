# Model Training And Inference

## Training Output

`scripts/train_model_from_oss.py` writes the model artifact here:

```text
{output_dir}/model.joblib
```

The joblib file is a dict:

```python
{
    "model": model,          # any object with predict(X)
    "features": feature_cols,
    "medians": medians,      # training medians for missing-value fill
    "feature_transform": <cloudpickle bytes>,  # embedded callable
    "feature_transform_format": "cloudpickle",
    "feature_transform_name": "cross_section_zscore_by_date",
    "label_transform": "cross_section_zscore_by_date",
    "date_col": "_date",
}
```

This is intentionally model-agnostic. OLS, LightGBM, XGBoost, or another
sklearn-like model can use the same inference path as long as it supports
`predict(X)`.

## Train Example

```bash
python scripts/train_model_from_oss.py \
  --prefix eillen_protected \
  --result-bucket stock-mdl-data-result \
  --data-bucket quant-mdl-data \
  --start-date 20250102 \
  --end-date 20250124 \
  --shard-mode auto \
  --model ols \
  --price-col adj_close \
  --valid-start-date 20250123 \
  --no-save-train-table \
  --no-save-predictions
```

## CLI Inference From OSS

Production-like inference reads generated factor files from OSS and reads
`daily_basic` market data from OSS. It uses the same local cache as training.
`scripts/infer_model.py` is standalone: dev does not need
`scripts/train_model_from_oss.py`. If the model artifact says
`feature_transform_format=cloudpickle`, inference loads the embedded callable
from `model.joblib` and applies it before prediction. The production script does
not need the training script source.

```bash
python scripts/infer_model.py \
  --model artifacts/model_validation/model.joblib \
  --prefix eillen_protected \
  --result-bucket stock-mdl-data-result \
  --data-bucket quant-mdl-data \
  --start-date 20250123 \
  --end-date 20250124 \
  --shard-mode auto \
  --output artifacts/model_validation/predictions.csv \
  --save-market-data
```

The output keeps available id columns:

```text
date, code, _date, _code6, pred, position
```

By default, `position` keeps only top/bottom 10% per date:

- top 10% highest `pred`: equal-weight long, long weights sum to `+1`
- bottom 10% lowest `pred`: equal-weight short, short weights sum to `-1`
- middle stocks are dropped from the output

Use `--keep-zero-positions` to keep all stocks with middle names set to
`position=0`.

Minimum files to send to dev:

```text
scripts/infer_model.py
artifacts/model_validation/.../model.joblib
```

With `--save-market-data`, the script also writes:

```text
{output_dir}/market_data/risk_factors.csv
{output_dir}/market_data/industry_factors.csv
{output_dir}/market_data/daily_features.csv
```

## Local File Inference

For offline debugging only, input can also be `.csv`, `.jsonl`, `.jsonl.gz`, or
`.parquet`.

```bash
python scripts/infer_model.py \
  --model artifacts/model_validation/model.joblib \
  --input artifacts/factors/eillen_protected_202501_3days/factors.csv \
  --output artifacts/model_validation/predictions.csv
```

## Python Inference Snippet

```python
from pathlib import Path

import joblib
import pandas as pd

artifact = joblib.load("artifacts/model_validation/model.joblib")
model = artifact["model"]
features = artifact["features"]
medians = artifact["medians"]
feature_transform = artifact.get("feature_transform", "none")
feature_transform_format = artifact.get("feature_transform_format")

df = pd.read_csv("features_to_score.csv")

if feature_transform_format == "cloudpickle":
    import cloudpickle

    transform = cloudpickle.loads(feature_transform)
    x = transform(df=df, features=features, medians=medians)
else:
    x = pd.DataFrame(index=df.index)
    for feature in features:
        if feature in df.columns:
            x[feature] = pd.to_numeric(df[feature], errors="coerce")
        else:
            x[feature] = float("nan")
    x = x.fillna(medians.reindex(features)).fillna(0.0)

df["pred"] = model.predict(x)
df[["date", "code", "pred"]].to_csv("predictions.csv", index=False)
```
