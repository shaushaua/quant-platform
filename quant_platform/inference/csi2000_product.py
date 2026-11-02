# -*- coding: utf-8 -*-
"""中证2000独立交易信号（新产品，与全指信号并行产出,引擎内 schedule 触发）。

与实盘全指信号并列、互不影响:
  - 同一套因子(protected_eillen 全市场计算) + 同一个模型(models_*.pkl)
  - 池 = 中证2000成分(universe 限定, 纯2000选股)
  - 基准 = 中证2000 — 通过 inference(benchmark_code="932000") 参数传入,
    不写 os.environ, 同一引擎进程内与全指信号(000985)互不污染
  - 资金/持仓/委托 完全独立 —— 通过 BrokerAdapter 注入:
      * 现在: VirtualBrokerAdapter (CSV 模拟持仓, 虚拟 1000 万)
      * 将来: 实现同接口的新券商 API 适配器 (查仓 get_positions /
              下单 push_orders), 优化器链路零改动

触发方式:
  引擎内: COMPUTE_SCHEDULES 含 "csi2000_signal" (schedule.py 注册,
          CSI2000_SIGNAL_TRIGGER_TIME 默认 15:12, daily 因子产出后)
  手动:   scripts/csi2000_signal.py --date YYYYMMDD (同一核心逻辑)

输出: {OUT_DIR}/csi2000_targets_{date}.csv    目标持仓(信号)
       {OUT_DIR}/csi2000_orders_{date}.csv    委托差额(虚盘落盘)
       {OUT_DIR}/csi2000_positions_{date}.csv 模拟持仓(次日 current)
"""
from __future__ import annotations

import io
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

BENCHMARK = "932000"
VIRTUAL_CAPITAL = 10_000_000.0
OUT_DIR = Path(os.environ.get("CSI2000_OUT_DIR", "/data/factors/csi2000"))


# ══════════════════════════════════════════════════════════════════
# 券商适配器: 中证2000产品的仓位/下单通道
# 将来接新券商 API 时实现本接口即可, 优化器与信号链路零改动。
# ══════════════════════════════════════════════════════════════════

class BrokerAdapter(ABC):
    """中证2000产品与券商 API 的边界接口。

    约定:
      get_positions 返回 PortfolioContext(positions/account)。
        positions 列: code(6位) / qty(float) / last_price(float)
        account 列: total_asset / available_cash / market_value
      push_orders 接收订单 DataFrame(code/side/volume/price), 由适配器
        翻译成券商 API 调用。虚盘实现只落盘。
    """

    name: str = "base"

    @abstractmethod
    def get_positions(self, trading_day: str) -> "PortfolioContext":
        """拉取产品当前持仓(实盘=券商查仓 API)。"""

    @abstractmethod
    def push_orders(self, orders: pd.DataFrame, trading_day: str) -> bool:
        """推送委托(实盘=券商下单 API)。返回是否成功。"""


class VirtualBrokerAdapter(BrokerAdapter):
    """虚拟适配器(默认): CSV 模拟持仓 + 订单落盘, 不打真实委托。"""

    name = "virtual"

    def _latest_holdings(self, trading_day: str) -> pd.DataFrame:
        prev = sorted(OUT_DIR.glob("csi2000_positions_*.csv"))
        prev = [p for p in prev if p.stem != f"csi2000_positions_{trading_day}"]
        if not prev:
            return pd.DataFrame()
        h = pd.read_csv(prev[-1])
        return h if not h.empty else pd.DataFrame()

    def get_positions(self, trading_day: str) -> "PortfolioContext":
        from quant_platform.inference.interface import PortfolioContext

        holdings = self._latest_holdings(trading_day)
        if holdings.empty:
            account = pd.DataFrame([{
                "total_asset": VIRTUAL_CAPITAL,
                "available_cash": VIRTUAL_CAPITAL,
                "market_value": 0.0,
            }])
            logger.info("[csi2000] 空仓起步(虚拟 %.0f万)", VIRTUAL_CAPITAL / 1e4)
        else:
            mv = float((holdings["qty"].astype(float) * holdings["last_price"].astype(float)).sum())
            account = pd.DataFrame([{
                "total_asset": VIRTUAL_CAPITAL,
                "available_cash": 0.0,
                "market_value": mv,
            }])
            logger.info("[csi2000] 模拟持仓 %d 只, 市值 %.0f万", len(holdings), mv / 1e4)
        return PortfolioContext(
            account_id="csi2000-virtual", broker=self.name,
            positions=holdings, account=account, meta={},
        )

    def push_orders(self, orders: pd.DataFrame, trading_day: str) -> bool:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUT_DIR / f"csi2000_orders_{trading_day}.csv"
        orders.to_csv(path, index=False)
        logger.info("[csi2000] (虚拟)订单落盘: %s (%d 笔)", path, len(orders))
        return True


def get_broker(name: str) -> BrokerAdapter:
    """适配器注册表。实盘接入时在此注册新适配器, 例如:
        CSI2000_BROKER=new_broker → NewBrokerAdapter(api_key=...)
    """
    brokers = {"virtual": VirtualBrokerAdapter}
    if name not in brokers:
        raise RuntimeError(f"未知 CSI2000_BROKER={name!r}, 可选: {list(brokers)}")
    return brokers[name]()


# ══════════════════════════════════════════════════════════════════
# 数据加载
# ══════════════════════════════════════════════════════════════════

def load_composition_csi2000(trading_day: str) -> pd.DataFrame:
    """优先 OSS composition(ZICN修复后含932000), fallback MySQL 直查。"""
    from quant_platform.data.oss_loader import OSSDataLoader

    oss = OSSDataLoader(base_path="2026")
    key = f"2026/2026{trading_day[4:6]}/{trading_day}/{trading_day}_composition.parquet"
    try:
        obj = oss._oss_bucket.get_object(key)
        comp = pd.read_parquet(io.BytesIO(obj.read()))
        sub = comp[comp["INDEX_CODE"].astype(str) == BENCHMARK]
        if not sub.empty:
            sub = sub.copy()
            if "trade_date" not in sub.columns:
                if "date" in sub.columns:
                    sub["trade_date"] = sub["date"]
                elif "TS" in sub.columns:
                    sub["trade_date"] = (
                        pd.to_datetime(sub["TS"], errors="coerce")
                        .dt.strftime("%Y%m%d")
                    )
                else:
                    sub["trade_date"] = trading_day
            sub["trade_date"] = (
                pd.to_datetime(sub["trade_date"], errors="coerce")
                .dt.strftime("%Y%m%d")
                .fillna(trading_day)
            )
            sub["ID_QI"] = sub["ID_QI"].astype(str).str.zfill(6)
            logger.info("[csi2000] composition OSS 命中: %d 只成分", len(sub))
            return sub
        logger.info("[csi2000] OSS composition 无 932000(旧数据), fallback MySQL")
    except Exception as exc:
        logger.info("[csi2000] OSS composition 读取失败(%s), fallback MySQL", exc)

    from quant_platform.data.mysql_loader import MySQLLoader
    mysql = MySQLLoader()
    mysql.connect()
    df = mysql.get_index_weight([f"{BENCHMARK}.ZICN"], trading_day)
    if df.empty:
        return df
    # get_index_weight 无日期列; 补成与 OSS composition 同构(TS/trade_date),
    # 否则 inference 的 long 分支无法按日筛选、.so 缺 trade_date 列会失败。
    df = df.copy()
    td_dash = f"{trading_day[:4]}-{trading_day[4:6]}-{trading_day[6:8]}"
    df["TS"] = td_dash
    df["trade_date"] = td_dash
    logger.info("[csi2000] MySQL fallback composition: %d 只(trade_date=%s)",
                len(df), td_dash)
    return df


def _daily_factor_key(trading_day: str) -> str:
    return (f"live-factors/2026/2026{trading_day[4:6]}/{trading_day}"
            f"/daily-feature/daily.parquet")


def load_daily_factors(trading_day: str) -> pd.DataFrame:
    """加载当日正式 daily 因子(15:10 schedule 产出, OSS live-factors)。"""
    import oss2
    from quant_platform.data.oss_loader import OSSDataLoader

    oss = OSSDataLoader(base_path="2026")
    auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
    rb = oss2.Bucket(auth, os.environ["OSS_ENDPOINT"], "stock-mdl-data-result")
    df = pd.read_parquet(io.BytesIO(rb.get_object(_daily_factor_key(trading_day)).read()))
    logger.info("[csi2000] 当日 daily 因子: %d 行, %d 列", len(df), len(df.columns))
    return df


def daily_factors_ready(trading_day: str) -> bool:
    """当天 daily 因子是否已上传 OSS(引擎 15:10 schedule 产出)。"""
    import oss2
    from quant_platform.data.oss_loader import OSSDataLoader

    oss = OSSDataLoader(base_path="2026")
    auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
    rb = oss2.Bucket(auth, os.environ["OSS_ENDPOINT"], "stock-mdl-data-result")
    try:
        rb.head_object(_daily_factor_key(trading_day))
        return True
    except Exception:
        return False


def _load_close_prices(trading_day: str) -> pd.Series:
    from quant_platform.data.oss_loader import OSSDataLoader
    oss = OSSDataLoader(base_path="2026")
    key = f"2026/2026{trading_day[4:6]}/{trading_day}/{trading_day}_daily_basic_data.parquet"
    try:
        obj = oss._oss_bucket.get_object(key)
        db = pd.read_parquet(io.BytesIO(obj.read()))
        return db.set_index(db["ID_QI"].astype(str).str.zfill(6))["close"].astype(float)
    except Exception:
        return pd.Series(dtype=float)


# ══════════════════════════════════════════════════════════════════
# 信号主流程
# ══════════════════════════════════════════════════════════════════

def run_csi2000_signal(trading_day: str,
                       broker: Optional[BrokerAdapter] = None) -> Optional[pd.DataFrame]:
    """产出当日中证2000 targets。供引擎 schedule 与手动 CLI 共用。

    返回 targets DataFrame; 失败抛异常(引擎侧 catch 并记日志,不影响其他 schedule)。
    """
    broker = broker or get_broker(os.environ.get("CSI2000_BROKER", "virtual"))

    comp = load_composition_csi2000(trading_day)
    if comp.empty:
        raise RuntimeError("中证2000成分获取失败(检查 ZICN 修复是否已部署)")
    universe_codes = sorted(comp["ID_QI"].astype(str).str.zfill(6).unique())
    universe_df = pd.DataFrame({"code": universe_codes})
    logger.info("[csi2000] universe: %d 只(纯2000池)", len(universe_codes))

    factors = load_daily_factors(trading_day)
    ctx = broker.get_positions(trading_day)

    from quant_platform.inference.tree_model_inference import inference
    targets = inference(
        date_str=trading_day,
        end_time="150000",
        prev_day_factors_df=factors,
        intraday_factors_df=factors,
        daily_basic_df=None,           # 内部 load_production_daily_basic 处理
        trading_universe_df=universe_df,
        index_composition_df=comp,     # long 格式, inference 内按 benchmark 过滤
        portfolio_context=ctx,
        benchmark_code=BENCHMARK,      # 参数传递,不污染进程 env(全指仍是 000985)
    )
    if targets is None or targets.empty:
        raise RuntimeError("inference 未产出目标持仓")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tgt_path = OUT_DIR / f"csi2000_targets_{trading_day}.csv"
    targets.to_csv(tgt_path, index=False)
    logger.info("[csi2000] targets %d 只 → %s", len(targets), tgt_path)

    # 订单 = targets vs 持仓 的差额(方向/数量), 走 broker 通道(虚盘落盘/实盘API)
    orders = _targets_to_orders(targets, ctx, trading_day)
    if not orders.empty:
        broker.push_orders(orders, trading_day)

    # 虚盘: 更新模拟持仓文件(次日 get_positions 用)
    if broker.name == "virtual":
        _write_virtual_positions(trading_day, targets, getattr(ctx, "positions", pd.DataFrame()))
    return targets


def _targets_to_orders(targets: pd.DataFrame, ctx, trading_day: str) -> pd.DataFrame:
    """targets(目标权重) vs 当前持仓 → 订单(code/side/volume/price)。"""
    pos = getattr(ctx, "positions", pd.DataFrame())
    px = _load_close_prices(trading_day)
    tgt = targets[["_code6", "position"]].copy() if "_code6" in targets.columns \
        else targets[["code", "position"]].rename(columns={"code": "_code6"})
    tgt["_code6"] = tgt["_code6"].astype(str).str.split(".").str[0].str.zfill(6)
    cur_qty: pd.Series = pd.Series(dtype=float)
    if not pos.empty and "qty" in pos.columns:
        cur_qty = pos.set_index(pos["code"].astype(str).str.zfill(6))["qty"].astype(float)

    rows = []
    target_codes = set(tgt["_code6"])

    # 1) 调仓: 目标内每只票 计算 目标量-持仓量 的差额单
    for _, r in tgt.iterrows():
        code6, w = r["_code6"], float(r["position"])
        price = float(px.get(code6, 0.0))
        if price <= 0:
            continue
        want = int(w * VIRTUAL_CAPITAL / price / 100) * 100
        have = float(cur_qty.get(code6, 0.0))
        delta = want - have
        if abs(delta) < 100:
            continue
        rows.append({
            "code": code6, "side": "buy" if delta > 0 else "sell",
            "volume": abs(delta), "price": round(price, 2),
        })

    # 2) 清仓: 持有但不在新目标中的票 → 全量卖单(否则虚拟持仓静默删票、
    #    回放与订单不一致; 实盘适配器接入后同样依赖这里生成清仓委托)
    for code6, have in cur_qty.items():
        if code6 in target_codes or have < 100:
            continue
        price = float(px.get(code6, 0.0))
        if price <= 0:
            # 无价格无法卖: 持仓保留在 _write_virtual_positions 中,
            # 待次日有价格再生成清仓单(不静默删除)
            logger.warning("[csi2000] %s 已移出目标但无收盘价, 本轮不生成卖单(保留持仓)",
                           code6)
            continue
        rows.append({
            "code": code6, "side": "sell",
            "volume": int(have // 100) * 100, "price": round(price, 2),
        })
    return pd.DataFrame(rows)


def _write_virtual_positions(trading_day: str, targets: pd.DataFrame,
                             prev_positions: pd.DataFrame) -> None:
    """按 targets 写模拟持仓; 无价格的遗留持仓原样保留(与订单一致)。

    保留条件: 旧持仓不在新目标 且 无收盘价(清仓单未生成) → 行原样搬运,
    避免出现"没有卖出记录但持仓消失"。
    """
    px = _load_close_prices(trading_day)
    rows = []
    target_codes = set()
    for _, r in targets.iterrows():
        code6 = str(r.get("_code6") or r.get("code")).split(".")[0].zfill(6)
        target_codes.add(code6)
        w = float(r["position"])
        price = float(px.get(code6, 0.0))
        qty = int(w * VIRTUAL_CAPITAL / price / 100) * 100 if price > 0 else 0
        rows.append({"code": code6, "weight": w, "last_price": price, "qty": qty})
    if not prev_positions.empty and "code" in prev_positions.columns:
        for _, r in prev_positions.iterrows():
            code6 = str(r["code"]).split(".")[0].zfill(6)
            if code6 in target_codes:
                continue
            price = float(px.get(code6, 0.0))
            if price <= 0 and float(r.get("qty", 0) or 0) >= 100:
                rows.append({"code": code6, "weight": 0.0,
                             "last_price": float(r.get("last_price", 0) or 0),
                             "qty": int(float(r.get("qty", 0) or 0))})
    sim = pd.DataFrame(rows)
    path = OUT_DIR / f"csi2000_positions_{trading_day}.csv"
    sim.to_csv(path, index=False)
    used = float((sim["qty"] * sim["last_price"]).sum()) if len(sim) else 0.0
    logger.info("[csi2000] 虚拟持仓 → %s (建仓 %.0f万 / %.0f万)",
                path, used / 1e4, VIRTUAL_CAPITAL / 1e4)
