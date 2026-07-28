"""
data_pipeline.py  —  Tushare-based data pipeline for convertible bond research.

Replaces the manually updated Excel file with programmatic API calls.
Outputs wide-format CSV files (rows = trading dates, columns = bond ts_codes)
that can be loaded directly by the backtest scripts.

Usage:
    python data_pipeline.py                       # full refresh
    python data_pipeline.py --start 20230101      # incremental from date

Output files (all in the same directory as this script):
    cb_price_cache.csv        可转债收盘价
    cb_convert_val_cache.csv  转换价值（正股价 × 面值 / 转股价）
    cb_bond_floor_cache.csv   纯债价值（DCF）
    cb_maturity_cache.csv     剩余期限（年）
    cb_stock_mv_cache.csv     正股总市值（万元）
    cb_balance_cache.csv      转债待偿余额（万元，来自 cb_basic）
    cb_amount_cache.csv       转债日成交额（万元）
    cb_rating_cache.csv       信用评级（字符串，ffill 到日频）
    cb_bps_cache.csv          正股每股净资产（季频 ffill）
    cb_basic_info.csv         静态基础信息（转债→正股映射、赎回价等）

Verified Tushare field names (as of 2026):
    cb_basic:  ts_code, stk_code, pay_per_year, coupon_rate, par,
               maturity_date, maturity_call_price, conv_price, remain_size
    cb_daily:  ts_code, trade_date, close, amount, cb_value, bond_value
    daily:     ts_code, trade_date, close
    daily_basic: ts_code, trade_date, total_mv
    fina_indicator: ts_code, ann_date, end_date, bps
    cb_rating: ts_code, ann_date, rating_date, rating
"""

import argparse
import os
import re
import shutil
import time
import warnings
from collections.abc import Callable
from datetime import datetime
from typing import TypeVar
from zoneinfo import ZoneInfo

import akshare as ak
import numpy as np
import pandas as pd
import requests
import tushare as ts
from tqdm import tqdm

from market_data_contracts import (
    DataContractError,
    build_conversion_price_matrix,
    build_implied_credit_spread_matrix,
    build_point_in_time_balance_matrix,
    build_point_in_time_rating_matrix,
    extract_clause_terms,
    interpolate_observed_yield_curve,
    parse_coupon_schedule,
    select_completed_weekly_dates,
    validate_balance_wan_units,
    validate_observed_source_coverage,
    validate_pricing_coverage,
    validate_stock_market_value_wan_units,
)
from token_loader import load_tushare_token

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置
# ==========================================
CHINA_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
DEFAULT_START = '20170101'
DEFAULT_END = datetime.now(CHINA_MARKET_TIMEZONE).strftime('%Y%m%d')
OUT_DIR       = os.path.dirname(os.path.abspath(__file__))

OUT_PRICE     = os.path.join(OUT_DIR, 'cb_price_cache.csv')
OUT_CV        = os.path.join(OUT_DIR, 'cb_convert_val_cache.csv')
OUT_FLOOR     = os.path.join(OUT_DIR, 'cb_bond_floor_cache.csv')
OUT_MATURITY  = os.path.join(OUT_DIR, 'cb_maturity_cache.csv')
OUT_STOCK_MV  = os.path.join(OUT_DIR, 'cb_stock_mv_cache.csv')
OUT_BALANCE   = os.path.join(OUT_DIR, 'cb_balance_cache.csv')
OUT_AMOUNT    = os.path.join(OUT_DIR, 'cb_amount_cache.csv')
OUT_RATING    = os.path.join(OUT_DIR, 'cb_rating_cache.csv')
OUT_BPS       = os.path.join(OUT_DIR, 'cb_bps_cache.csv')
OUT_BASIC     = os.path.join(OUT_DIR, 'cb_basic_info.csv')
RF_CACHE      = os.path.join(OUT_DIR, 'rf_yield_cache.csv')
OUT_CONV_PRICE = os.path.join(OUT_DIR, 'cb_conversion_price_cache.csv')
OUT_CONV_EVENTS = os.path.join(OUT_DIR, 'cb_conversion_price_events.csv')
OUT_SHARE_EVENTS = os.path.join(OUT_DIR, 'cb_share_events.csv')
OUT_CLAUSES = os.path.join(OUT_DIR, 'cb_clause_terms.csv')
OUT_CREDIT_SPREAD = os.path.join(OUT_DIR, 'cb_credit_spread_cache.csv')
CB_DAILY_CHECKPOINT_ROOT = os.path.join(OUT_DIR, '.cb_daily_checkpoint')
CB_DAILY_CHECKPOINT_EVERY = 50
T = TypeVar("T")
CB_BASIC_FIELDS = ",".join(
    [
        "ts_code",
        "bond_full_name",
        "bond_short_name",
        "cb_code",
        "cb_type",
        "stk_code",
        "stk_short_name",
        "maturity",
        "par",
        "issue_price",
        "issue_size",
        "remain_size",
        "value_date",
        "maturity_date",
        "rate_type",
        "coupon_rate",
        "add_rate",
        "pay_per_year",
        "list_date",
        "delist_date",
        "exchange",
        "conv_start_date",
        "conv_end_date",
        "conv_stop_date",
        "first_conv_price",
        "conv_price",
        "rate_clause",
        "put_clause",
        "maturity_call_price",
        "call_clause",
        "reset_clause",
        "conv_clause",
    ]
)


def call_tushare_with_retry(
    function: Callable[..., T],
    *args,
    attempts: int = 5,
    initial_delay_seconds: float = 1.0,
    **kwargs,
) -> T:
    """Retry transient HTTP failures with exponential backoff."""

    if attempts < 1:
        raise ValueError("attempts must be positive")
    delay = float(initial_delay_seconds)
    for attempt in range(1, attempts + 1):
        try:
            return function(*args, **kwargs)
        except requests.exceptions.RequestException:
            if attempt == attempts:
                raise
            print(
                f"   Tushare 网络请求失败，{delay:g} 秒后重试 "
                f"({attempt}/{attempts})..."
            )
            time.sleep(delay)
            delay *= 2.0
    raise AssertionError("unreachable")


# ==========================================
# 2. 初始化 Tushare
# ==========================================
def init_tushare():
    pro = ts.pro_api(load_tushare_token(), timeout=120)
    print("Tushare Pro 初始化成功")
    return pro


# ==========================================
# 3. 工具函数
# ==========================================
def _quarter_ranges(start: str, end: str) -> list:
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    ranges = []
    cur = s
    while cur <= e:
        nxt = min(cur + pd.DateOffset(months=3) - pd.Timedelta(days=1), e)
        ranges.append((cur.strftime('%Y%m%d'), nxt.strftime('%Y%m%d')))
        cur = nxt + pd.Timedelta(days=1)
    return ranges


def _load_existing(path: str):
    if not os.path.exists(path):
        return None
    return pd.read_csv(path, index_col=0, parse_dates=True)


def _merge_wide(existing, new: pd.DataFrame) -> pd.DataFrame:
    if existing is None:
        return new
    combined = existing.combine_first(new)
    combined.update(new)
    return combined.sort_index()


def fetch_trade_dates(pro, start: str, end: str) -> list[str]:
    """Return actual open SSE dates for bounded per-day market queries."""

    calendar = call_tushare_with_retry(
        pro.trade_cal,
        exchange='SSE',
        start_date=start,
        end_date=end,
    )
    if calendar is None or calendar.empty:
        raise DataContractError(f"trade_cal returned no dates for {start}..{end}")
    required = {'cal_date', 'is_open'}
    missing = required.difference(calendar.columns)
    if missing:
        raise DataContractError(
            f"trade_cal missing columns: {sorted(missing)}"
        )
    open_days = calendar.loc[
        pd.to_numeric(calendar['is_open'], errors='coerce') == 1,
        'cal_date',
    ].astype(str)
    dates = sorted(day for day in open_days if start <= day <= end)
    if not dates:
        raise DataContractError(f"no open trading dates for {start}..{end}")
    return dates


def resolve_completed_weekly_end(
    pro,
    *,
    start: str,
    requested_end: str,
    as_of: pd.Timestamp | None = None,
) -> str:
    """Cap a weekly rebuild at the latest fully published W-FRI week."""

    open_dates = pd.to_datetime(fetch_trade_dates(pro, start, requested_end))
    market_as_of = (
        pd.Timestamp(datetime.now(CHINA_MARKET_TIMEZONE)).tz_localize(None)
        if as_of is None
        else pd.Timestamp(as_of)
    )
    if market_as_of.tzinfo is not None:
        market_as_of = market_as_of.tz_convert(
            CHINA_MARKET_TIMEZONE
        ).tz_localize(None)
    requested_end_cutoff = (
        pd.Timestamp(requested_end).normalize()
        + pd.Timedelta(days=1)
        - pd.Timedelta(nanoseconds=1)
    )
    effective_as_of = min(market_as_of, requested_end_cutoff)
    completed_dates = select_completed_weekly_dates(
        open_dates,
        as_of=effective_as_of,
    )
    if len(completed_dates) == 0:
        raise DataContractError(
            f"no completed weekly trading date for {start}..{requested_end}"
        )
    return completed_dates[-1].strftime("%Y%m%d")


# ==========================================
# 4. 获取转债基础信息（修正字段名）
# ==========================================
def fetch_cb_basic(pro) -> pd.DataFrame:
    """
    cb_basic 实际字段（经验证）：
      ts_code, stk_code, bond_short_name, maturity_date,
      coupon_rate, pay_per_year, par, maturity_call_price,
      conv_price, remain_size,
      list_date, delist_date
    """
    print("\n[Step 1] 拉取转债基础信息 cb_basic ...")
    df = call_tushare_with_retry(
        pro.cb_basic,
        fields=CB_BASIC_FIELDS,
    )
    if df is None or df.empty:
        raise RuntimeError("cb_basic 返回空数据，请检查 Token 权限")
    required_contract_fields = {
        "ts_code",
        "stk_code",
        "maturity_date",
        "par",
        "pay_per_year",
        "maturity_call_price",
    }
    missing_contract_fields = required_contract_fields.difference(df.columns)
    if missing_contract_fields:
        raise DataContractError(
            "cb_basic missing contractual fields: "
            f"{sorted(missing_contract_fields)}"
        )

    # 日期列
    for col in ('list_date', 'delist_date', 'maturity_date'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 统一列名（对齐后续代码）
    df = df.rename(columns={
        'stk_code':    'stk_cd',        # 正股代码
        'pay_per_year': 'interest_freq', # 年付息次数
        'par':         'par_value',      # 面值
    })

    # coupon_rate 单位：% → 小数
    if 'coupon_rate' in df.columns:
        df['coupon_rate'] = pd.to_numeric(df['coupon_rate'], errors='coerce') / 100.0

    # 缺失频次不能假设为年付一次；后续定价会将其视为数据契约错误。
    if 'interest_freq' in df.columns:
        df['interest_freq'] = pd.to_numeric(df['interest_freq'], errors='coerce')

    # 面值必须来自发行条款，不能静默补 100。
    if 'par_value' in df.columns:
        df['par_value'] = pd.to_numeric(df['par_value'], errors='coerce')
    df['maturity_call_price'] = pd.to_numeric(
        df['maturity_call_price'], errors='coerce'
    )

    # conv_price（当前转股价）
    if 'conv_price' in df.columns:
        df['conv_price'] = pd.to_numeric(df['conv_price'], errors='coerce')

    # Tushare cb_basic.remain_size is reported in yuan. It is preserved in
    # source units here; point-in-time balance builders convert to 万元.
    if 'remain_size' in df.columns:
        df['remain_size'] = pd.to_numeric(df['remain_size'], errors='coerce')

    df = df.drop_duplicates(subset='ts_code', keep='last').reset_index(drop=True)
    print(f"   获取 {len(df)} 只转债基础信息，列: {list(df.columns)}")
    return df


# ==========================================
# 5. 获取转债日线（价格 + 成交额）
# ==========================================
def _load_cb_daily_checkpoints(
    checkpoint_dir: str,
) -> tuple[set[str], list[pd.DataFrame]]:
    completed_dates: set[str] = set()
    chunks: list[pd.DataFrame] = []
    if not os.path.isdir(checkpoint_dir):
        return completed_dates, chunks

    for name in sorted(os.listdir(checkpoint_dir)):
        if not name.endswith(".pkl"):
            continue
        path = os.path.join(checkpoint_dir, name)
        try:
            payload = pd.read_pickle(path)
            completed_dates.update(str(date) for date in payload["trade_dates"])
            data = payload["data"]
        except Exception as exc:
            raise DataContractError(
                f"cb_daily checkpoint 损坏，无法读取 {path}: {exc}"
            ) from exc
        if isinstance(data, pd.DataFrame) and not data.empty:
            chunks.append(data)
    return completed_dates, chunks


def _save_cb_daily_checkpoint(
    checkpoint_dir: str,
    trade_dates: list[str],
    chunks: list[pd.DataFrame],
) -> None:
    if not trade_dates:
        return
    os.makedirs(checkpoint_dir, exist_ok=True)
    payload = {
        "trade_dates": list(trade_dates),
        "data": (
            pd.concat(chunks, ignore_index=True)
            if chunks
            else pd.DataFrame()
        ),
    }
    filename = f"{trade_dates[0]}_{trade_dates[-1]}.pkl"
    path = os.path.join(checkpoint_dir, filename)
    temp_path = f"{path}.tmp"
    pd.to_pickle(payload, temp_path)
    os.replace(temp_path, path)


def _remove_cb_daily_checkpoints(checkpoint_dir: str) -> None:
    if os.path.isdir(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    if (
        os.path.isdir(CB_DAILY_CHECKPOINT_ROOT)
        and not os.listdir(CB_DAILY_CHECKPOINT_ROOT)
    ):
        os.rmdir(CB_DAILY_CHECKPOINT_ROOT)


def fetch_cb_daily(pro, start: str, end: str) -> dict:
    """
    cb_daily 实际字段（经验证）：
      ts_code, trade_date, pre_close, open, high, low, close,
      change, pct_chg, vol, amount
    """
    print(f"\n[Step 2] 拉取转债日线 cb_daily ({start} → {end}) ...")
    trade_dates = fetch_trade_dates(pro, start, end)
    checkpoint_dir = os.path.join(CB_DAILY_CHECKPOINT_ROOT, f"{start}_{end}")
    completed_dates, chunks = _load_cb_daily_checkpoints(checkpoint_dir)
    if completed_dates:
        print(
            f"   从检查点恢复 {len(completed_dates)}/{len(trade_dates)} 个交易日"
        )

    failures = []
    pending_checkpoint_dates: list[str] = []
    pending_checkpoint_chunks: list[pd.DataFrame] = []
    for trade_date in tqdm(trade_dates, desc='cb_daily'):
        if trade_date in completed_dates:
            continue
        try:
            df = call_tushare_with_retry(
                pro.cb_daily,
                trade_date=trade_date,
                fields=(
                    'ts_code,trade_date,close,amount,'
                    'cb_value,bond_value'
                ),
            )
            if df is not None and not df.empty:
                chunks.append(df)
                pending_checkpoint_chunks.append(df)
            else:
                failures.append((trade_date, "empty response for open date"))
                continue
            pending_checkpoint_dates.append(trade_date)
            if len(pending_checkpoint_dates) >= CB_DAILY_CHECKPOINT_EVERY:
                _save_cb_daily_checkpoint(
                    checkpoint_dir,
                    pending_checkpoint_dates,
                    pending_checkpoint_chunks,
                )
                pending_checkpoint_dates = []
                pending_checkpoint_chunks = []
        except Exception as ex:
            failures.append((trade_date, str(ex)))
        time.sleep(0.3)

    _save_cb_daily_checkpoint(
        checkpoint_dir,
        pending_checkpoint_dates,
        pending_checkpoint_chunks,
    )
    if failures:
        sample = "; ".join(f"{date}: {error}" for date, error in failures[:5])
        raise DataContractError(
            f"cb_daily failed for {len(failures)} trading dates: {sample}"
        )
    if not chunks:
        raise RuntimeError("cb_daily 全部批次返回空数据")

    raw = pd.concat(chunks, ignore_index=True)
    raw['trade_date'] = pd.to_datetime(raw['trade_date'])
    raw['close']  = pd.to_numeric(raw['close'],  errors='coerce')
    raw.loc[raw['close'] <= 0, 'close'] = np.nan
    for col in ('amount', 'cb_value', 'bond_value'):
        raw[col] = pd.to_numeric(raw[col], errors='coerce')

    def _pivot(col):
        w = raw.pivot_table(index='trade_date', columns='ts_code', values=col, aggfunc='last')
        w.index.name = None
        w.columns.name = None
        return w

    price  = _pivot('close')
    amount = _pivot('amount')
    convert_value = _pivot('cb_value')
    provider_bond_value = _pivot('bond_value')
    _remove_cb_daily_checkpoints(checkpoint_dir)
    print(f"   cb_daily: 价格 {price.shape}, 成交额 {amount.shape}")
    return {
        'price': price,
        'amount': amount,
        'convert_value': convert_value,
        'provider_bond_value': provider_bond_value,
    }


# ==========================================
# 6. 历史转股价与转换价值
# ==========================================
def _batches(values: list[str], batch_size: int):
    for offset in range(0, len(values), batch_size):
        yield values[offset:offset + batch_size]


def fetch_conversion_price_events(
    pro,
    bonds: list[str],
    *,
    batch_size: int = 100,
) -> pd.DataFrame:
    """Download every historical conversion-price change from Tushare."""

    print("\n[Step 3] 拉取历史转股价变动 cb_price_chg ...")
    chunks = []
    failures = []
    unique_bonds = sorted(set(bonds))
    for batch in tqdm(list(_batches(unique_bonds, batch_size)), desc='cb_price_chg'):
        codes = ",".join(batch)
        try:
            frame = call_tushare_with_retry(
                pro.cb_price_chg, ts_code=codes
            )
            if frame is not None and not frame.empty:
                chunks.append(frame)
        except Exception as exc:
            failures.append((codes, str(exc)))
        time.sleep(0.1)
    if failures:
        sample = "; ".join(f"{codes}: {error}" for codes, error in failures[:3])
        raise DataContractError(
            f"cb_price_chg failed for {len(failures)} batches: {sample}"
        )
    if not chunks:
        raise DataContractError("cb_price_chg returned no conversion-price history")
    result = pd.concat(chunks, ignore_index=True)
    result = result.drop_duplicates(
        subset=['ts_code', 'change_date', 'convertprice_aft'],
        keep='last',
    )
    return result.sort_values(['ts_code', 'change_date'])


def fetch_clause_terms_akshare(bonds: list[str]) -> pd.DataFrame:
    """Download contractual redemption/put clauses from Eastmoney via AkShare."""

    print("\n[Step 3] 拉取可转债回售/赎回条款 AkShare ...")
    rows = []
    for ts_code in tqdm(sorted(set(bonds)), desc='bond clauses'):
        symbol = str(ts_code).split('.')[0]
        try:
            detail = ak.bond_zh_cov_info(symbol=symbol)
            if detail is None or detail.empty:
                raise DataContractError("empty AkShare bond detail")
            record = detail.iloc[0]
            par_value = pd.to_numeric(record.get('PAR_VALUE'), errors='coerce')
            if pd.isna(par_value) or float(par_value) <= 0:
                raise DataContractError("contractual par value unavailable")
            resale_clause = record.get('RESALE_CLAUSE')
            redeem_clause = record.get('REDEEM_CLAUSE')
            terms = extract_clause_terms(
                resale_clause,
                redeem_clause,
                par_value=float(par_value),
            )
            rows.append(
                {
                    'ts_code': ts_code,
                    'source_ok': True,
                    'source_error': '',
                    'resale_clause': resale_clause,
                    'redeem_clause': redeem_clause,
                    **terms.__dict__,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    'ts_code': ts_code,
                    'source_ok': False,
                    'source_error': str(exc),
                }
            )
        time.sleep(0.05)
    return pd.DataFrame(rows)


def calc_convert_val(
    pro,
    df_price: pd.DataFrame,
    cb_basic: pd.DataFrame,
    df_conversion_price: pd.DataFrame,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Derive conversion value only when cb_daily.cb_value is absent.

    Both stock close and conversion price must be observed on the same date.
    No cross-date forward fill is allowed here.
    """
    print(f"\n[Step 4] 补充转换价值（拉取正股日线 {start}→{end}）...")
    bond_to_stock = (
        cb_basic.dropna(subset=['ts_code', 'stk_cd'])
        .set_index('ts_code')['stk_cd'].to_dict()
    )

    stock_codes = sorted(set(bond_to_stock.values()))
    chunks = []
    failures = []
    for stock_code in tqdm(stock_codes, desc='stock daily for CV'):
        try:
            df = call_tushare_with_retry(
                pro.daily,
                ts_code=stock_code,
                start_date=start,
                end_date=end,
                fields='ts_code,trade_date,close'
            )
            if df is not None and not df.empty:
                chunks.append(df)
        except Exception as ex:
            failures.append((stock_code, str(ex)))
        time.sleep(0.1)

    if failures:
        sample = "; ".join(f"{code}: {error}" for code, error in failures[:5])
        raise DataContractError(
            f"stock daily failed for {len(failures)} securities: {sample}"
        )

    if not chunks:
        print("   正股日线无数据，转换价值将全部为 NaN")
        return pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)

    raw = pd.concat(chunks, ignore_index=True)
    raw['trade_date'] = pd.to_datetime(raw['trade_date'])
    raw['close'] = pd.to_numeric(raw['close'], errors='coerce')
    stk_wide = raw.pivot_table(index='trade_date', columns='ts_code', values='close', aggfunc='last')

    result = pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)
    for bond in df_price.columns:
        stk = bond_to_stock.get(bond)
        if stk is None or bond not in df_conversion_price.columns:
            continue
        if stk not in stk_wide.columns:
            continue
        stk_prices = stk_wide[stk].reindex(df_price.index)
        conversion_price = pd.to_numeric(
            df_conversion_price[bond].reindex(df_price.index),
            errors='coerce',
        )
        par_value = pd.to_numeric(
            cb_basic.loc[cb_basic['ts_code'] == bond, 'par_value'].iloc[-1]
            if 'par_value' in cb_basic and (cb_basic['ts_code'] == bond).any()
            else np.nan,
            errors='coerce',
        )
        if pd.isna(par_value) or par_value <= 0:
            continue
        result[bond] = stk_prices * float(par_value) / conversion_price

    result.index.name   = None
    result.columns.name = None
    filled = result.notna().mean().mean()
    print(f"   转换价值矩阵非空率: {filled:.1%}")
    return result


# ==========================================
# 7. 计算剩余期限矩阵（年）
# ==========================================
def build_maturity_matrix(df_price: pd.DataFrame, cb_basic: pd.DataFrame) -> pd.DataFrame:
    print("\n[Step 4] 构建剩余期限矩阵 ...")
    mat_map = cb_basic.dropna(subset=['ts_code', 'maturity_date']).set_index('ts_code')['maturity_date'].to_dict()
    dates   = df_price.index
    data    = {}
    for bond in df_price.columns:
        mat = mat_map.get(bond)
        if mat is None or pd.isna(mat):
            data[bond] = np.nan
            continue
        remaining = (pd.Timestamp(mat) - dates).days.values / 365.0
        data[bond] = np.maximum(remaining, 0.0)
    df_mat = pd.DataFrame(data, index=dates)
    df_mat.index.name   = None
    df_mat.columns.name = None
    print(f"   剩余期限矩阵: {df_mat.shape}")
    return df_mat


# ==========================================
# 8. 无风险利率曲线
# ==========================================
def _normalize_yield_table(df: pd.DataFrame) -> pd.DataFrame:
    """统一利率表列为 float 期限并去重排序。

    CSV 读回的列名是字符串（'1.0'），新拉取的是 float（1.0），若不统一，
    combine_first 会把同一期限拼成两列（历史上曾把缓存写坏成 10 列）。
    """
    df = df.copy()
    # 兼容 pandas 对重复列的改名（'1.0.1' → 1.0）
    df.columns = [float(re.match(r'^(\d+(?:\.\d+)?)', str(c)).group(1)) for c in df.columns]
    # 同期限重复列合并（优先保留靠前列的非空值）
    df = df.T.groupby(level=0).first().T
    return df.sort_index(axis=1)


def fetch_yield_curve(start: str, end: str) -> pd.DataFrame:
    print("\n[Step 5] 拉取国债收益率曲线 ...")
    if os.path.exists(RF_CACHE):
        existing = pd.read_csv(RF_CACHE, index_col=0, parse_dates=True)
        existing = _normalize_yield_table(existing)
        last_date = existing.index.max()
        end_ts    = pd.Timestamp(end)
        if last_date >= end_ts:
            print(f"   利率缓存已到 {last_date.date()}，跳过拉取")
            return existing
        fetch_start = (last_date + pd.Timedelta(days=1)).strftime('%Y%m%d')
        print(f"   增量拉取利率: {fetch_start} → {end}")
    else:
        existing    = None
        fetch_start = start

    try:
        yield_chunks = []
        cursor = pd.Timestamp(fetch_start)
        end_ts = pd.Timestamp(end)
        while cursor <= end_ts:
            segment_end = min(
                pd.Timestamp(year=cursor.year, month=12, day=31),
                end_ts,
            )
            frame = ak.bond_china_yield(
                start_date=cursor.strftime('%Y%m%d'),
                end_date=segment_end.strftime('%Y%m%d'),
            )
            if frame is not None and not frame.empty:
                yield_chunks.append(frame)
            cursor = segment_end + pd.Timedelta(days=1)
        if not yield_chunks:
            raise DataContractError("AkShare returned no government yield rows")
        df_yield = pd.concat(yield_chunks, ignore_index=True)
        target = df_yield[df_yield['曲线名称'] == '中债国债收益率曲线'].copy()
        target['日期'] = pd.to_datetime(target['日期'])
        target = target.set_index('日期').sort_index()
        tenor_cols = ['1年', '2年', '3年', '5年', '7年', '10年']
        available  = [c for c in tenor_cols if c in target.columns]
        yield_tbl  = target[available] / 100.0
        yield_tbl.columns = [float(c.replace('年', '')) for c in available]
        yield_tbl.index.name = None

        if existing is not None:
            yield_tbl = _merge_wide(existing, yield_tbl)
        yield_tbl = _normalize_yield_table(yield_tbl)

        yield_tbl.to_csv(RF_CACHE)
        print(f"   利率曲线已更新: {yield_tbl.shape}")
        return yield_tbl
    except Exception as e:
        print(f"   利率拉取失败: {e}")
        if existing is not None:
            return existing
        raise DataContractError(
            "observed government yield curve is unavailable and no cache exists"
        ) from e


# ==========================================
# 9. 纯债价值 DCF
# ==========================================
def calc_bond_floor_dcf(
    cb_basic: pd.DataFrame,
    df_maturity: pd.DataFrame,
    yield_tbl: pd.DataFrame,
) -> pd.DataFrame:
    print("\n[Step 6] DCF 计算纯债价值 ...")
    info   = cb_basic.dropna(subset=['ts_code']).set_index('ts_code')
    dates  = df_maturity.index
    bonds  = df_maturity.columns

    if yield_tbl.empty:
        raise DataContractError("observed government yield curve is unavailable")
    yield_tbl = _normalize_yield_table(yield_tbl)

    floor_matrix = np.full((len(dates), len(bonds)), np.nan)

    for j, bond in enumerate(tqdm(bonds, desc='DCF')):
        if bond not in info.index:
            continue
        row         = info.loc[bond]
        try:
            coupon_schedule = parse_coupon_schedule(row.get('rate_clause'))
        except DataContractError as exc:
            raise DataContractError(f"{bond}: {exc}") from exc
        freq = pd.to_numeric(row.get('interest_freq'), errors='coerce')
        par = pd.to_numeric(row.get('par_value'), errors='coerce')
        value_date = pd.to_datetime(row.get('value_date'), errors='coerce')
        maturity_date = pd.to_datetime(row.get('maturity_date'), errors='coerce')
        if pd.isna(freq) or int(freq) <= 0:
            raise DataContractError(f"{bond}: contractual interest frequency unavailable")
        if pd.isna(par) or float(par) <= 0:
            raise DataContractError(f"{bond}: contractual par value unavailable")
        if pd.isna(value_date) or pd.isna(maturity_date):
            raise DataContractError(f"{bond}: contractual cash-flow dates unavailable")
        freq = int(freq)
        par = float(par)
        months = 12 // freq
        if months <= 0 or 12 % freq:
            raise DataContractError(f"{bond}: unsupported interest frequency {freq}")

        payment_dates = []
        payment = pd.Timestamp(value_date) + pd.DateOffset(months=months)
        while payment < pd.Timestamp(maturity_date):
            payment_dates.append(payment)
            payment += pd.DateOffset(months=months)
        payment_dates.append(pd.Timestamp(maturity_date))

        for i in range(len(dates)):
            T = df_maturity.iloc[i, j]
            if pd.isna(T) or T <= 0:
                continue
            valuation_date = pd.Timestamp(dates[i])
            pv = 0.0
            for cash_date in payment_dates:
                if cash_date <= valuation_date:
                    continue
                rate_date = min(cash_date - pd.Timedelta(days=1), coupon_schedule.index.max())
                eligible_rate = coupon_schedule.loc[
                    coupon_schedule.index <= rate_date
                ]
                if eligible_rate.empty:
                    raise DataContractError(
                        f"{bond}: coupon unavailable for payment {cash_date.date()}"
                    )
                coupon = par * float(eligible_rate.iloc[-1]) / freq
                cash_t = (cash_date - valuation_date).days / 365.0
                discount_rate = interpolate_observed_yield_curve(
                    yield_tbl, valuation_date, cash_t
                )
                pv += coupon * np.exp(-discount_rate * cash_t)
            r_T = interpolate_observed_yield_curve(
                yield_tbl, valuation_date, float(T)
            )
            pv += par * np.exp(-r_T * float(T))
            floor_matrix[i, j] = pv

    df_floor = pd.DataFrame(floor_matrix, index=dates, columns=bonds)
    df_floor.index.name   = None
    df_floor.columns.name = None
    print(f"   DCF 完成: {df_floor.shape}")
    return df_floor


def build_observed_bond_floor(
    *,
    provider_bond_value: pd.DataFrame,
    market_price: pd.DataFrame,
) -> pd.DataFrame:
    """Use Tushare's daily pure-bond value only on observed market cells."""

    aligned = provider_bond_value.reindex(
        index=market_price.index,
        columns=market_price.columns,
    ).apply(pd.to_numeric, errors='coerce')
    observed_price = market_price.apply(pd.to_numeric, errors='coerce')
    valid = (
        observed_price.notna()
        & (observed_price > 0)
        & aligned.notna()
        & (aligned > 0)
    )
    return aligned.where(valid)


# ==========================================
# 10. 正股市值
# ==========================================
def fetch_stock_mv(
    pro,
    cb_basic: pd.DataFrame,
    df_price: pd.DataFrame,
    start: str,
    end: str,
) -> pd.DataFrame:
    print(f"\n[Step 7] 拉取正股市值 daily_basic ({start}→{end}) ...")
    bond_to_stock = (
        cb_basic.dropna(subset=['ts_code', 'stk_cd'])
        .set_index('ts_code')['stk_cd'].to_dict()
    )
    stock_codes = sorted(set(bond_to_stock.values()))
    chunks = []
    failures = []
    for stock_code in tqdm(stock_codes, desc='daily_basic'):
        try:
            df = call_tushare_with_retry(
                pro.daily_basic,
                ts_code=stock_code,
                start_date=start,
                end_date=end,
                fields='ts_code,trade_date,total_mv'
            )
            if df is not None and not df.empty:
                chunks.append(df)
        except Exception as ex:
            failures.append((stock_code, str(ex)))
        time.sleep(0.1)

    if failures:
        sample = "; ".join(f"{code}: {error}" for code, error in failures[:5])
        raise DataContractError(
            f"daily_basic failed for {len(failures)} securities: {sample}"
        )

    if not chunks:
        print("   daily_basic 无数据")
        return pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)

    raw = pd.concat(chunks, ignore_index=True)
    raw['trade_date'] = pd.to_datetime(raw['trade_date'])
    raw['total_mv']   = pd.to_numeric(raw['total_mv'], errors='coerce')
    stk_wide = raw.pivot_table(index='trade_date', columns='ts_code', values='total_mv', aggfunc='last')

    stock_to_bonds: dict = {}
    for bond, stk in bond_to_stock.items():
        stock_to_bonds.setdefault(stk, []).append(bond)

    result = pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)
    for stk, bonds in stock_to_bonds.items():
        if stk not in stk_wide.columns:
            continue
        series = stk_wide[stk].reindex(df_price.index, method='ffill')
        for bond in bonds:
            if bond in result.columns:
                result[bond] = series.values

    result.index.name   = None
    result.columns.name = None
    print(f"   正股市值矩阵非空率: {result.notna().mean().mean():.1%}")
    return result


# ==========================================
# 11. 信用评级
# ==========================================
def fetch_rating_events(
    pro,
    bonds: list[str],
    *,
    batch_size: int = 100,
) -> pd.DataFrame:
    chunks = []
    failures = []
    for batch in _batches(sorted(set(bonds)), batch_size):
        codes = ",".join(batch)
        try:
            frame = call_tushare_with_retry(
                pro.cb_rating, ts_code=codes
            )
            if frame is not None and not frame.empty:
                chunks.append(frame)
        except Exception as exc:
            failures.append((codes, str(exc)))
        time.sleep(0.1)
    if failures:
        sample = "; ".join(f"{codes}: {error}" for codes, error in failures[:3])
        raise DataContractError(
            f"cb_rating failed for {len(failures)} batches: {sample}"
        )
    if not chunks:
        raise DataContractError("cb_rating returned no rating history")
    return pd.concat(chunks, ignore_index=True)


def fetch_ratings(pro, df_price: pd.DataFrame) -> pd.DataFrame:
    print("\n[Step 8] 拉取信用评级 ...")
    events = fetch_rating_events(pro, list(df_price.columns))
    result = build_point_in_time_rating_matrix(
        dates=df_price.index,
        bonds=list(df_price.columns),
        rating_events=events,
    )

    result.index.name   = None
    result.columns.name = None
    print(f"   评级非空率: {result.notna().mean().mean():.1%}")
    return result


def fetch_share_events(
    pro,
    bonds: list[str],
    *,
    batch_size: int = 100,
) -> pd.DataFrame:
    """Download published outstanding-balance observations from cb_share."""

    print("\n[Step 8] 拉取历史转股结果 cb_share ...")
    chunks = []
    failures = []
    for batch in tqdm(list(_batches(sorted(set(bonds)), batch_size)), desc='cb_share'):
        codes = ",".join(batch)
        try:
            frame = call_tushare_with_retry(
                pro.cb_share, ts_code=codes
            )
            if frame is not None and not frame.empty:
                chunks.append(frame)
        except Exception as exc:
            failures.append((codes, str(exc)))
        time.sleep(0.1)
    if failures:
        sample = "; ".join(f"{codes}: {error}" for codes, error in failures[:3])
        raise DataContractError(
            f"cb_share failed for {len(failures)} batches: {sample}"
        )
    if not chunks:
        raise DataContractError("cb_share returned no outstanding-balance history")
    result = pd.concat(chunks, ignore_index=True)
    return result.drop_duplicates(
        subset=['ts_code', 'publish_date', 'end_date'],
        keep='last',
    )


# ==========================================
# 12. 每股净资产 BPS
# ==========================================
def fetch_bps(
    pro,
    cb_basic: pd.DataFrame,
    df_price: pd.DataFrame,
    start: str,
    end: str,
) -> pd.DataFrame:
    print(f"\n[Step 9] 拉取 BPS fina_indicator ({start}→{end}) ...")
    bond_to_stock = (
        cb_basic.dropna(subset=['ts_code', 'stk_cd'])
        .set_index('ts_code')['stk_cd'].to_dict()
    )
    stock_codes = list(set(bond_to_stock.values()))

    # 增量更新加速：通过 disclosure_date 预筛期间有财报披露的正股
    if start >= "20200101" and len(stock_codes) > 50:
        try:
            print("   正在通过财报披露日 (disclosure_date) 预筛当期有更新的个股...")
            start_yr = int(start[:4])
            end_yr = int(end[:4])
            q_ends = [f"{y}{q}" for y in range(start_yr - 1, end_yr + 1) for q in ["0331", "0630", "0930", "1231"]]
            q_ends = [ed for ed in q_ends if int(ed) >= (start_yr - 1) * 10000]
            
            disclosed_stocks = set()
            for ed in q_ends:
                df_disc = call_tushare_with_retry(
                    pro.disclosure_date, end_date=ed
                )
                if df_disc is not None and not df_disc.empty and "actual_date" in df_disc.columns:
                    match_df = df_disc[(df_disc["actual_date"] >= start) & (df_disc["actual_date"] <= end)]
                    disclosed_stocks.update(match_df["ts_code"].tolist())
                time.sleep(0.1)
            
            orig_len = len(stock_codes)
            stock_codes = [s for s in stock_codes if s in disclosed_stocks]
            print(f"   预筛完成：仅 {len(stock_codes)} / {orig_len} 只正股在 [{start}, {end}] 区间内披露了财报。")
            if not stock_codes:
                print("   当期无正股财报披露，直接跳过 fina_indicator 耗时网络查询。")
                return pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)
        except Exception as e:
            print(f"   财报预筛失败 ({e})，将回退至全量正股查询。")

    bps_series: dict = {}
    failures = []
    first_report_year = max(1990, int(start[:4]) - 2)
    last_report_year = int(end[:4])
    for stk in tqdm(stock_codes, desc='fina_indicator'):
        try:
            frames = []
            for chunk_start_year in range(
                first_report_year,
                last_report_year + 1,
                2,
            ):
                chunk_end_year = min(
                    chunk_start_year + 1,
                    last_report_year,
                )
                frame = call_tushare_with_retry(
                    pro.fina_indicator,
                    ts_code=stk,
                    start_date=f"{chunk_start_year}0101",
                    end_date=f"{chunk_end_year}1231",
                    fields='ts_code,ann_date,end_date,bps,update_flag',
                )
                if frame is not None and not frame.empty:
                    if len(frame) >= 100:
                        raise DataContractError(
                            "fina_indicator report-period chunk reached "
                            "the 100-row limit"
                        )
                    frames.append(frame)
                time.sleep(0.05)
            if not frames:
                continue
            df = pd.concat(frames, ignore_index=True)
            df['ann_date'] = pd.to_datetime(df['ann_date'], errors='coerce')
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
            df['_update_priority'] = (
                pd.to_numeric(
                    df.get('update_flag', pd.Series(index=df.index, dtype=float)),
                    errors='coerce',
                )
                .fillna(0)
                .astype(int)
            )
            announcement_cutoff = pd.Timestamp(end)
            df = df.loc[df['ann_date'] <= announcement_cutoff]
            df = df.dropna(subset=['ann_date', 'end_date']).sort_values(
                ['ann_date', 'end_date', '_update_priority']
            )
            df = df.drop_duplicates(
                subset=['ann_date', 'end_date'],
                keep='last',
            )
            df['bps'] = pd.to_numeric(df['bps'], errors='coerce')
            series = df.set_index('ann_date')['bps']
            series = series[~series.index.duplicated(keep='last')]
            bps_series[stk] = series
        except Exception as exc:
            failures.append((stk, str(exc)))
    if failures:
        sample = "; ".join(
            f"{stock}: {error}" for stock, error in failures[:5]
        )
        raise DataContractError(
            f"fina_indicator failed for {len(failures)} stocks: {sample}"
        )

    stock_to_bonds: dict = {}
    for bond, stk in bond_to_stock.items():
        stock_to_bonds.setdefault(stk, []).append(bond)

    result = pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)
    for stk, bonds in stock_to_bonds.items():
        if stk not in bps_series:
            continue
        series = bps_series[stk].reindex(df_price.index, method='ffill')
        for bond in bonds:
            if bond in result.columns:
                result[bond] = series.values

    result.index.name   = None
    result.columns.name = None
    print(f"   BPS 非空率: {result.notna().mean().mean():.1%}")
    return result


# ==========================================
# 13. 静态余额（来自 cb_basic.remain_size）
# ==========================================
def build_balance_from_basic(df_price: pd.DataFrame, cb_basic: pd.DataFrame) -> pd.DataFrame:
    """remain_size 是当前静态余额，广播到所有日期行。"""
    balance_map = (
        cb_basic.dropna(subset=['ts_code', 'remain_size'])
        .set_index('ts_code')['remain_size'].to_dict()
    )
    result = pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=float)
    for bond in df_price.columns:
        val = balance_map.get(bond)
        if val is not None:
            result[bond] = float(val)
    result.index.name   = None
    result.columns.name = None
    return result


# ==========================================
# 14. 主流程
# ==========================================
def run_pipeline(
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
    *,
    rebuild_all: bool = False,
    weekly_validation: bool = False,
) -> None:
    pro = init_tushare()
    requested_end = end
    if weekly_validation:
        end = resolve_completed_weekly_end(
            pro,
            start=start,
            requested_end=requested_end,
        )
        if end != requested_end:
            print(
                "Weekly rebuild excludes the incomplete current week: "
                f"{requested_end} -> {end}"
            )

    print(f"\n{'='*55}")
    print(f"Convertible Bond Data Pipeline  {start} → {end}")
    print(f"{'='*55}")

    # --- 基础信息 ---
    cb_basic = fetch_cb_basic(pro)
    # 合并已有 cb_basic_info（保留 maturity_price 等引导数据）
    if os.path.exists(OUT_BASIC) and not rebuild_all:
        existing_basic = pd.read_csv(OUT_BASIC)   # 不用 index_col，ts_code 是普通列
        # 新数据字段优先，旧数据补充缺失列（如 maturity_price 来自 Excel 引导）
        merged_basic = (
            cb_basic.set_index('ts_code')
            .combine_first(existing_basic.set_index('ts_code'))
            .reset_index()
        )
        cb_basic = merged_basic
    cb_basic.to_csv(OUT_BASIC, index=False)
    print(f"   已保存: {OUT_BASIC}  ({len(cb_basic)} 条)")

    # --- 转债日线 ---
    daily = fetch_cb_daily(pro, start, end)
    df_price_new = daily['price']
    df_price = (
        df_price_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_PRICE), df_price_new)
    )
    df_price = df_price.where(df_price > 0)
    df_price.to_csv(OUT_PRICE)

    amount_new = daily['amount']
    amount = (
        amount_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_AMOUNT), amount_new)
    )
    amount.to_csv(OUT_AMOUNT)

    # --- 历史转股价与转换价值 ---
    conversion_events = fetch_conversion_price_events(
        pro, list(df_price.columns)
    )
    conversion_events.to_csv(OUT_CONV_EVENTS, index=False)
    conversion_price = build_conversion_price_matrix(
        dates=df_price.index,
        bonds=list(df_price.columns),
        cb_basic=cb_basic,
        change_events=conversion_events,
    )
    conversion_price.to_csv(OUT_CONV_PRICE)
    clause_terms = fetch_clause_terms_akshare(list(df_price.columns))
    clause_terms.to_csv(OUT_CLAUSES, index=False)
    clause_coverage = clause_terms['source_ok'].fillna(False).mean()
    print(f"   条款数据覆盖率: {clause_coverage:.1%}")
    completed_weekly_dates = select_completed_weekly_dates(
        df_price_new.index,
        as_of=pd.Timestamp(end),
    )
    if len(completed_weekly_dates) == 0:
        raise DataContractError("no completed weekly source date")
    latest_weekly_date = completed_weekly_dates[-1]
    source_validation_dates = (
        completed_weekly_dates
        if rebuild_all and weekly_validation
        else pd.DatetimeIndex([latest_weekly_date])
    )
    clause_ok = (
        clause_terms.drop_duplicates("ts_code", keep="last")
        .set_index("ts_code")["source_ok"]
        .astype(str)
        .str.lower()
        .eq("true")
        .reindex(df_price_new.columns, fill_value=False)
    )
    clause_matrix = pd.DataFrame(
        np.tile(
            clause_ok.where(clause_ok, np.nan).astype(float).to_numpy(),
            (len(source_validation_dates), 1),
        ),
        index=source_validation_dates,
        columns=df_price_new.columns,
    )
    validate_pricing_coverage(
        market_price=df_price_new,
        model_price=clause_matrix,
        dates=source_validation_dates,
        min_coverage=float(
            os.environ.get("MIN_WEEKLY_SOURCE_COVERAGE", "0.98")
        ),
        min_count=int(os.environ.get("MIN_WEEKLY_SOURCE_COUNT", "20")),
        label="clause source",
    )

    observed_cv = daily['convert_value'].reindex(
        index=df_price_new.index,
        columns=df_price_new.columns,
    )
    derived_cv = calc_convert_val(
        pro,
        df_price_new,
        cb_basic,
        conversion_price.reindex(
            index=df_price_new.index,
            columns=df_price_new.columns,
        ),
        start,
        end,
    )
    df_cv_new = observed_cv.combine_first(derived_cv)
    df_cv = (
        df_cv_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_CV), df_cv_new)
    )
    df_cv.to_csv(OUT_CV)

    # --- 剩余期限 ---
    df_mat_new  = build_maturity_matrix(df_price_new, cb_basic)
    df_maturity = (
        df_mat_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_MATURITY), df_mat_new)
    )
    df_maturity.to_csv(OUT_MATURITY)

    # --- 无风险利率 ---
    yield_tbl = fetch_yield_curve(start, end)

    # --- Tushare 每日纯债价值；不再以固定票息/利差自建 DCF 代替 ---
    df_floor_new = build_observed_bond_floor(
        provider_bond_value=daily['provider_bond_value'],
        market_price=df_price_new,
    )
    df_floor = (
        df_floor_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_FLOOR), df_floor_new)
    )
    df_floor.to_csv(OUT_FLOOR)

    # --- 由 Tushare 纯债价值 + 契约现金流 + AkShare 国债曲线反解信用利差 ---
    clause_maturity = (
        clause_terms.drop_duplicates("ts_code", keep="last")
        .set_index("ts_code")["maturity_redemption_price"]
    )
    spread_basic = cb_basic.copy()
    spread_basic["maturity_redemption_price"] = (
        spread_basic["ts_code"].map(clause_maturity)
    )
    credit_spread_new = build_implied_credit_spread_matrix(
        observed_bond_value=df_floor_new,
        maturity=df_mat_new,
        cb_basic=spread_basic,
        government_curve=yield_tbl,
    )
    credit_spread = (
        credit_spread_new
        if rebuild_all
        else _merge_wide(
            _load_existing(OUT_CREDIT_SPREAD),
            credit_spread_new,
        )
    )
    credit_spread.to_csv(OUT_CREDIT_SPREAD)
    print(
        f"   隐含信用利差非空率: "
        f"{credit_spread_new.notna().mean().mean():.1%}"
    )
    valid_curve = yield_tbl.dropna(how="all")
    if valid_curve.empty:
        raise DataContractError("risk-free yield curve has no valid observations")
    credit_validation_dates = source_validation_dates[
        source_validation_dates >= valid_curve.index.min()
    ]
    if len(credit_validation_dates) == 0:
        raise DataContractError(
            "no completed weekly source date overlaps the risk-free curve"
        )
    validate_pricing_coverage(
        market_price=df_price_new,
        model_price=credit_spread_new,
        dates=credit_validation_dates,
        min_coverage=float(
            os.environ.get("MIN_WEEKLY_SOURCE_COVERAGE", "0.98")
        ),
        min_count=int(os.environ.get("MIN_WEEKLY_SOURCE_COUNT", "20")),
        label="implied credit spread source",
    )

    # --- 正股市值 ---
    df_mv_new  = fetch_stock_mv(pro, cb_basic, df_price_new, start, end)
    df_stk_mv = (
        df_mv_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_STOCK_MV), df_mv_new)
    )
    validate_stock_market_value_wan_units(df_stk_mv)
    df_stk_mv.to_csv(OUT_STOCK_MV)

    # --- 公告时点可得的历史余额 ---
    share_events = fetch_share_events(pro, list(df_price.columns))
    share_events.to_csv(OUT_SHARE_EVENTS, index=False)
    bal_new = build_point_in_time_balance_matrix(
        dates=df_price_new.index,
        bonds=list(df_price_new.columns),
        cb_basic=cb_basic,
        share_events=share_events,
    )
    balance = (
        bal_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_BALANCE), bal_new)
    )
    validate_balance_wan_units(balance=balance, cb_basic=cb_basic)
    balance.to_csv(OUT_BALANCE)

    # --- 信用评级 ---
    df_rating_new = fetch_ratings(pro, df_price_new)
    df_rating = (
        df_rating_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_RATING), df_rating_new)
    )
    df_rating.to_csv(OUT_RATING)

    # --- BPS ---
    df_bps_new = fetch_bps(pro, cb_basic, df_price_new, start, end)
    df_bps = (
        df_bps_new
        if rebuild_all
        else _merge_wide(_load_existing(OUT_BPS), df_bps_new)
    )
    # Point-in-time carry-forward across incremental weekly boundaries.
    # Values remain unavailable before their announcement dates.
    df_bps = df_bps.sort_index().ffill()
    df_bps.to_csv(OUT_BPS)

    weekly_sources = {
        "conversion value source": (df_cv, True),
        "bond floor source": (df_floor, True),
        "maturity source": (df_maturity, True),
        "stock market value source": (df_stk_mv, True),
        "outstanding balance source": (balance, True),
        "turnover amount source": (amount, True),
        "rating source": (df_rating, False),
        "BPS source": (df_bps, True),
    }
    for source_label, (source_frame, numeric) in weekly_sources.items():
        validate_observed_source_coverage(
            market_price=df_price,
            source=source_frame,
            dates=source_validation_dates,
            min_coverage=float(
                os.environ.get("MIN_WEEKLY_SOURCE_COVERAGE", "0.98")
            ),
            min_count=int(
                os.environ.get("MIN_WEEKLY_SOURCE_COUNT", "20")
            ),
            label=source_label,
            require_finite_numeric=numeric,
        )

    # --- 汇总 ---
    print(f"\n{'='*55}")
    print("数据管道完成！")
    for label, df in [
        ('可转债价格',   df_price),
        ('转换价值',     df_cv),
        ('纯债价值',     df_floor),
        ('隐含信用利差', credit_spread),
        ('剩余期限',     df_maturity),
        ('正股市值',     df_stk_mv),
        ('转债余额',     balance),
        ('成交额',       amount),
        ('信用评级',     df_rating),
        ('每股净资产',   df_bps),
    ]:
        if hasattr(df, 'shape'):
            idx = df.index
            date_range = f"{idx.min().date()} → {idx.max().date()}" if len(idx) > 0 else 'empty'
            print(f"  {label:8s}: {df.shape}  [{date_range}]")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CB Data Pipeline (Tushare)')
    parser.add_argument('--start', default=DEFAULT_START, help='起始日期 YYYYMMDD')
    parser.add_argument('--end',   default=DEFAULT_END,   help='结束日期 YYYYMMDD')
    parser.add_argument(
        '--rebuild-all',
        action='store_true',
        help='忽略旧宽表并用真实历史源完整重建指定区间',
    )
    parser.add_argument(
        '--weekly',
        action='store_true',
        help='全量重建时逐一验证所有已完成周度截面的数据覆盖率',
    )
    args = parser.parse_args()
    run_pipeline(
        args.start,
        args.end,
        rebuild_all=args.rebuild_all,
        weekly_validation=args.weekly,
    )
