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
               maturity_date, conv_price, remain_size
    cb_daily:  ts_code, trade_date, close, vol, amount
    daily:     ts_code, trade_date, close
    daily_basic: ts_code, trade_date, total_mv
    fina_indicator: ts_code, end_date, bps
    rating:    ts_code, rating_date, rating
"""

import argparse
import os
import time
import warnings
from datetime import datetime

import akshare as ak
import numpy as np
import pandas as pd
import tushare as ts
from tqdm import tqdm

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置
# ==========================================
TUSHARE_TOKEN = 'ddd1b26b20ff085ac9b60c9bd902ae76bbff60910863e8cc0168da53'
DEFAULT_START = '20190101'
DEFAULT_END   = datetime.today().strftime('%Y%m%d')
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


# ==========================================
# 2. 初始化 Tushare
# ==========================================
def init_tushare():
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
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


# ==========================================
# 4. 获取转债基础信息（修正字段名）
# ==========================================
def fetch_cb_basic(pro) -> pd.DataFrame:
    """
    cb_basic 实际字段（经验证）：
      ts_code, stk_code, bond_short_name, maturity_date,
      coupon_rate, pay_per_year, par, conv_price, remain_size,
      list_date, delist_date
    """
    print("\n[Step 1] 拉取转债基础信息 cb_basic ...")
    df = pro.cb_basic()
    if df is None or df.empty:
        raise RuntimeError("cb_basic 返回空数据，请检查 Token 权限")

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

    # interest_freq 默认年付 1 次
    if 'interest_freq' in df.columns:
        df['interest_freq'] = pd.to_numeric(df['interest_freq'], errors='coerce').fillna(1).astype(int)
    else:
        df['interest_freq'] = 1

    # par_value 默认 100
    if 'par_value' in df.columns:
        df['par_value'] = pd.to_numeric(df['par_value'], errors='coerce').fillna(100.0)
    else:
        df['par_value'] = 100.0

    # conv_price（当前转股价）
    if 'conv_price' in df.columns:
        df['conv_price'] = pd.to_numeric(df['conv_price'], errors='coerce')

    # remain_size 为静态余额（万元）
    if 'remain_size' in df.columns:
        df['remain_size'] = pd.to_numeric(df['remain_size'], errors='coerce')

    df = df.drop_duplicates(subset='ts_code', keep='last').reset_index(drop=True)
    print(f"   获取 {len(df)} 只转债基础信息，列: {list(df.columns)}")
    return df


# ==========================================
# 5. 获取转债日线（价格 + 成交额）
# ==========================================
def fetch_cb_daily(pro, start: str, end: str) -> dict:
    """
    cb_daily 实际字段（经验证）：
      ts_code, trade_date, pre_close, open, high, low, close,
      change, pct_chg, vol, amount
    """
    print(f"\n[Step 2] 拉取转债日线 cb_daily ({start} → {end}) ...")
    quarters = _quarter_ranges(start, end)
    chunks = []
    for s, e in tqdm(quarters, desc='cb_daily'):
        try:
            df = pro.cb_daily(start_date=s, end_date=e,
                              fields='ts_code,trade_date,close,amount')
            if df is not None and not df.empty:
                chunks.append(df)
        except Exception as ex:
            print(f"   警告: {s}–{e} 失败: {ex}")
        time.sleep(0.3)

    if not chunks:
        raise RuntimeError("cb_daily 全部批次返回空数据")

    raw = pd.concat(chunks, ignore_index=True)
    raw['trade_date'] = pd.to_datetime(raw['trade_date'])
    raw['close']  = pd.to_numeric(raw['close'],  errors='coerce')
    raw['amount'] = pd.to_numeric(raw['amount'], errors='coerce')

    def _pivot(col):
        w = raw.pivot_table(index='trade_date', columns='ts_code', values=col, aggfunc='last')
        w.index.name = None
        w.columns.name = None
        return w

    price  = _pivot('close')
    amount = _pivot('amount')
    print(f"   cb_daily: 价格 {price.shape}, 成交额 {amount.shape}")
    return {'price': price, 'amount': amount}


# ==========================================
# 6. 计算转换价值（正股价 × 100 / 转股价）
# ==========================================
def calc_convert_val(
    pro,
    df_price: pd.DataFrame,
    cb_basic: pd.DataFrame,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    convert_val = stock_close × 100 / conv_price
    需要拉取正股日线数据。
    """
    print(f"\n[Step 3] 计算转换价值（拉取正股日线 {start}→{end}）...")
    bond_to_stock = (
        cb_basic.dropna(subset=['ts_code', 'stk_cd'])
        .set_index('ts_code')['stk_cd'].to_dict()
    )
    conv_price_map = (
        cb_basic.dropna(subset=['ts_code', 'conv_price'])
        .set_index('ts_code')['conv_price'].to_dict()
    )

    stock_codes = list(set(bond_to_stock.values()))
    quarters    = _quarter_ranges(start, end)

    chunks = []
    for s, e in tqdm(quarters, desc='stock daily for CV'):
        try:
            df = pro.daily(
                start_date=s, end_date=e,
                fields='ts_code,trade_date,close'
            )
            if df is not None and not df.empty:
                chunks.append(df[df['ts_code'].isin(stock_codes)])
        except Exception as ex:
            print(f"   警告: {s}–{e} 失败: {ex}")
        time.sleep(0.3)

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
        cp  = conv_price_map.get(bond)
        if stk is None or cp is None or cp <= 0:
            continue
        if stk not in stk_wide.columns:
            continue
        stk_prices = stk_wide[stk].reindex(df_price.index, method='ffill')
        result[bond] = stk_prices.values * 100.0 / cp

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
def fetch_yield_curve(start: str, end: str) -> pd.DataFrame:
    print("\n[Step 5] 拉取国债收益率曲线 ...")
    if os.path.exists(RF_CACHE):
        existing = pd.read_csv(RF_CACHE, index_col=0, parse_dates=True)
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
        df_yield = ak.bond_china_yield(
            start_date=fetch_start[:4] + '0101',
            end_date=end
        )
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

        yield_tbl.to_csv(RF_CACHE)
        print(f"   利率曲线已更新: {yield_tbl.shape}")
        return yield_tbl
    except Exception as e:
        print(f"   利率拉取失败: {e}，使用已有缓存或默认值 2%")
        if existing is not None:
            return existing
        return pd.DataFrame()


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
        tenors    = np.array([1.0, 3.0, 5.0, 7.0, 10.0])
        yield_arr = np.full((len(dates), len(tenors)), 0.02)
    else:
        tenors    = yield_tbl.columns.astype(float).values
        yield_arr = yield_tbl.reindex(dates, method='ffill').fillna(0.02).values

    floor_matrix = np.full((len(dates), len(bonds)), np.nan)

    for j, bond in enumerate(tqdm(bonds, desc='DCF')):
        if bond not in info.index:
            continue
        row         = info.loc[bond]
        coupon_rate = float(row.get('coupon_rate', 0.005)) if pd.notna(row.get('coupon_rate')) else 0.005
        freq        = int(row.get('interest_freq', 1))     if pd.notna(row.get('interest_freq')) else 1
        par         = float(row.get('par_value', 100))     if pd.notna(row.get('par_value'))     else 100.0
        coupon      = par * coupon_rate / max(freq, 1)

        for i in range(len(dates)):
            T = df_maturity.iloc[i, j]
            if pd.isna(T) or T <= 0:
                continue
            fp    = yield_arr[i]
            steps = max(1, int(round(T * freq)))
            pv    = 0.0
            for k in range(1, steps + 1):
                t   = k / freq
                r_t = float(np.interp(t, tenors, fp))
                pv += coupon * np.exp(-r_t * t)
            r_T = float(np.interp(T, tenors, fp))
            pv += par * np.exp(-r_T * T)
            floor_matrix[i, j] = pv

    df_floor = pd.DataFrame(floor_matrix, index=dates, columns=bonds)
    df_floor.index.name   = None
    df_floor.columns.name = None
    print(f"   DCF 完成: {df_floor.shape}")
    return df_floor


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
    stock_codes = list(set(bond_to_stock.values()))
    quarters    = _quarter_ranges(start, end)

    chunks = []
    for s, e in tqdm(quarters, desc='daily_basic'):
        try:
            df = pro.daily_basic(
                start_date=s, end_date=e,
                fields='ts_code,trade_date,total_mv'
            )
            if df is not None and not df.empty:
                chunks.append(df[df['ts_code'].isin(stock_codes)])
        except Exception as ex:
            print(f"   警告: {s}–{e} 失败: {ex}")
        time.sleep(0.3)

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
def fetch_ratings(pro, df_price: pd.DataFrame) -> pd.DataFrame:
    print("\n[Step 8] 拉取信用评级 ...")
    try:
        df_rating = pro.rating(bond_type='CB', fields='ts_code,rating_date,rating')
    except Exception as ex:
        print(f"   rating 接口失败: {ex}")
        return pd.DataFrame(index=df_price.index, columns=df_price.columns)

    if df_rating is None or df_rating.empty:
        print("   rating 返回空数据")
        return pd.DataFrame(index=df_price.index, columns=df_price.columns)

    df_rating['rating_date'] = pd.to_datetime(df_rating['rating_date'], errors='coerce')
    df_rating = df_rating.dropna(subset=['rating_date']).sort_values('rating_date')

    result = pd.DataFrame(index=df_price.index, columns=df_price.columns, dtype=object)
    for bond in tqdm(df_price.columns, desc='ratings ffill'):
        sub = df_rating[df_rating['ts_code'] == bond].set_index('rating_date')['rating']
        if sub.empty:
            continue
        sub     = sub[~sub.index.duplicated(keep='last')].sort_index()
        aligned = sub.reindex(df_price.index, method='ffill')
        result[bond] = aligned.values

    result.index.name   = None
    result.columns.name = None
    print(f"   评级非空率: {result.notna().mean().mean():.1%}")
    return result


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

    bps_series: dict = {}
    for stk in tqdm(stock_codes, desc='fina_indicator'):
        try:
            df = pro.fina_indicator(
                ts_code=stk,
                start_date=start,
                end_date=end,
                fields='ts_code,end_date,bps'
            )
            if df is None or df.empty:
                continue
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
            df = df.dropna(subset=['end_date']).sort_values('end_date')
            df['bps'] = pd.to_numeric(df['bps'], errors='coerce')
            series = df.set_index('end_date')['bps']
            series = series[~series.index.duplicated(keep='last')]
            bps_series[stk] = series
        except Exception:
            pass
        time.sleep(0.05)

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
def run_pipeline(start: str = DEFAULT_START, end: str = DEFAULT_END) -> None:
    print(f"\n{'='*55}")
    print(f"Convertible Bond Data Pipeline  {start} → {end}")
    print(f"{'='*55}")

    pro = init_tushare()

    # --- 基础信息 ---
    cb_basic = fetch_cb_basic(pro)
    # 合并已有 cb_basic_info（保留 maturity_price 等引导数据）
    if os.path.exists(OUT_BASIC):
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
    df_price = _merge_wide(_load_existing(OUT_PRICE), df_price_new)
    df_price.to_csv(OUT_PRICE)

    amount_new = daily['amount']
    amount = _merge_wide(_load_existing(OUT_AMOUNT), amount_new)
    amount.to_csv(OUT_AMOUNT)

    # --- 转换价值（正股日线计算）---
    df_cv_new = calc_convert_val(pro, df_price_new, cb_basic, start, end)
    df_cv = _merge_wide(_load_existing(OUT_CV), df_cv_new)
    df_cv.to_csv(OUT_CV)

    # --- 剩余期限 ---
    df_mat_new  = build_maturity_matrix(df_price_new, cb_basic)
    df_maturity = _merge_wide(_load_existing(OUT_MATURITY), df_mat_new)
    df_maturity.to_csv(OUT_MATURITY)

    # --- 无风险利率 ---
    yield_tbl = fetch_yield_curve(start, end)

    # --- 纯债价值（仅对新日期做 DCF）---
    if not df_mat_new.empty and not yield_tbl.empty:
        df_floor_new = calc_bond_floor_dcf(cb_basic, df_mat_new, yield_tbl)
        df_floor = _merge_wide(_load_existing(OUT_FLOOR), df_floor_new)
    else:
        df_floor = _load_existing(OUT_FLOOR) or pd.DataFrame()
    df_floor.to_csv(OUT_FLOOR)

    # --- 正股市值 ---
    df_mv_new  = fetch_stock_mv(pro, cb_basic, df_price_new, start, end)
    df_stk_mv  = _merge_wide(_load_existing(OUT_STOCK_MV), df_mv_new)
    df_stk_mv.to_csv(OUT_STOCK_MV)

    # --- 静态余额（cb_basic.remain_size，广播到新日期行）---
    bal_new = build_balance_from_basic(df_price_new, cb_basic)
    balance = _merge_wide(_load_existing(OUT_BALANCE), bal_new)
    balance.to_csv(OUT_BALANCE)

    # --- 信用评级 ---
    df_rating_new = fetch_ratings(pro, df_price_new)
    df_rating     = _merge_wide(_load_existing(OUT_RATING), df_rating_new)
    df_rating.to_csv(OUT_RATING)

    # --- BPS ---
    df_bps_new = fetch_bps(pro, cb_basic, df_price_new, start, end)
    df_bps     = _merge_wide(_load_existing(OUT_BPS), df_bps_new)
    df_bps.to_csv(OUT_BPS)

    # --- 汇总 ---
    print(f"\n{'='*55}")
    print("数据管道完成！")
    for label, df in [
        ('可转债价格',   df_price),
        ('转换价值',     df_cv),
        ('纯债价值',     df_floor),
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
    args = parser.parse_args()
    run_pipeline(args.start, args.end)
