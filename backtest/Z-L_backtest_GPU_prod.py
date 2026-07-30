import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak
from tqdm import tqdm
import warnings
import time
import os
import sys
import json
import hashlib
import zlib
import math
from collections import Counter
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from numba import cuda, int8
from numba.cuda.random import create_xoroshiro128p_states, xoroshiro128p_normal_float64

from market_data_contracts import (
    DataContractError,
    PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    ZL_HISTORICAL_MIN_COVERAGE,
    ZL_MIN_COVERAGE_ENFORCED_FROM,
    ZL_MIN_PRICING_COVERAGE,
    build_clause_history_state,
    build_observed_volatility,
    build_risk_free_rate_matrix,
    calculate_accrued_interest,
    load_rebuildable_matrix_cache,
    parse_coupon_schedule,
    select_completed_weekly_dates,
    select_dates_after_checkpoint,
    select_input_refresh_dates,
    select_pending_calculation_dates,
    validate_pricing_coverage,
)
from token_loader import load_tushare_token
from zl_cpu_backend import price_batch_cpu

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 忽略计算警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置与数据读取 (基于 Tushare Pipeline CSV 缓存)
# ==========================================
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))  # backtest/ 目录
def _option_value(name, default):
    try:
        return sys.argv[sys.argv.index(name) + 1].strip().lower()
    except (ValueError, IndexError):
        return default


REBUILD_ALL = '--rebuild-all' in sys.argv
REFRESH_INPUT_CACHE = '--refresh-input-cache' in sys.argv
WEEKLY_ONLY = '--weekly' in sys.argv
OFFLINE_INPUTS = '--offline-inputs' in sys.argv
RESUME_CHECKPOINT = '--resume-checkpoint' in sys.argv
EXECUTION_BACKEND = _option_value("--backend", "cuda")
if EXECUTION_BACKEND not in {"cpu", "cuda"}:
    raise SystemExit("--backend must be either cpu or cuda")
MC_N_PATHS = 10000
ZL_INPUT_CONTRACT_VERSION = "weekly-observed-v2"
ZL_MODEL_IMPLEMENTATION_VERSION = (
    "4d28dd36326b3e69a45197b0458695d407e7d38d774075a5f208610db23f4431"
)
ZL_MANIFEST_FILE = os.path.join(PIPELINE_DIR, "ZL_Model_Manifest.json")
MODEL_PARAMETERS = {
    "mc_paths": MC_N_PATHS,
    "weekly_only": WEEKLY_ONLY,
    "seed_scheme": "crc32-date-v1",
}
BASIC_FINGERPRINT_FIELDS = [
    "par_value",
    "value_date",
    "maturity_date",
    "maturity_call_price",
    "rate_clause",
]
CLAUSE_FINGERPRINT_FIELDS = [
    "put_trigger_ratio",
    "put_window_days",
    "put_eligible_years",
    "redeem_trigger_ratio",
    "redeem_window_days",
    "redeem_required_days",
]


def _load_verified_manifest() -> dict:
    try:
        with open(ZL_MANIFEST_FILE, encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return manifest if isinstance(manifest, dict) else {}


verified_manifest = _load_verified_manifest()
manifest_contract_matches = (
    verified_manifest.get("contract_version")
    == ZL_INPUT_CONTRACT_VERSION
)
can_reuse_history = False
verified_dates = {
    pd.Timestamp(value)
    for value in verified_manifest.get("verified_dates", [])
    if pd.notna(pd.to_datetime(value, errors="coerce"))
}


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _update_frame_digest(
    digest,
    *,
    label: str,
    frame: pd.DataFrame,
) -> None:
    normalized = frame.sort_index().sort_index(axis=1).copy()
    columns_payload = json.dumps(
        [str(column) for column in normalized.columns],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    dtypes_payload = json.dumps(
        [str(dtype) for dtype in normalized.dtypes],
        separators=(",", ":"),
    )
    row_hashes = pd.util.hash_pandas_object(
        normalized,
        index=True,
        categorize=False,
    )
    digest.update(label.encode("utf-8"))
    digest.update(b"\0")
    digest.update(columns_payload.encode("utf-8"))
    digest.update(b"\0")
    digest.update(dtypes_payload.encode("ascii"))
    digest.update(b"\0")
    digest.update(row_hashes.to_numpy(dtype=np.uint64).tobytes())
    digest.update(b"\0")


def _build_input_fingerprint(cutoff: pd.Timestamp) -> str:
    price_prefix = df_price.loc[df_price.index <= cutoff]
    active_bonds = price_prefix.columns[price_prefix.notna().any(axis=0)]
    if price_prefix.empty or len(active_bonds) == 0:
        raise DataContractError(
            f"cannot fingerprint empty ZL inputs through {cutoff.date()}"
        )

    digest = hashlib.sha256()
    digest.update(ZL_MODEL_IMPLEMENTATION_VERSION.encode("ascii"))
    digest.update(
        json.dumps(
            MODEL_PARAMETERS,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    matrix_inputs = {
        "market_price": df_price,
        "conversion_value_history": df_cv_history,
        "bond_floor": df_floor,
        "maturity": df_maturity,
        "volatility": df_volatility,
        "risk_free_rate": rf_df,
        "credit_spread": df_spread,
    }
    for label, frame in matrix_inputs.items():
        prefix = frame.loc[frame.index <= cutoff].reindex(columns=active_bonds)
        _update_frame_digest(digest, label=label, frame=prefix)

    static_inputs = {
        "basic_terms": _basic.reindex(columns=BASIC_FINGERPRINT_FIELDS),
        "clause_terms": _clauses.reindex(
            columns=CLAUSE_FINGERPRINT_FIELDS
        ),
    }
    for label, frame in static_inputs.items():
        relevant = frame.reindex(index=active_bonds)
        _update_frame_digest(digest, label=label, frame=relevant)
    return digest.hexdigest()


def _load_csv(filename):
    path = os.path.join(PIPELINE_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Pipeline cache not found: {path}\n"
            "请先运行 python backtest/data_pipeline.py 生成数据缓存。"
        )
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, errors='coerce')
    df = df[df.index.notna()]
    df = df[~df.index.duplicated(keep='first')]
    return df.apply(pd.to_numeric, errors='coerce')


print("1. 正在从 Tushare Pipeline CSV 读取数据 ...")
df_price    = _load_csv('cb_price_cache.csv')
df_cv       = _load_csv('cb_convert_val_cache.csv')
df_floor    = _load_csv('cb_bond_floor_cache.csv')
df_maturity = _load_csv('cb_maturity_cache.csv')

# 对齐日期索引（取交集）
common_idx = df_price.index.intersection(df_cv.index).intersection(df_floor.index)
df_price    = df_price.loc[common_idx]
df_cv       = df_cv.loc[common_idx]
df_floor    = df_floor.loc[common_idx]
df_maturity = df_maturity.loc[common_idx]
valuation_dates = (
    select_completed_weekly_dates(df_price.index)
    if WEEKLY_ONLY
    else df_price.index
)
if len(valuation_dates) == 0:
    raise DataContractError("no completed weekly valuation date")
coverage_dates = valuation_dates[-1:]

# ==========================================
# 2. 读取转债列表
# ==========================================
print("2. 正在读取转债列表...")


def normalize_code(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip().upper()
    if s == "" or s == "NAN":
        return np.nan
    return s


# 2.1 从 cb_basic_info.csv 读取映射
try:
    df_basic_info = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_basic_info.csv'))
    df_map = df_basic_info.dropna(subset=['ts_code', 'stk_cd'])
    df_map = df_map.drop_duplicates(subset=['ts_code'], keep='first')
    bond_to_stock      = dict(zip(df_map['ts_code'], df_map['stk_cd']))
    stock_code_to_bond = dict(zip(df_map['stk_cd'],  df_map['ts_code']))
    stock_name_to_bond = {}
    print(f"   已加载 {len(bond_to_stock)} 条转债映射关系（来源：cb_basic_info.csv）")
except Exception as e:
    print(f"   映射读取失败: {e}")
    bond_to_stock = {}
    stock_code_to_bond = {}
    stock_name_to_bond = {}

# 确保列与 df_price 一致 (取交集)
valid_bonds = df_price.columns.intersection(pd.Index(bond_to_stock))
df_price    = df_price[valid_bonds]
df_cv       = df_cv[valid_bonds]
df_floor    = df_floor[valid_bonds]
df_maturity = df_maturity[valid_bonds]

print(f"   转债映射对齐完成，有效转债数量: {len(valid_bonds)}")

# 2.2 获取真实回售/赎回条款
# ==========================================
_basic = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_basic_info.csv'))
_basic = _basic.drop_duplicates('ts_code', keep='last').set_index('ts_code')
_clauses = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_clause_terms.csv'))
_clauses = _clauses.loc[
    _clauses['source_ok'].astype(str).str.lower().eq('true')
].drop_duplicates('ts_code', keep='last').set_index('ts_code')
print(f"   已加载 {_clauses.shape[0]} 只转债的真实条款")


# ==========================================
# 3. Tushare 获取正股波动率
# ==========================================
print("3. 开始通过 Tushare 获取正股历史波动率...")
# Tushare token 从环境变量 TUSHARE_TOKEN 或 backtest/tushare_token.txt 读取
try:
    # Token 仅传入当前客户端，不写入用户主目录 tk.csv。
    pro = ts.pro_api(load_tushare_token())
except Exception as e:
    # 如果初始化失败，打印警告信息并将 pro 设为 None，后续代码会跳过网络请求
    print(f"Warning: Tushare 初始化失败: {e}")
    pro = None

# Z-L 模型缓存文件
VOL_CACHE_FILE = os.path.join(PIPELINE_DIR, "bs_volatility_cache.csv")
def _fetch_bond_stock_data(bond_code):
    """拉取单只转债正股的 250 日滚动年化波动率，失败返回 None。"""
    stock_code_full = bond_to_stock.get(bond_code)
    if not stock_code_full or pro is None:
        return None
    start_dt = (
        df_price.index.min() - pd.Timedelta(days=500)
    ).strftime("%Y%m%d")
    end_dt = df_price.index.max().strftime("%Y%m%d")
    df_k = ts.pro_bar(
        ts_code=stock_code_full,
        adj='qfq',
        start_date=start_dt,
        end_date=end_dt,
        api=pro,
    )
    if df_k is None or df_k.empty:
        return None
    df_k['trade_date'] = pd.to_datetime(df_k['trade_date'])
    df_k = df_k.sort_values('trade_date').set_index('trade_date')
    vol = build_observed_volatility(
        adjusted_close=df_k['close'],
        target_dates=df_price.index,
        window=250,
        min_observations=60,
    )
    return vol


df_volatility = load_rebuildable_matrix_cache(
    path=VOL_CACHE_FILE,
    index=df_price.index,
    columns=df_price.columns,
    refresh_cache=REFRESH_INPUT_CACHE,
)
# 增量补算: 有市场价但波动率缺失的债券（新债，或缓存生成后新增的交易日）
# 修复历史 bug: 旧逻辑缓存存在时直接用 NaN→0.40 兜底，新交易日永远拿不到真实波动率
# 注意: 旧版波动率缓存曾把 0.40 兜底值写进文件，无法与真实值区分，建议删除两个 zl_*_cache.csv 重建
input_check_dates = select_input_refresh_dates(
    all_dates=valuation_dates,
    coverage_dates=coverage_dates,
    refresh_cache=REFRESH_INPUT_CACHE,
)
pending = (
    df_price.loc[input_check_dates].notna()
    & df_volatility.loc[input_check_dates].isna()
).any()
pending_bonds = pending[pending].index.tolist()
if pending_bonds and OFFLINE_INPUTS:
    print(
        f"   {len(pending_bonds)} 只转债的波动率存在空值；"
        "离线输入模式下不发起 Tushare 请求，将由定价覆盖率契约校验。"
    )
elif pending_bonds:
    print(f"   {len(pending_bonds)} 只转债的正股数据存在缺口，开始增量补算 ...")
    updated_count = 0
    for bond_code in tqdm(pending_bonds, desc="Fetching Stock Data"):
        try:
            fetched = _fetch_bond_stock_data(bond_code)
            if fetched is not None:
                df_volatility[bond_code] = fetched
                updated_count += 1
            time.sleep(0.02)
        except Exception as e:
            print(f"   获取 {bond_code} 正股数据失败: {e}")
    if updated_count:
        df_volatility.to_csv(VOL_CACHE_FILE)
        print(f"   缓存已更新 ({updated_count} 只)。")

# 缺失波动率保持 NaN，由定价样本门禁排除。

print("   股票数据准备完成。")

# ==========================================
# 4. 获取无风险利率 (Akshare)
# ==========================================
print("4. 获取国债收益率...")
# 注意: rf_yield_cache.csv 是期限格式（列 = 1/2/3/5/7/10 年），由 data_pipeline.py 维护。
# 绝不可 reindex 到债券列（会全变 NaN 被 0.02 静默吞掉），必须按期限插值。
RF_CACHE_FILE = os.path.join(PIPELINE_DIR, "rf_yield_cache.csv")


def _load_tenor_yield_table():
    """返回期限格式利率表（行=日期，列=float 期限，单调递增），失败返回 None。"""
    if os.path.exists(RF_CACHE_FILE):
        print("   发现利率缓存（期限格式），正在读取...")
        tbl = pd.read_csv(RF_CACHE_FILE, index_col=0, parse_dates=True)
        import re as _re
        tbl.columns = [float(_re.match(r'^(\d+(?:\.\d+)?)', str(c)).group(1)) for c in tbl.columns]
        tbl = tbl.T.groupby(level=0).first().T.sort_index(axis=1)
        return tbl
    try:
        end_dt = df_price.index.max().strftime("%Y%m%d")
        df_yield = ak.bond_china_yield(start_date="20190101", end_date=end_dt)
        target_curve = df_yield[df_yield['曲线名称'] == '中债国债收益率曲线'].copy()
        target_curve['日期'] = pd.to_datetime(target_curve['日期'])
        target_curve.set_index('日期', inplace=True)
        tenor_map = {'1年': 1.0, '2年': 2.0, '3年': 3.0, '5年': 5.0, '7年': 7.0, '10年': 10.0}
        available_cols = [c for c in tenor_map if c in target_curve.columns]
        tbl = target_curve[available_cols] / 100.0
        tbl.columns = [tenor_map[c] for c in available_cols]
        return tbl.sort_index(axis=1).sort_index()
    except Exception as e:
        print(f"   获取利率失败: {e}")
        return None


yield_table = _load_tenor_yield_table()

if yield_table is None or yield_table.empty:
    raise DataContractError("无可用 AkShare 国债收益率曲线")
else:
    df_cv_history = df_cv.copy()
    if WEEKLY_ONLY:
        curve_start = yield_table.dropna(how="all").index.min()
        valuation_dates = valuation_dates[valuation_dates >= curve_start]
        if len(valuation_dates) == 0:
            raise DataContractError(
                "weekly dates are earlier than the yield curve"
            )
        supported_dates = df_price.index[df_price.index >= curve_start]
        df_price = df_price.loc[supported_dates]
        df_cv = df_cv.loc[supported_dates]
        df_floor = df_floor.loc[supported_dates]
        df_maturity = df_maturity.loc[supported_dates]
        df_volatility = df_volatility.loc[supported_dates]
        coverage_dates = valuation_dates[-1:]
    rf_df = build_risk_free_rate_matrix(
        curve=yield_table,
        maturity=df_maturity,
    )
    print("   无风险利率期限结构匹配完成。")

# ==========================================
# 5. Z-L 模型 (Monte Carlo)
# ==========================================
print("5. 启动 Z-L 模型蒙特卡洛模拟...")

# 信用利差由 data_pipeline.py 使用 Tushare 每日纯债价值反解。
df_spread = _load_csv('cb_credit_spread_cache.csv').reindex(
    index=df_price.index,
    columns=df_price.columns,
)
print(f"   隐含信用利差非空率: {df_spread.notna().mean().mean():.1%}")

# ==========================================
# Z-L 蒙特卡洛核心 —— GPU (CUDA) 批处理版
# ==========================================
# 注意: 本 kernel 保持项目既定的 ZL 条款博弈逻辑
#   - 未获得逐债下修规则时，不假设自动下修
#   - 强赎收益: 纯转股 S*(100/X) (不与赎回价取 max)
#   - 时间步: steps = max(50, int(T*240)), dt = T/steps
#   - 回售执行: put_count>=30 且 t > put_start_idx (严格大于)
# 仅把执行设备从 CPU(njit) 换成 GPU(cuda.jit), 模型设定不变。
@cuda.jit
def zl_mc_kernel_batch(S0_arr, X0_arr, r_arr, cs_arr, sigma_arr, T_arr,
                       maturity_redem_arr, call_price_arr,
                       put_price_arr, put_barrier_arr, put_window_arr,
                       put_years_arr, redeem_ratio_arr, redeem_window_arr,
                       redeem_required_arr, initial_put_count_arr,
                       initial_redeem_count_arr, initial_redeem_flags_arr,
                       N, rng_states, out_pv):
    """每个线程模拟一只债券的一条路径; tid = bond_idx * N + path_idx。"""
    tid = cuda.grid(1)
    bond_idx = tid // N

    if bond_idx >= S0_arr.shape[0]:
        return

    S0 = S0_arr[bond_idx]
    X0 = X0_arr[bond_idx]
    r = r_arr[bond_idx]
    credit_spread = cs_arr[bond_idx]
    sigma = sigma_arr[bond_idx]
    T = T_arr[bond_idx]
    redemption_price = maturity_redem_arr[bond_idx]
    call_price = call_price_arr[bond_idx]
    put_price = put_price_arr[bond_idx]
    put_barrier = put_barrier_arr[bond_idx]
    put_window = int(put_window_arr[bond_idx])
    put_years = put_years_arr[bond_idx]
    redeem_ratio = redeem_ratio_arr[bond_idx]
    redeem_window = int(redeem_window_arr[bond_idx])
    redeem_required = int(redeem_required_arr[bond_idx])

    # 时间步 / dt / 回售起始步 —— 与 CPU 版完全一致
    steps = max(50, int(T * 240.0))
    dt = T / steps
    put_start_time = T - put_years
    if put_start_time < 0.0:
        put_start_time = 0.0
    put_start_idx = int(put_start_time / dt)

    drift = (r - 0.5 * sigma * sigma) * dt
    vol_sqrt_dt = sigma * math.sqrt(dt)

    S_curr = S0
    X_curr = X0
    put_count = int(initial_put_count_arr[bond_idx])
    redeem_count = int(initial_redeem_count_arr[bond_idx])
    redeem_flags = cuda.local.array(64, dtype=int8)
    for flag_idx in range(64):
        redeem_flags[flag_idx] = initial_redeem_flags_arr[
            bond_idx, flag_idx
        ]
    path_end_time = T
    path_end_val = 0.0
    is_active = True

    for t in range(1, steps + 1):
        z = xoroshiro128p_normal_float64(rng_states, tid)
        S_curr = S_curr * math.exp(drift + vol_sqrt_dt * z)

        # (1) 回售触发计数
        if t <= put_start_idx:
            put_count = 0
        elif S_curr < put_barrier * X_curr:
            put_count += 1
        else:
            put_count = 0

        # 未获得逐债下修条款时不假设自动下修。

        # (2) 投资者博弈: 按真实连续天数执行回售
        if put_count >= put_window:
            path_end_time = t * dt
            path_end_val = put_price
            is_active = False
            break

        # (3) 强赎: 真实 N 日窗口内至少 M 日达到触发比例
        slot = (t - 1) % redeem_window
        redeem_count -= redeem_flags[slot]
        flag = 1 if S_curr >= redeem_ratio * X_curr else 0
        redeem_flags[slot] = flag
        redeem_count += flag
        if redeem_count >= redeem_required:
            path_end_time = t * dt
            conv_value = S_curr * (X0 / X_curr)
            path_end_val = conv_value if conv_value > call_price else call_price
            is_active = False
            break

    # 持有到期: max(赎回价, 转股价值)
    if is_active:
        conv_val = X0 / X_curr * S_curr
        path_end_val = conv_val if conv_val > redemption_price else redemption_price

    out_pv[tid] = path_end_val * math.exp(-(r + credit_spread) * path_end_time)


def zl_mc_pricing_batch(params, N=10000, seed=42, threads_per_block=256):
    """
    GPU 批量定价: 一次 kernel 启动算完当日所有待定价债券。

    params: dict of np.float64 一维数组, 键为
        S0, X0, r, cs, sigma, T, maturity_redem, call_price,
        put_price, put_barrier, put_window, put_years, redeem_ratio,
        redeem_window, redeem_required
        initial_put_count, initial_redeem_count, initial_redeem_flags
        (长度均 = 当日待算债券数 num_bonds)
    返回: np.ndarray, shape=(num_bonds,), 各债券模型价(路径均值)。
    """
    num_bonds = len(params["S0"])
    total_threads = num_bonds * N

    out_pv_device = cuda.device_array(total_threads, dtype=np.float64)
    rng_states = create_xoroshiro128p_states(total_threads, seed=seed)

    blocks_per_grid = (total_threads + threads_per_block - 1) // threads_per_block
    zl_mc_kernel_batch[blocks_per_grid, threads_per_block](
        cuda.to_device(params["S0"]), cuda.to_device(params["X0"]),
        cuda.to_device(params["r"]), cuda.to_device(params["cs"]),
        cuda.to_device(params["sigma"]), cuda.to_device(params["T"]),
        cuda.to_device(params["maturity_redem"]),
        cuda.to_device(params["call_price"]), cuda.to_device(params["put_price"]),
        cuda.to_device(params["put_barrier"]), cuda.to_device(params["put_window"]),
        cuda.to_device(params["put_years"]), cuda.to_device(params["redeem_ratio"]),
        cuda.to_device(params["redeem_window"]), cuda.to_device(params["redeem_required"]),
        cuda.to_device(params["initial_put_count"]),
        cuda.to_device(params["initial_redeem_count"]),
        cuda.to_device(params["initial_redeem_flags"]),
        N, rng_states, out_pv_device,
    )
    out_pv = out_pv_device.copy_to_host()
    return out_pv.reshape(num_bonds, N).mean(axis=1)

# 结果存储
results = []

calc_dates = valuation_dates

print(f"即将开始计算：共 {len(calc_dates)} 个交易日，{len(df_price.columns)} 只转债")

# 创建结果 DataFrame
df_zl_model = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_zl_error = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_diff_pct = pd.DataFrame(index=df_price.index, columns=df_price.columns)

# 增量计算逻辑
SUMMARY_FILE = os.path.join(PIPELINE_DIR, "ZL_Model_Summary.xlsx")
resume_checkpoint_cutoff = None
if (
    manifest_contract_matches
    and verified_dates
    and os.path.exists(SUMMARY_FILE)
):
    verified_cutoff = max(verified_dates)
    current_input_fingerprint = _build_input_fingerprint(verified_cutoff)
    can_reuse_history = (
        verified_manifest.get("input_cutoff")
        == verified_cutoff.date().isoformat()
        and verified_manifest.get("model_parameters") == MODEL_PARAMETERS
        and verified_manifest.get("input_fingerprint")
        == current_input_fingerprint
        and verified_manifest.get("output_sha256")
        == _sha256_file(SUMMARY_FILE)
    )
if os.path.exists(SUMMARY_FILE) and (
    (not REBUILD_ALL and can_reuse_history)
    or (REBUILD_ALL and RESUME_CHECKPOINT)
):
    print(f"   发现已存在的汇总文件：{SUMMARY_FILE}")
    if REBUILD_ALL and RESUME_CHECKPOINT:
        print("   Resuming explicitly from the local rebuild checkpoint...")
    else:
        print("   读取历史结果并执行增量计算...")
    try:
        df_zl_model_hist = pd.read_excel(SUMMARY_FILE, sheet_name="理论价格", index_col=0, engine='openpyxl')
        df_zl_model_hist.index = pd.to_datetime(df_zl_model_hist.index, errors='coerce')
        df_zl_model_hist = df_zl_model_hist[df_zl_model_hist.index.notnull()]
        df_zl_model_hist = df_zl_model_hist.apply(pd.to_numeric, errors='coerce')
        if REBUILD_ALL and RESUME_CHECKPOINT:
            saved_rows = df_zl_model_hist.dropna(how="all")
            if saved_rows.empty:
                raise DataContractError("local ZL checkpoint has no saved rows")
            resume_checkpoint_cutoff = saved_rows.index.max()
        else:
            df_zl_model_hist = df_zl_model_hist.loc[
                df_zl_model_hist.index.isin(verified_dates)
            ]
        df_zl_model_hist = df_zl_model_hist.reindex(index=df_price.index, columns=df_price.columns)
        df_zl_model.update(df_zl_model_hist)

    except Exception as e:
        if REBUILD_ALL and RESUME_CHECKPOINT:
            raise DataContractError(
                f"cannot resume local ZL checkpoint: {e}"
            ) from e
        print(f"   历史结果读取失败，改为全量计算：{e}")
elif os.path.exists(SUMMARY_FILE) and not REBUILD_ALL:
    print(
        "   Existing ZL history ignored: no current-contract verification "
        "manifest"
    )

if WEEKLY_ONLY:
    # Recalculate the latest completed week under the current input contract.
    df_zl_model.loc[coverage_dates] = np.nan
    df_zl_error.loc[coverage_dates] = np.nan
    df_diff_pct.loc[coverage_dates] = np.nan

pending_mask = df_price.notna() & df_zl_model.isna()
calc_dates_to_run = select_pending_calculation_dates(
    calculation_dates=calc_dates,
    pending_mask=pending_mask,
)
if resume_checkpoint_cutoff is not None:
    calc_dates_to_run = select_dates_after_checkpoint(
        calculation_dates=calc_dates_to_run,
        checkpoint_cutoff=resume_checkpoint_cutoff,
    )
calc_dates_to_run = calc_dates_to_run[calc_dates_to_run.notnull()]

print(f"增量待计算交易日：{len(calc_dates_to_run)}")

# --- 检查点 (checkpoint) 变量 ---
_dates_since_checkpoint = 0
_CHECKPOINT_EVERY = 10  # 每处理 10 个交易日保存一次中间结果

# GPU 可用性检查（本脚本仅支持 CUDA）
if EXECUTION_BACKEND == "cuda":
    if not cuda.is_available():
        raise SystemExit(
            "CUDA is unavailable. Use --backend cpu on a CPU-only runner."
        )
    device_name = cuda.get_current_device().name
    if isinstance(device_name, bytes):
        device_name = device_name.decode()
    print(f"   ZL execution backend: CUDA ({device_name})")
else:
    print("   ZL execution backend: CPU (Numba parallel)")

def _observed_clause_inputs(bond_code, date):
    if bond_code not in _basic.index or bond_code not in _clauses.index:
        return None
    basic_row = _basic.loc[bond_code]
    clause_row = _clauses.loc[bond_code]
    values = {
        field: pd.to_numeric(clause_row.get(field), errors='coerce')
        for field in CLAUSE_FINGERPRINT_FIELDS
    }
    if any(pd.isna(value) for value in values.values()):
        return None
    redeem_window = int(values['redeem_window_days'])
    put_window = int(values['put_window_days'])
    if (
        redeem_window <= 0
        or redeem_window > 64
        or put_window <= 0
        or int(values['redeem_required_days']) <= 0
        or int(values['redeem_required_days']) > redeem_window
    ):
        return None
    par = pd.to_numeric(basic_row.get('par_value'), errors='coerce')
    value_date = pd.to_datetime(basic_row.get('value_date'), errors='coerce')
    maturity_date = pd.to_datetime(
        basic_row.get('maturity_date'), errors='coerce'
    )
    maturity_redemption = pd.to_numeric(
        basic_row.get('maturity_call_price'),
        errors='coerce',
    )
    if (
        pd.isna(par)
        or par <= 0
        or pd.isna(value_date)
        or pd.isna(maturity_date)
        or pd.isna(maturity_redemption)
        or maturity_redemption <= 0
    ):
        return None
    try:
        coupon_schedule = parse_coupon_schedule(basic_row.get('rate_clause'))
        accrued = calculate_accrued_interest(
            as_of=pd.Timestamp(date),
            value_date=value_date,
            par_value=float(par),
            coupon_schedule=coupon_schedule,
        )
    except DataContractError:
        return None
    exercise_price = float(par) + float(accrued)
    return {
        'par': float(par),
        'maturity_redem': float(maturity_redemption),
        'call_price': exercise_price,
        'put_price': exercise_price,
        'put_barrier': float(values['put_trigger_ratio']),
        'put_window': put_window,
        'put_years': float(values['put_eligible_years']),
        'put_eligible_start': maturity_date - pd.DateOffset(
            years=int(values['put_eligible_years'])
        ),
        'redeem_ratio': float(values['redeem_trigger_ratio']),
        'redeem_window': redeem_window,
        'redeem_required': int(values['redeem_required_days']),
    }


# 遍历每一天 (使用 tqdm 显示进度); 每个交易日组装参数后一次性 GPU 批处理
date_error_count = 0
skip_reasons = Counter()
for date in tqdm(calc_dates_to_run, desc="ZL Model Backtest (GPU)"):
    try:
        row_price = df_price.loc[date]
        row_cv = df_cv.loc[date]
        row_rf = rf_df.loc[date]
        row_vol = df_volatility.loc[date]
        row_mat = df_maturity.loc[date]
        row_spread = df_spread.loc[date]

        # 组装当日待定价债券参数 (数据清洗与归一化逻辑与 CPU 版逐字对应)
        batch_codes = []
        (
            p_S0, p_X0, p_r, p_cs, p_sigma, p_T,
            p_maturity_redem, p_call, p_put, p_barrier, p_put_window,
            p_put_years, p_redeem_ratio, p_redeem_window,
            p_redeem_required, p_initial_put_count,
            p_initial_redeem_count, p_initial_redeem_flags,
        ) = (
            [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
            [], [], []
        )
        batch_market = []
        for bond_code in df_price.columns:
            if pd.notna(df_zl_model.loc[date, bond_code]):
                continue

            market_price = row_price[bond_code]
            cv = row_cv[bond_code]
            r = row_rf[bond_code]
            sigma = row_vol[bond_code]
            T = row_mat[bond_code]
            cs = row_spread[bond_code]

            clause_inputs = _observed_clause_inputs(bond_code, date)
            required_values = {
                'market_price': market_price,
                'conversion_value': cv,
                'maturity': T,
                'volatility': sigma,
                'risk_free_rate': r,
                'credit_spread': cs,
            }
            missing_fields = [
                name for name, value in required_values.items()
                if pd.isna(value)
            ]
            if missing_fields:
                skip_reasons[f"missing_{missing_fields[0]}"] += 1
                continue
            if T <= 0:
                skip_reasons['nonpositive_maturity'] += 1
                continue
            if sigma <= 0:
                skip_reasons['nonpositive_volatility'] += 1
                continue
            if cs < 0:
                skip_reasons['negative_credit_spread'] += 1
                continue
            if clause_inputs is None:
                skip_reasons['missing_clause_terms'] += 1
                continue
            try:
                clause_history = build_clause_history_state(
                    conversion_value=df_cv_history[bond_code],
                    valuation_date=date,
                    par_value=clause_inputs['par'],
                    put_trigger_ratio=clause_inputs['put_barrier'],
                    put_eligible_start=clause_inputs[
                        'put_eligible_start'
                    ],
                    redeem_trigger_ratio=clause_inputs['redeem_ratio'],
                    redeem_window_days=clause_inputs['redeem_window'],
                )
            except DataContractError:
                skip_reasons['invalid_clause_history'] += 1
                continue

            S0_sim = cv
            X0_sim = clause_inputs['par']

            batch_codes.append(bond_code)
            batch_market.append(market_price)
            p_S0.append(S0_sim)
            p_X0.append(X0_sim)
            p_r.append(r)
            p_cs.append(cs)
            p_sigma.append(sigma)
            p_T.append(T)
            p_maturity_redem.append(clause_inputs['maturity_redem'])
            p_call.append(clause_inputs['call_price'])
            p_put.append(clause_inputs['put_price'])
            p_barrier.append(clause_inputs['put_barrier'])
            p_put_window.append(clause_inputs['put_window'])
            p_put_years.append(clause_inputs['put_years'])
            p_redeem_ratio.append(clause_inputs['redeem_ratio'])
            p_redeem_window.append(clause_inputs['redeem_window'])
            p_redeem_required.append(clause_inputs['redeem_required'])
            p_initial_put_count.append(
                clause_history.put_consecutive_days
            )
            p_initial_redeem_count.append(clause_history.redeem_count)
            p_initial_redeem_flags.append(clause_history.redeem_flags)

        if not batch_codes:
            continue

        params = {
            "S0": np.asarray(p_S0, dtype=np.float64),
            "X0": np.asarray(p_X0, dtype=np.float64),
            "r": np.asarray(p_r, dtype=np.float64),
            "cs": np.asarray(p_cs, dtype=np.float64),
            "sigma": np.asarray(p_sigma, dtype=np.float64),
            "T": np.asarray(p_T, dtype=np.float64),
            "maturity_redem": np.asarray(p_maturity_redem, dtype=np.float64),
            "call_price": np.asarray(p_call, dtype=np.float64),
            "put_price": np.asarray(p_put, dtype=np.float64),
            "put_barrier": np.asarray(p_barrier, dtype=np.float64),
            "put_window": np.asarray(p_put_window, dtype=np.float64),
            "put_years": np.asarray(p_put_years, dtype=np.float64),
            "redeem_ratio": np.asarray(p_redeem_ratio, dtype=np.float64),
            "redeem_window": np.asarray(p_redeem_window, dtype=np.float64),
            "redeem_required": np.asarray(p_redeem_required, dtype=np.float64),
            "initial_put_count": np.asarray(
                p_initial_put_count, dtype=np.int32
            ),
            "initial_redeem_count": np.asarray(
                p_initial_redeem_count, dtype=np.int32
            ),
            "initial_redeem_flags": np.asarray(
                p_initial_redeem_flags, dtype=np.int8
            ),
        }
        # 按交易日派生确定性种子: 可复现, 且各债券/路径随机流互相独立 (tid 偏移)
        day_seed = zlib.crc32(str(date).encode()) & 0x7FFFFFFF
        if EXECUTION_BACKEND == "cuda":
            model_prices = zl_mc_pricing_batch(
                params,
                N=MC_N_PATHS,
                seed=day_seed,
            )
        else:
            model_prices = price_batch_cpu(
                params,
                paths=MC_N_PATHS,
                seed=day_seed,
            )

        for bond_code, model_price, market_price in zip(batch_codes, model_prices, batch_market):
            df_zl_model.loc[date, bond_code] = model_price
            df_zl_error.loc[date, bond_code] = model_price - market_price

    except Exception as e:
        date_error_count += 1
        if date_error_count <= 5:
            print(f"Date-level error on {date}: {e}")

    # --- 周期检查点 ---
    _dates_since_checkpoint += 1
    if _dates_since_checkpoint >= _CHECKPOINT_EVERY:
        try:
            _chk_diff = df_zl_error.copy()
            _chk_pct  = df_zl_error / df_price.replace(0, np.nan)
            with pd.ExcelWriter(SUMMARY_FILE) as _w:
                df_zl_model.to_excel(_w, sheet_name="理论价格")
                df_price.to_excel(_w, sheet_name="市场价格")
                _chk_diff.to_excel(_w, sheet_name="绝对偏差")
                _chk_pct.to_excel(_w, sheet_name="相对偏差")
            tqdm.write(f"  [checkpoint] 已保存至 {date.date()}")
        except Exception as _ce:
            tqdm.write(f"  [checkpoint] 保存失败: {_ce}")
        _dates_since_checkpoint = 0

print(f"日期级异常数量：{date_error_count}")
if skip_reasons:
    print(f"Skipped pricing cells by reason: {dict(skip_reasons)}")

if REBUILD_ALL:
    rebuild_market = df_price.loc[calc_dates]
    rebuild_model = df_zl_model.loc[calc_dates]
    expected_cells = int(rebuild_market.notna().sum().sum())
    priced_cells = int(
        (rebuild_market.notna() & rebuild_model.notna()).sum().sum()
    )
    rebuild_coverage = (
        priced_cells / expected_cells if expected_cells else 0.0
    )
    min_rebuild_coverage = float(
        os.environ.get("ZL_MIN_REBUILD_COVERAGE", "0.90")
    )
    print(
        f"ZL rebuild coverage: {priced_cells}/{expected_cells} "
        f"({rebuild_coverage:.2%})"
    )
    if rebuild_coverage < min_rebuild_coverage:
        raise DataContractError(
            "ZL rebuild coverage "
            f"{rebuild_coverage:.2%} is below required "
            f"{min_rebuild_coverage:.2%}; skips={dict(skip_reasons)}"
        )

contract_validation_dates = (
    valuation_dates if REBUILD_ALL and WEEKLY_ONLY else coverage_dates
)
validate_pricing_coverage(
    market_price=df_price,
    model_price=df_zl_model,
    dates=contract_validation_dates,
    min_coverage=float(
        os.environ.get(
            "ZL_MIN_PRICING_COVERAGE",
            str(ZL_MIN_PRICING_COVERAGE),
        )
    ),
    min_count=int(os.environ.get("ZL_MIN_PRICING_COUNT", "20")),
    label="ZL weekly" if WEEKLY_ONLY else "ZL latest",
    min_count_enforced_from=PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    historical_min_coverage=ZL_HISTORICAL_MIN_COVERAGE,
    min_coverage_enforced_from=ZL_MIN_COVERAGE_ENFORCED_FROM,
)

if WEEKLY_ONLY:
    # Persist only one observed valuation row per completed week. Daily source
    # history remains in the input caches for volatility and clause state.
    df_zl_model = df_zl_model.loc[valuation_dates]
    df_zl_error = df_zl_error.loc[valuation_dates]
    df_price = df_price.loc[valuation_dates]

# 偏差不得复用旧工作表，始终以已认证理论价和当前市场价重新计算。
df_diff = df_zl_model - df_price

safe_price = df_price.replace(0, np.nan)
df_diff_pct = df_diff / safe_price

df_zl_model.to_csv(os.path.join(PIPELINE_DIR, "ZL_Model_Prices.csv"))
# 注意: 不可写 Market_Prices.csv —— 那是 B-S 脚本的输出，两边 df_price 列集不同会互相覆盖
df_price.to_csv(os.path.join(PIPELINE_DIR, "ZL_Market_Prices.csv"))
df_diff.to_csv(os.path.join(PIPELINE_DIR, "ZL_Model_Deviation_Abs.csv"))
df_diff_pct.to_csv(os.path.join(PIPELINE_DIR, "ZL_Model_Deviation_Pct.csv"))

print("计算完成！")
print("结果已保存:")
print("1. 理论价格：'ZL_Model_Prices.csv'")
print("2. 市场价格：'ZL_Market_Prices.csv'")
print("3. 绝对偏差 (Model - Market): 'ZL_Model_Deviation_Abs.csv'")
print("4. 相对偏差 (Model - Market)/Market: 'ZL_Model_Deviation_Pct.csv'")

with pd.ExcelWriter(SUMMARY_FILE) as writer:
    df_zl_model.to_excel(writer, sheet_name="理论价格")
    df_price.to_excel(writer, sheet_name="市场价格")
    df_diff.to_excel(writer, sheet_name="绝对偏差")
    df_diff_pct.to_excel(writer, sheet_name="相对偏差")
print(f"5. 汇总 Excel: '{SUMMARY_FILE}'")

verified_output_dates = [
    pd.Timestamp(date).date().isoformat()
    for date in df_zl_model.index[df_zl_model.notna().any(axis=1)]
]
if not verified_output_dates:
    raise DataContractError("ZL output has no verified valuation dates")
verified_cutoff = pd.Timestamp(max(verified_output_dates))
manifest_payload = {
    "contract_version": ZL_INPUT_CONTRACT_VERSION,
    "execution_backend": EXECUTION_BACKEND,
    "verified_dates": verified_output_dates,
    "input_cutoff": verified_cutoff.date().isoformat(),
    "input_fingerprint": _build_input_fingerprint(verified_cutoff),
    "output_sha256": _sha256_file(SUMMARY_FILE),
    "model_parameters": MODEL_PARAMETERS,
}
manifest_temp = f"{ZL_MANIFEST_FILE}.tmp"
with open(manifest_temp, "w", encoding="utf-8") as manifest_file:
    json.dump(manifest_payload, manifest_file, ensure_ascii=False, indent=2)
os.replace(manifest_temp, ZL_MANIFEST_FILE)

# 误差指标 —— 在全部有效 (交易日 × 转债) 单元上汇总 (pooled)，
# 与真正写盘的 summary 完全一致。
# 修复历史 bug: 旧逻辑用 df.mean().mean()（先按债券列求均值再平均=按债等权），
# 且 SMAPE 对齐到只含历史 checkpoint 的 df_price_loaded（部分日期），
# 导致打印的统计量与实际写盘文件不符、有误导性。现统一用 df_zl_model 与
# df_price（即写盘的 理论价格/市场价格）逐单元 pooled 计算。
_m = df_zl_model.apply(pd.to_numeric, errors='coerce')
_p = df_price.apply(pd.to_numeric, errors='coerce')
_m = _m.reindex(index=_p.index, columns=_p.columns)
_mask = (_m.notna() & _p.notna() & (_p != 0)).values
_n = int(_mask.sum())
if _n == 0:
    mean_error = mae = rmse = mape = smape = float('nan')
else:
    _d = (_m - _p).values[_mask]
    _mk = _m.values[_mask]
    _pk = _p.values[_mask]
    mean_error = _d.mean()                                    # 平均误差 (Bias)
    mae = np.abs(_d).mean()                                   # 平均绝对误差
    rmse = np.sqrt((_d ** 2).mean())                          # 均方根误差
    mape = np.mean(np.abs(_d) / np.abs(_pk)) * 100            # 平均绝对百分比误差
    smape = np.mean(np.abs(_d) / ((np.abs(_mk) + np.abs(_pk)) / 2)) * 100  # 对称 MAPE

print("-" * 30)
print("模型整体误差指标:")
print(f"有效单元 (date×bond): {_n}")
print(f"Mean Error (Bias): {mean_error:.4f} 元")
print(f"MAE (平均绝对误差): {mae:.4f} 元")
print(f"RMSE (均方根误差): {rmse:.4f} 元")
print(f"MAPE (平均绝对百分比误差): {mape:.4f} %")
print(f"SMAPE (对称平均绝对百分比误差): {smape:.4f} %")
print("-" * 30)

# ==========================================
# 6. 绘图 
# ==========================================
print("6. 正在绘制图表...")

# 图1: 转债平均价格走势 (市场价 vs 模型价 vs 定价错误)
plt.figure(figsize=(12, 6))
ax1 = plt.gca()
# 计算每日市场平均价和模型平均价
daily_market_avg = df_price.mean(axis=1)
daily_model_avg = df_zl_model.mean(axis=1)
# 计算每日平均相对误差 (%)
daily_err_pct = df_diff_pct.mean(axis=1) * 100

l1, = ax1.plot(daily_market_avg.index, daily_model_avg, 'k-', label='ZL模型', linewidth=1.5)
l2, = ax1.plot(daily_market_avg.index, daily_market_avg, 'k--', label='市场价格', linewidth=1.5)
ax1.set_ylabel('转债平均价格 (元)')
ax1.set_xlabel('年份')

ax2 = ax1.twinx()
# 绘制误差面积图 (灰色填充)
# 确保数据类型为 float，并处理 inf/nan
daily_err_pct = daily_err_pct.astype(float).replace(
    [np.inf, -np.inf], np.nan
)
ax2.fill_between(daily_err_pct.index, daily_err_pct, 0, color='gray', alpha=0.5, label='定价错误')
ax2.set_ylabel('平均定价错误 (%)')
# 设置右轴范围，使其看起来像论文中的下方分布
ax2.set_ylim(-30, 80) 

# 合并图例
lines = [l1, l2]
labels = [line.get_label() for line in lines]
patch = mpatches.Patch(color='gray', alpha=0.5, label='定价错误')
lines.append(patch)
labels.append('定价错误')
ax1.legend(lines, labels, loc='upper center')

plt.title('图1 ZL模型定价结果与市场价格对比')
plt.savefig(os.path.join(PIPELINE_DIR, "Fig1_ZL_Price_Time_Series.png"), dpi=300)
plt.close()

# 图2: 定价结果与在值程度的关系 (Moneyness)
# 在值程度 = log(S/X)
# 我们需要对所有样本点进行 Moneyness 分组
# 展开数据
# 在值程度 measure: ln(CV/100)
moneyness = np.log(df_cv / 100.0)

# 将 DataFrame 展平为 Series
s_market = df_price.stack()
s_model = df_zl_model.stack()
s_moneyness = moneyness.stack()

# 合并
df_scatter = pd.DataFrame({'market': s_market, 'model': s_model, 'moneyness': s_moneyness})
df_scatter = df_scatter.dropna()

# 按 Moneyness 分组 (区间步长 0.05)
bins = np.arange(-0.4, 0.45, 0.05)
df_scatter['bin'] = pd.cut(df_scatter['moneyness'], bins=bins)
grouped = df_scatter.groupby('bin')[['market', 'model']].mean()

# 绘图
plt.figure(figsize=(10, 6))
# 取 bin 的中点作为 x 轴
x_axis = [i.mid for i in grouped.index]
plt.plot(x_axis, grouped['model'], 'k:', marker='None', label='ZL模型')
plt.plot(x_axis, grouped['market'], 'k.-', label='市场价格')

plt.xlabel('在值程度 ln(S/X)')
plt.ylabel('平均价格 (元)')
plt.legend()
plt.title('图2 ZL模型定价结果与在值程度的关系')
plt.grid(True, linestyle='--', alpha=0.3)
plt.savefig(os.path.join(PIPELINE_DIR, "Fig2_ZL_Moneyness.png"), dpi=300)
plt.close()

# 图3: 定价结果与剩余期限的关系
# 按剩余期限分组 (天)
# 将 maturity (年) 转换为 天
maturity_days = df_maturity * 365
s_days = maturity_days.stack()

df_scatter_mat = pd.DataFrame({'market': s_market, 'model': s_model, 'days': s_days})
df_scatter_mat = df_scatter_mat.dropna()

# 自定义分组 (仿照论文)
bins_days = [0, 30, 50, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 2000]
labels_days = [str(b) for b in bins_days[1:-1]] + ['>1600'] # 简单起见，用右端点

df_scatter_mat['bin'] = pd.cut(df_scatter_mat['days'], bins=bins_days)
grouped_mat = df_scatter_mat.groupby('bin')[['market', 'model']].mean()

plt.figure(figsize=(10, 6))
# x轴用字符串标签
x_idx = range(len(grouped_mat))
plt.plot(x_idx, grouped_mat['model'], 'k:', label='ZL模型')
plt.plot(x_idx, grouped_mat['market'], 'k.-', label='市场价格')

plt.xticks(x_idx, [str(int(i.right)) for i in grouped_mat.index], rotation=0)
plt.xlabel('剩余期限 (天)')
plt.ylabel('平均价格 (元)')
plt.legend()
plt.title('图3 ZL模型定价结果与剩余期限的关系')
plt.grid(True, linestyle='--', alpha=0.3)
plt.savefig(os.path.join(PIPELINE_DIR, "Fig3_ZL_Maturity.png"), dpi=300)
plt.close()

print("绘图完成！")
print("1. Fig1_ZL_Price_Time_Series.png")
print("2. Fig2_ZL_Moneyness.png")
print("3. Fig3_ZL_Maturity.png")

# ==========================================
# 图 4: 错误定价与评级的关系
# ==========================================
try:
    print("   正在绘制图 4: 错误定价与评级的关系...")
    df_rating = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_rating_cache.csv'), index_col=0)
    df_rating.index = pd.to_datetime(df_rating.index, errors='coerce')
    df_rating = df_rating[df_rating.index.notnull()].sort_index()
    date_col = 'cb_rating_cache.csv'
    print(f"   评级数据来源: {date_col}")

    if date_col:
        # 1. 筛选 2019 年以后的数据
        start_date = '2019-01-01'
        print(f"   正在筛选 {start_date} 以来的数据进行回测统计...")
        
        # 2. 对齐数据
        # 以模型数据的索引（交易日）为基准
        model_idx_2019 = df_diff_pct.index[df_diff_pct.index >= pd.to_datetime(start_date)]
        
        if len(model_idx_2019) == 0:
            print("   错误：模型数据中没有 2019 年以后的数据")
            # raise ValueError("No model data after 2019")
        else:
            # 仅保留在模型列中的评级列 (转债代码)
            valid_rating_cols = df_rating.columns.intersection(df_diff_pct.columns)
            
            if len(valid_rating_cols) == 0:
                print("   错误：评级数据与模型数据没有重叠的转债代码")
                # raise ValueError("No overlapping bond codes")
            else:
                # 提取评级数据子集
                df_rating_subset = df_rating[valid_rating_cols]
                # 去重索引 (防止评级数据有重复日期)
                df_rating_subset = df_rating_subset[~df_rating_subset.index.duplicated(keep='last')]
                
                # Reindex 到模型的时间轴，并向前填充 (ffill)
                # 这样每一天都有当时的评级
                df_rating_aligned = df_rating_subset.reindex(model_idx_2019, method='ffill')
                
                # 提取对应的模型错误定价数据
                df_mispricing_aligned = df_diff_pct.loc[model_idx_2019, valid_rating_cols]
                
                print(f"   统计区间：{df_rating_aligned.index.min().date()} 至 {df_rating_aligned.index.max().date()}")
                print(f"   样本交易日天数：{len(df_rating_aligned)}")
                print(f"   包含转债数量：{len(valid_rating_cols)}")
                
                # 3. 展平并合并
                # stack() 会自动过滤掉 NaN
                s_rating_flat = df_rating_aligned.stack()
                s_mispricing_flat = df_mispricing_aligned.stack()
                
                s_rating_flat.name = 'Rating'
                s_mispricing_flat.name = 'Mispricing'
                
                # 按照 (Date, Bond) 索引对齐合并
                df_plot4 = pd.concat([s_mispricing_flat, s_rating_flat], axis=1, join='inner')
                
                print(f"   总样本点数：{len(df_plot4)}")
                
                # 4. 绘图
                plt.figure(figsize=(10, 6))
                rating_order = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB']
                
                # 过滤有效评级
                df_plot4 = df_plot4[df_plot4['Rating'].isin(rating_order)]
                df_plot4['Rating'] = pd.Categorical(df_plot4['Rating'], categories=rating_order, ordered=True)
                df_plot4['Mispricing_Pct'] = df_plot4['Mispricing'] * 100
                
                # 计算每个评级的平均错误定价
                df_bar = df_plot4.groupby('Rating', observed=True)['Mispricing_Pct'].mean().reset_index()
                
                sns.barplot(x='Rating', y='Mispricing_Pct', data=df_bar, palette='coolwarm')
                plt.title('图 4 ZL 模型错误定价与评级的关系 (2019 年以来平均)')
                plt.ylabel('平均错误定价 (%)')
                plt.xlabel('信用评级')
                plt.axhline(0, color='k', linewidth=0.8)
                plt.grid(axis='y', linestyle='--', alpha=0.3)
                
                plt.savefig(os.path.join(PIPELINE_DIR, "Fig4_ZL_Rating.png"), dpi=300)
                plt.close()
                print("4. Fig4_ZL_Rating.png")
        
    else:
        print("   无法识别评级数据的日期列，跳过图 4 绘制。")

except Exception as e:
    print(f"   绘制失败：{e}")

print("绘图完成！")
print("1. Fig1_ZL_Price_Time_Series.png")
print("2. Fig2_ZL_Moneyness.png")
print("3. Fig3_ZL_Maturity.png")
print("4. Fig4_ZL_Rating.png")

print("Z-L模型计算完成！")
