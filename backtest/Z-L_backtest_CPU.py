import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak
from scipy.stats import norm
from tqdm import tqdm
import warnings
import time
import os
import zlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from numba import njit

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 忽略计算警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置与数据读取 (基于 Tushare Pipeline CSV 缓存)
# ==========================================
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))  # backtest/ 目录


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

# ==========================================
# 2. 读取转债列表与 BPS 数据
# ==========================================
print("2. 正在读取转债列表与 BPS 数据...")


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

# 2.2 从 cb_bps_cache.csv 读取 BPS
try:
    df_bps = _load_csv('cb_bps_cache.csv')
    df_bps = df_bps.reindex(df_price.index, method='ffill')
    print(f"   BPS 数据加载完成（来源：cb_bps_cache.csv）")
except Exception as e:
    print(f"   BPS 读取失败: {e}，将使用全零 BPS")
    df_bps = pd.DataFrame(0.0, index=df_price.index, columns=df_price.columns)

# 确保列与 df_price 一致 (取交集)
valid_bonds = df_price.columns.intersection(df_bps.columns)
df_price    = df_price[valid_bonds]
df_cv       = df_cv[valid_bonds]
df_floor    = df_floor[valid_bonds]
df_maturity = df_maturity[valid_bonds]
df_bps      = df_bps[valid_bonds]

print(f"   BPS 数据处理完成，有效对齐转债数量: {len(valid_bonds)}")

# 2.3 获取到期赎回价 (从 cb_basic_info.csv 读取)
# ==========================================
DEFAULT_REDEMPTION_PRICE = 110.0

try:
    _basic = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_basic_info.csv'))
    redemption_map = (
        _basic.dropna(subset=['ts_code', 'maturity_price'])
        .set_index('ts_code')['maturity_price'].to_dict()
    )
    print(f"   成功读取 {len(redemption_map)} 条赎回价数据（来源：cb_basic_info.csv）")
except Exception as e:
    print(f"   赎回价读取失败: {e}，使用默认值 {DEFAULT_REDEMPTION_PRICE}")
    redemption_map = {}


# ==========================================
# 3. Tushare 获取正股波动率
# ==========================================
print("3. 开始通过 Tushare 获取正股历史波动率...")
# Tushare token 从环境变量 TUSHARE_TOKEN 或 backtest/tushare_token.txt 读取
from token_loader import load_tushare_token

try:
    ts.set_token(load_tushare_token())
    # 初始化 Tushare 的 pro_api 接口
    pro = ts.pro_api()
except Exception as e:
    # 如果初始化失败，打印警告信息并将 pro 设为 None，后续代码会跳过网络请求
    print(f"Warning: Tushare 初始化失败: {e}")
    pro = None

# Z-L 模型缓存文件
VOL_CACHE_FILE = os.path.join(PIPELINE_DIR, "zl_stock_volatility_cache.csv")
PRICE_CACHE_FILE = os.path.join(PIPELINE_DIR, "zl_stock_price_cache.csv")


def _fetch_bond_stock_data(bond_code):
    """拉取单只转债正股的收盘价与 250 日滚动年化波动率（对齐 df_price.index），失败返回 None。"""
    stock_code_full = bond_to_stock.get(bond_code)
    if not stock_code_full or pro is None:
        return None
    start_dt = df_price.index.min().strftime("%Y%m%d")
    end_dt = df_price.index.max().strftime("%Y%m%d")
    df_k = ts.pro_bar(ts_code=stock_code_full, adj='qfq', start_date=start_dt, end_date=end_dt)
    if df_k is None or df_k.empty:
        return None
    df_k['trade_date'] = pd.to_datetime(df_k['trade_date'])
    df_k = df_k.sort_values('trade_date').set_index('trade_date')
    log_ret = np.log(df_k['close'] / df_k['close'].shift(1))
    vol = log_ret.rolling(window=250, min_periods=2).std() * np.sqrt(250)
    return df_k['close'].reindex(df_price.index), vol.reindex(df_price.index)


def _load_cache_or_empty(path):
    if os.path.exists(path):
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df.reindex(index=df_price.index, columns=df_price.columns)
    return pd.DataFrame(index=df_price.index, columns=df_price.columns)


df_volatility = _load_cache_or_empty(VOL_CACHE_FILE)
df_stock_price = _load_cache_or_empty(PRICE_CACHE_FILE)

# 增量补算: 有市场价但波动率/正股价缺失的债券（新债，或缓存生成后新增的交易日）
# 修复历史 bug: 旧逻辑缓存存在时直接用 NaN→0.40 兜底，新交易日永远拿不到真实波动率
# 注意: 旧版波动率缓存曾把 0.40 兜底值写进文件，无法与真实值区分，建议删除两个 zl_*_cache.csv 重建
pending = (df_price.notna() & (df_volatility.isna() | df_stock_price.isna())).any()
pending_bonds = pending[pending].index.tolist()
if pending_bonds:
    print(f"   {len(pending_bonds)} 只转债的正股数据存在缺口，开始增量补算 ...")
    updated_count = 0
    for bond_code in tqdm(pending_bonds, desc="Fetching Stock Data"):
        try:
            fetched = _fetch_bond_stock_data(bond_code)
            if fetched is not None:
                price_series, vol_series = fetched
                df_stock_price[bond_code] = price_series
                df_volatility[bond_code] = vol_series
                updated_count += 1
            time.sleep(0.02)
        except Exception as e:
            print(f"   获取 {bond_code} 正股数据失败: {e}")
    if updated_count:
        df_volatility.to_csv(VOL_CACHE_FILE)
        df_stock_price.to_csv(PRICE_CACHE_FILE)
        print(f"   缓存已更新 ({updated_count} 只)。")

# 兜底填充只在内存中进行，不写回缓存，下次运行仍会尝试补算
df_volatility = df_volatility.fillna(0.40)
df_stock_price = df_stock_price.ffill()

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
    print("   无可用利率数据，使用默认值 2%")
    rf_df = pd.DataFrame(0.02, index=df_price.index, columns=df_price.columns)
else:
    tenors = yield_table.columns.astype(float).values
    # 对齐到回测日期（节假日 ffill），早于缓存起点的日期用 0.02 兜底
    yield_aligned = yield_table.reindex(df_price.index, method='ffill')
    print(f"   正在进行利率期限结构线性插值 (期限点: {tenors})...")
    yield_vals = yield_aligned.values.astype(float)
    maturity_vals = df_maturity.fillna(0).values.astype(float)
    rf_matrix = np.full_like(maturity_vals, np.nan, dtype=float)
    for i in range(len(rf_matrix)):
        if np.isnan(yield_vals[i]).all():
            continue
        rf_matrix[i, :] = np.interp(maturity_vals[i, :], tenors, yield_vals[i, :])
    rf_df = pd.DataFrame(rf_matrix, index=df_price.index, columns=df_price.columns).fillna(0.02)
    print("   无风险利率期限结构匹配完成。")

# ==========================================
# 5. Z-L 模型 (Monte Carlo)
# ==========================================
print("5. 启动 Z-L 模型蒙特卡洛模拟...")

# 根据郑振龙和林海(2003)的方法确定信用利差
# 将1个月、1年、3年和5年的信用风险溢价划分别设为 0.65%、0.72%、0.90% 和 0.98%
# 然后根据转债的剩余期限进行插值作为无风险利率加信用利差


def get_credit_spread_by_maturity(maturity_years):
    """
    根据剩余期限(年)插值确定信用利差
    参考郑振龙和林海(2003)的方法:
    - 1个月 (1/12年): 0.65%
    - 1年: 0.72%
    - 3年: 0.90%
    - 5年: 0.98%
    """
    # 定义插值点 (年, 信用利差)
    # 1个月、1年、3年、5年
    maturities = np.array([1/12, 1, 3, 5])
    spreads = np.array([0.0065, 0.0072, 0.0090, 0.0098])
    
    # 使用线性插值
    # 对于低于1个月的，使用1个月的利差
    # 对于超过5年的，使用5年的利差
    result = np.interp(maturity_years, maturities, spreads)
    
    return result


def get_credit_spread_matrix(df_maturity):
    """
    根据剩余期限矩阵计算信用利差矩阵（向量化，applymap 已被 pandas 弃用）
    """
    print("   正在根据剩余期限构建信用利差矩阵...")

    vals = df_maturity.values.astype(float)
    interp = get_credit_spread_by_maturity(vals)
    spread = np.where(np.isnan(vals) | (vals <= 0), 0.0072, interp)
    return pd.DataFrame(spread, index=df_maturity.index, columns=df_maturity.columns)

# 计算利差矩阵 (基于剩余期限插值)
df_spread = get_credit_spread_matrix(df_maturity)

@njit
def zl_mc_core(S0, X0, r, credit_spread, sigma, T, BPS,
               redemption_price, put_price, put_barrier, put_start_idx,
               N, steps, random_matrix):
    """
    Z-L 模型蒙特卡洛核心算法 (Rational Game 优化版)
    """
    dt = T / steps
    
    # 初始化路径
    S_path = np.zeros((N, steps + 1))
    S_path[:, 0] = S0
    
    X_path = np.zeros((N, steps + 1))
    X_path[:, 0] = X0
    
    # 路径状态管理
    is_alive = np.ones(N, dtype=np.bool_)     # 路径是否存活
    path_end_times = np.full(N, T)            # 结束时间 (默认到期)
    path_end_values = np.zeros(N)             # 结束价值
    put_counts = np.zeros(N, dtype=np.int32)  # 回售计数器
    
    # 模拟路径
    for t in range(1, steps + 1):
        # 1. 股价演变 (GBM) - 向量化更新所有路径股价
        drift = (r - 0.5 * sigma**2) * dt
        diffusion = sigma * np.sqrt(dt) * random_matrix[:, t-1]
        S_path[:, t] = S_path[:, t-1] * np.exp(drift + diffusion)
        
        # 继承上一时刻转股价
        X_path[:, t] = X_path[:, t-1]
        
        # 2. 逐条路径处理博弈逻辑
        for i in range(N):
            if not is_alive[i]:
                continue
                
            S_t = S_path[i, t]
            X_t = X_path[i, t]
            
            # --- (1) 判断回售条件 (Put Trigger) ---
            if S_t < put_barrier * X_t:
                put_counts[i] += 1
            else:
                put_counts[i] = 0
            
            # --- (2) 发行人博弈 (Rational Down-fix) ---
            # 压力临近 (count > 20)，尝试下修
            if put_counts[i] > 20:
                # 尝试下修到市价，但受限于 BPS (归一化后的 BPS)
                # 假设面值约束在 BPS 约束内或被忽略 (通常 SOE 关注 BPS)
                new_X = max(S_t, BPS) 
                
                # 只有当新转股价低于当前转股价时才执行
                if new_X < X_t:
                    X_path[i, t] = new_X
                    # 下修后检查压力是否缓解
                    if S_t >= put_barrier * new_X:
                        put_counts[i] = 0
            
            # 更新 X_t 以供后续判断
            X_t = X_path[i, t]

            # --- (3) 投资者博弈 (Execute Put) ---
            # 满足回售计数且在回售期内
            if put_counts[i] >= 30 and t > put_start_idx:
                path_end_values[i] = put_price
                path_end_times[i] = t * dt
                is_alive[i] = False
                continue
                
            # --- (4) 强赎检查 (Call Trigger) ---
            # 价格显著高于转股价
            if S_t > 1.3 * X_t:
                # 触发强赎，投资者转股
                path_end_values[i] = S_t * (100.0 / X_t)
                path_end_times[i] = t * dt
                is_alive[i] = False
                continue
    
    # 处理未提前结束的路径 (持有到期)
    for i in range(N):
        if is_alive[i]:
            S_T = S_path[i, -1]
            X_T = X_path[i, -1]
            conv_val_T = 100.0 / X_T * S_T
            # 到期价值 = max(赎回价, 转股价值)
            path_end_values[i] = max(redemption_price, conv_val_T)
    
    # 折现
    present_values = path_end_values * np.exp(
        -(r + credit_spread) * path_end_times
    )
    
    return np.mean(present_values)


def zl_mc_pricing(S0, X0, r, credit_spread, sigma, T, BPS,
                  redemption_price=100.0, put_price=100.0, put_barrier=0.7, put_start_year=2.0,
                  N=10000, steps=2000, seed=42):
    """
    Z-L 模型蒙特卡洛定价 (Numba JIT 优化版本)
    使用 JIT 编译加速计算，速度提升 10-100 倍

    seed: 随机种子。调用方应按 (债券, 日期) 派生独立种子——
    若所有定价共享同一组随机路径，蒙特卡洛误差会变成横截面系统性偏差。
    """
    # 计算回售开始的时间步
    put_start_time = max(0.0, T - put_start_year)
    dt = T / steps
    put_start_idx = int(put_start_time / dt)

    # 预生成随机数矩阵（独立 RNG，不污染全局随机状态）
    rng = np.random.default_rng(seed)
    random_matrix = rng.standard_normal((N, steps))
    
    # 调用 JIT 编译的核心函数
    return zl_mc_core(
        S0, X0, r, credit_spread, sigma, T, BPS,
        redemption_price, put_price, put_barrier, put_start_idx,
        N, steps, random_matrix
    )

# 结果存储
results = []

calc_dates = df_price.index # 全历史回测

print(f"即将开始计算：共 {len(calc_dates)} 个交易日，{len(df_price.columns)} 只转债")

# 创建结果 DataFrame
df_zl_model = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_zl_error = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_diff_pct = pd.DataFrame(index=df_price.index, columns=df_price.columns)

# 增量计算逻辑
SUMMARY_FILE = os.path.join(PIPELINE_DIR, "ZL_Model_Summary.xlsx")
if os.path.exists(SUMMARY_FILE):
    print(f"   发现已存在的汇总文件：{SUMMARY_FILE}")
    print("   读取历史结果并执行增量计算...")
    try:
        df_zl_model_hist = pd.read_excel(SUMMARY_FILE, sheet_name="理论价格", index_col=0, engine='openpyxl')
        df_zl_model_hist.index = pd.to_datetime(df_zl_model_hist.index, errors='coerce')
        df_zl_model_hist = df_zl_model_hist[df_zl_model_hist.index.notnull()]
        df_zl_model_hist = df_zl_model_hist.apply(pd.to_numeric, errors='coerce')
        df_zl_model_hist = df_zl_model_hist.reindex(index=df_price.index, columns=df_price.columns)
        df_zl_model.update(df_zl_model_hist)

        df_price_loaded = pd.read_excel(SUMMARY_FILE, sheet_name="市场价格", index_col=0, engine='openpyxl')
        df_price_loaded.index = pd.to_datetime(df_price_loaded.index, errors='coerce')

        df_diff_hist = pd.read_excel(SUMMARY_FILE, sheet_name="绝对偏差", index_col=0, engine='openpyxl')
        df_diff_hist.index = pd.to_datetime(df_diff_hist.index, errors='coerce')
        df_diff_hist = df_diff_hist[df_diff_hist.index.notnull()]
        df_diff_hist = df_diff_hist.apply(pd.to_numeric, errors='coerce')
        df_diff_hist = df_diff_hist.reindex(index=df_price.index, columns=df_price.columns)
        df_zl_error.update(df_diff_hist)

        df_diff_pct_hist = pd.read_excel(SUMMARY_FILE, sheet_name="相对偏差", index_col=0, engine='openpyxl')
        df_diff_pct_hist.index = pd.to_datetime(df_diff_pct_hist.index, errors='coerce')
        df_diff_pct_hist = df_diff_pct_hist[df_diff_pct_hist.index.notnull()]
        df_diff_pct_hist = df_diff_pct_hist.apply(pd.to_numeric, errors='coerce')
        df_diff_pct_hist = df_diff_pct_hist.reindex(index=df_price.index, columns=df_price.columns)
        df_diff_pct.update(df_diff_pct_hist)
    except Exception as e:
        print(f"   历史结果读取失败，改为全量计算：{e}")

pending_mask = df_price.notna() & df_zl_model.isna()
pending_dates = pending_mask.any(axis=1)
calc_dates_to_run = calc_dates[pending_dates]
calc_dates_to_run = calc_dates_to_run[calc_dates_to_run.notnull()]

print(f"增量待计算交易日：{len(calc_dates_to_run)}")

# --- 检查点 (checkpoint) 变量 ---
_dates_since_checkpoint = 0
_CHECKPOINT_EVERY = 10  # 每处理 10 个交易日保存一次中间结果

# 遍历每一天 (使用 tqdm 显示进度)
date_error_count = 0
for date in tqdm(calc_dates_to_run, desc="ZL Model Backtest"):
    # 获取当天所有转债的数据
    try:
        row_price = df_price.loc[date]
        row_cv = df_cv.loc[date]
        row_bps = df_bps.loc[date]
        row_rf = rf_df.loc[date]
        row_vol = df_volatility.loc[date]
        row_mat = df_maturity.loc[date]
        row_stock_price = df_stock_price.loc[date]
        row_spread = df_spread.loc[date]
        
        # 遍历当天有交易的转债
        for bond_code in df_price.columns:
            # 跳过已计算的
            if pd.notna(df_zl_model.loc[date, bond_code]):
                continue
                
            market_price = row_price[bond_code]
            cv = row_cv[bond_code]
            bps = row_bps[bond_code]
            r = row_rf[bond_code]
            sigma = row_vol[bond_code]
            T = row_mat[bond_code]
            s_real = row_stock_price[bond_code]
            cs = row_spread[bond_code]
            
            # 数据清洗与有效性检查
            if pd.isna(market_price) or pd.isna(cv) or pd.isna(T) or T <= 0:
                continue
            
            if pd.isna(sigma): sigma = 0.4
            if pd.isna(r): r = 0.02
            if pd.isna(bps): bps = 0 
            if pd.isna(cs): cs = 0.015 # Default spread if NaN
            
            # 归一化模型参数
            S0_sim = cv
            X0_sim = 100.0
            
            # 计算归一化的 BPS
            # BPS_norm = BPS_real * CV / S_real
            if pd.notna(s_real) and s_real > 0:
                BPS_sim = bps * cv / s_real
            else:
                BPS_sim = 0.0 # 无法计算时忽略底价
            
            # 获取赎回价
            redeem_price = 106.0 # Default fallback
            
            # 优先从 map 中获取
            if bond_code in redemption_map:
                redeem_price = redemption_map[bond_code]
            else:
                # 尝试加后缀匹配
                for suffix in ['.SH', '.SZ']:
                    code_suffix = f"{bond_code}{suffix}"
                    if code_suffix in redemption_map:
                        redeem_price = redemption_map[code_suffix]
                        break
            
            # 额外校验: 如果价格异常低 (< 100)，强制使用默认值
            if redeem_price < 100.0:
                redeem_price = DEFAULT_REDEMPTION_PRICE

            # 运行蒙特卡洛
            # 为了保证回售条款判断的准确性 (30交易日)，必须使用日频模拟
            sim_steps = max(50, int(T * 240))
            
            # 按 (债券, 日期) 派生确定性种子: 可复现，且各定价随机路径互相独立
            mc_seed = zlib.crc32(f"{bond_code}|{date}".encode()) & 0x7FFFFFFF

            model_price = zl_mc_pricing(
                S0=S0_sim, X0=X0_sim, r=r, credit_spread=cs, sigma=sigma, T=T, BPS=BPS_sim,
                redemption_price=redeem_price,
                put_price=100.0,    # 假设回售价为 100 + 利息 (这里简化为100)
                put_barrier=0.7,    # 回售触发比例
                put_start_year=2.0, # 回售期通常为最后2年
                N=10000,
                steps=sim_steps,
                seed=mc_seed
            )
            
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

# 计算偏差 (理论价 - 实际价)
df_diff = df_zl_error

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
daily_err_pct = daily_err_pct.astype(float).replace([np.inf, -np.inf], np.nan).fillna(0)
ax2.fill_between(daily_err_pct.index, daily_err_pct, 0, color='gray', alpha=0.5, label='定价错误')
ax2.set_ylabel('平均定价错误 (%)')
# 设置右轴范围，使其看起来像论文中的下方分布
ax2.set_ylim(-30, 80) 

# 合并图例
lines = [l1, l2]
labels = [l.get_label() for l in lines]
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
                plt.title(f'图 4 ZL 模型错误定价与评级的关系 (2019 年以来平均)')
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
