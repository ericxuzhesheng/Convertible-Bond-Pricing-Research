# region agent log
import json as _json_dbg
import os as _os_dbg
import sys as _sys_dbg
import time as _time_dbg


def _agent_triangle_probe():
    try:
        _log_path = _os_dbg.path.join(
            _os_dbg.path.dirname(_os_dbg.path.abspath(__file__)),
            "debug-643150.log",
        )
        _payload = {
            "sessionId": "643150",
            "timestamp": int(_time_dbg.time() * 1000),
            "location": "Z-L模型历史回测_GPU.py:_agent_triangle_probe",
            "message": "python_started",
            "data": {
                "executable": _sys_dbg.executable,
                "cwd": _os_dbg.getcwd(),
                "argv": _sys_dbg.argv[:8],
                "file": _os_dbg.path.abspath(__file__),
                "runLabel": _os_dbg.environ.get("CURSOR_DEBUG_RUN", "editor-default"),
            },
            "hypothesisId": "H1_H2_H3_H5",
            "runId": "triangle-diagnose",
        }
        with open(_log_path, "a", encoding="utf-8") as _lf:
            _lf.write(_json_dbg.dumps(_payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


_agent_triangle_probe()
# endregion

import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak
from scipy.stats import norm
from tqdm import tqdm
import warnings
import time
import os
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from numba import njit, prange, cuda, float64
from numba.cuda.random import create_xoroshiro128p_states, xoroshiro128p_normal_float64
import math

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 忽略计算警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置与数据读取
# ==========================================
# 请修改为您的实际文件路径
BASE_DIR = r"D:\Python\浙商证券固收\转债错误定价"
EXCEL_PATH = os.path.join(BASE_DIR, "【浙商固收】转债资产端特征数据库【周更新外发】.xlsx")
DATA_FILE = os.path.join(BASE_DIR, "转债错误定价数据.xlsx")
SUMMARY_FILE = os.path.join(BASE_DIR, "ZL_Model_Summary.xlsx")

# 定义 Sheet 名称
SHEET_PRICE = "可转债价格"
SHEET_CV = "转换价值"
SHEET_FLOOR = "纯债价值"
SHEET_MATURITY = "剩余期限"
SHEET_STOCK_MAP = "正股市值"
SHEET_RATING = "信用评级"

def load_data(file_path, sheet_name):
    # 读取并解析索引为日期
    # 使用 openpyxl 引擎读取 Excel
    df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=0, engine='openpyxl')
    df.index = pd.to_datetime(df.index, errors='coerce')
    df = df[df.index.notnull()]
    df = df.dropna(how='all')
    # 去重索引
    df = df[~df.index.duplicated(keep='first')]
    return df.apply(pd.to_numeric, errors='coerce')

print("1. 正在读取 Excel 静态数据 ...")
df_price = load_data(EXCEL_PATH, SHEET_PRICE)
df_cv = load_data(EXCEL_PATH, SHEET_CV)
df_floor = load_data(EXCEL_PATH, SHEET_FLOOR)
df_maturity = load_data(EXCEL_PATH, SHEET_MATURITY)

# 对齐日期索引（取交集）
common_idx = df_price.index.intersection(df_cv.index).intersection(df_floor.index)
df_price = df_price.loc[common_idx]
df_cv = df_cv.loc[common_idx]
df_floor = df_floor.loc[common_idx]
df_maturity = df_maturity.loc[common_idx]

# ==========================================
# 2. 读取转债列表与 BPS 数据
# ==========================================
print("2. 正在读取转债列表与 BPS 数据...")


# 2.1 读取转债列表 (映射关系)
df_list_raw = pd.read_excel(DATA_FILE, sheet_name='转债list', header=None, engine='openpyxl')
row_labels = df_list_raw.iloc[:, 0].astype(str).str.strip()

def normalize_code(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip().upper()
    if s == "" or s == "NAN":
        return np.nan
    return s

def extract_row_values(label):
    matched_index = row_labels[row_labels == label].index
    if len(matched_index) == 0:
        return None
    row = df_list_raw.loc[matched_index[0], 1:]
    return row.map(normalize_code)

bond_codes_row = extract_row_values("转债代码")
stock_codes_row = extract_row_values("正股代码")
stock_names_row = extract_row_values("正股名称")

if bond_codes_row is None or stock_codes_row is None:
    raise ValueError("转债list 缺少“转债代码”或“正股代码”行，无法构建映射")

df_map = pd.DataFrame({"bond": bond_codes_row, "stock": stock_codes_row}).dropna()
df_map = df_map.drop_duplicates(subset=["bond"], keep="first")
bond_to_stock = dict(zip(df_map["bond"], df_map["stock"]))
stock_code_to_bond = dict(zip(df_map["stock"], df_map["bond"]))

if stock_names_row is not None:
    df_name_map = pd.DataFrame({"name": stock_names_row, "bond": bond_codes_row})
    df_name_map["name"] = df_name_map["name"].astype(str).str.strip()
    df_name_map = df_name_map[(df_name_map["name"] != "") & (df_name_map["name"] != "NAN")]
    df_name_map = df_name_map.dropna(subset=["bond"]).drop_duplicates(subset=["name"], keep="first")
    stock_name_to_bond = dict(zip(df_name_map["name"], df_name_map["bond"]))
else:
    stock_name_to_bond = {}

print(f"   已加载 {len(bond_to_stock)} 条转债映射关系")

# 2.2 读取每股净资产 (BPS)
df_bps_raw = pd.read_excel(DATA_FILE, sheet_name='每股净资产', header=4, index_col=0, engine='openpyxl')
df_bps_raw.columns = [normalize_code(c) for c in df_bps_raw.columns]
df_bps_raw = df_bps_raw.loc[:, [c for c in df_bps_raw.columns if pd.notna(c)]]
df_bps_raw = df_bps_raw.apply(pd.to_numeric, errors='coerce')

df_bps_raw.index = pd.to_datetime(df_bps_raw.index, errors='coerce')
df_bps_raw = df_bps_raw[~df_bps_raw.index.duplicated(keep='first')]
# 必须对索引排序才能使用 method='ffill' 进行重索引
df_bps_raw = df_bps_raw.sort_index()

mapped_cols = {}
for col in df_bps_raw.columns:
    if col in stock_code_to_bond:
        mapped_cols[col] = stock_code_to_bond[col]
    elif col in stock_name_to_bond:
        mapped_cols[col] = stock_name_to_bond[col]

if not mapped_cols:
    raise ValueError("每股净资产列无法映射到转债代码，请检查转债list与每股净资产格式")

df_bps = df_bps_raw[list(mapped_cols.keys())].rename(columns=mapped_cols)

# 对齐到 df_price 的索引 (Forward Fill)
# BPS 是季频数据，需要填充到周频
# 1. 先在季度层面 ffill，防止中间某个季度数据缺失（沿用上季度）
df_bps = df_bps.ffill()

# 确保索引单调递增 (reindex with method='ffill' requires monotonic index)
df_bps = df_bps.sort_index()

# 2. 使用 method='ffill' 进行重索引，确保：
#    a. 季度末非交易日的数据能正确填充到后续交易日
#    b. 交易日之间的数据沿用最近的季度末数据
df_bps = df_bps.reindex(df_price.index, method='ffill')

# 确保列与 df_price 一致 (取交集)
valid_bonds = df_price.columns.intersection(df_bps.columns)
df_price = df_price[valid_bonds]
df_cv = df_cv[valid_bonds]
df_floor = df_floor[valid_bonds]
df_maturity = df_maturity[valid_bonds]
df_bps = df_bps[valid_bonds]

print(f"   BPS 数据处理完成，有效对齐转债数量: {len(valid_bonds)}")

# 2.3 获取到期赎回价 (从 Excel 读取)
# ==========================================
DEFAULT_REDEMPTION_PRICE = 110.0

def get_redemption_prices_from_excel(file_path):
    """
    从 Excel 的 '到期赎回价' Sheet 读取赎回价
    """
    print("   正在从 Excel 读取到期赎回价...")
    try:
        # 根据截图:
        # 第一行 (Header=0) 是中文列名: 代码, 名称, 到期赎回价
        # 第二行是英文列名: Code, Name, callprice (需要剔除)
        # 数据从第三行开始
        
        # 读取 Header=0 (中文)
        df = pd.read_excel(file_path, sheet_name='到期赎回价', header=0, engine='openpyxl')
        
        # 剔除第二行 (英文名)
        df = df.iloc[1:]
        
        # 清洗列名 (去除空格)
        df.columns = [c.strip() for c in df.columns]
        
        # 提取需要的列: '代码' 和 '到期赎回价'
        # 截图显示列名可能是 "代码 " 或 "到期赎回价 " (带空格?) 
        # 我们尝试模糊匹配
        code_col = None
        price_col = None
        
        for col in df.columns:
            if "代码" in col: code_col = col
            if "到期赎回价" in col: price_col = col
            
        if not code_col or not price_col:
            print(f"   未找到对应的列名，当前列: {df.columns}")
            return {}
            
        # 清洗数据
        # 代码列: 118065.SH -> 118065 (如果需要去后缀)
        # 但我们之前保留了后缀，所以直接用
        
        # 价格列: 转为 numeric
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        
        # 转换为字典 {code: price}
        redemption_map = df.set_index(code_col)[price_col].dropna().to_dict()
        
        print(f"   成功读取 {len(redemption_map)} 条赎回价数据")
        return redemption_map
        
    except Exception as e:
        print(f"   Excel 读取失败: {e}")
        return {}

# 获取赎回价字典
redemption_map = get_redemption_prices_from_excel(DATA_FILE)

# ==========================================
# 3. Tushare 获取正股波动率
# ==========================================
print("3. 开始通过 Tushare 获取正股历史波动率...")
# 设置 Tushare 的 API Token
ts.set_token('3a3346cc26a4ca6ff9b860a2c6863e4dd31dc497afb79f047696ada9')
try:
    # 初始化 Tushare 的 pro_api 接口
    pro = ts.pro_api()
except Exception as e:
    # 如果初始化失败，打印警告信息并将 pro 设为 None，后续代码会跳过网络请求
    print(f"Warning: Tushare 初始化失败: {e}")
    pro = None

# Z-L 模型缓存文件
VOL_CACHE_FILE = "zl_stock_volatility_cache.csv"
PRICE_CACHE_FILE = "zl_stock_price_cache.csv"

if os.path.exists(VOL_CACHE_FILE) and os.path.exists(PRICE_CACHE_FILE):
    print(" 发现缓存，正在读取...")
    df_volatility = pd.read_csv(VOL_CACHE_FILE, index_col=0, parse_dates=True)
    df_volatility = df_volatility.reindex(index=df_price.index, columns=df_price.columns)
    
    df_stock_price = pd.read_csv(PRICE_CACHE_FILE, index_col=0, parse_dates=True)
    df_stock_price = df_stock_price.reindex(index=df_price.index, columns=df_price.columns)
else:
    df_volatility = pd.DataFrame(index=df_price.index, columns=df_price.columns)
    df_stock_price = pd.DataFrame(index=df_price.index, columns=df_price.columns)
    
    # 遍历每一个转债代码
    for bond_code in tqdm(df_price.columns, desc="Fetching Stock Data"):
        stock_code_full = bond_to_stock.get(bond_code)
        
        if not stock_code_full:
            continue
            
        try:
            if pro is None: break

            start_dt = df_price.index.min().strftime("%Y%m%d")
            end_dt = df_price.index.max().strftime("%Y%m%d")
            
            df_k = ts.pro_bar(ts_code=stock_code_full, adj='qfq', start_date=start_dt, end_date=end_dt)
            
            if df_k is None or df_k.empty:
                continue

            df_k['trade_date'] = pd.to_datetime(df_k['trade_date'])
            df_k = df_k.sort_values('trade_date').set_index('trade_date')
            
            # 保存价格
            price_series = df_k['close'].reindex(df_price.index)
            df_stock_price[bond_code] = price_series
            
            # 计算波动率
            df_k['log_ret'] = np.log(df_k['close'] / df_k['close'].shift(1))
            df_k['volatility'] = df_k['log_ret'].rolling(window=250, min_periods=2).std() * np.sqrt(250)
            
            vol_series = df_k['volatility'].reindex(df_price.index)
            df_volatility[bond_code] = vol_series
            
            time.sleep(0.02) 
            
        except Exception as e:
            print(f"   获取 {stock_code_full} 失败: {e}")

    df_volatility = df_volatility.fillna(0.40)
    df_stock_price = df_stock_price.ffill() # 简单填充
    
    # 保存缓存
    df_volatility.to_csv(VOL_CACHE_FILE)
    df_stock_price.to_csv(PRICE_CACHE_FILE)

df_volatility = df_volatility.reindex(index=df_price.index, columns=df_price.columns).fillna(0.40)
df_stock_price = df_stock_price.reindex(index=df_price.index, columns=df_price.columns).ffill()

print("   股票数据准备完成。")

# ==========================================
# 4. 获取无风险利率 (Akshare)
# ==========================================
print("4. 获取国债收益率...")
RF_CACHE_FILE = "rf_yield_cache.csv"

if os.path.exists(RF_CACHE_FILE):
    print("   发现利率缓存，正在读取...")
    rf_df = pd.read_csv(RF_CACHE_FILE, index_col=0, parse_dates=True)
    rf_df = rf_df.reindex(index=df_price.index, columns=df_price.columns).fillna(0.02)
else:
    try:
        df_yield = ak.bond_china_yield(start_date="20200101", end_date="20261231")
        target_curve = df_yield[df_yield['曲线名称']=='中债国债收益率曲线']
        target_curve['日期'] = pd.to_datetime(target_curve['日期'])
        target_curve.set_index('日期', inplace=True)
        
        tenor_cols = ['1年', '2年', '3年', '5年', '7年', '10年']
        available_cols = [c for c in tenor_cols if c in target_curve.columns]
        yield_table = target_curve[available_cols] / 100.0
        yield_table = yield_table.reindex(df_price.index).ffill().fillna(0.02)
        
        rf_df = pd.DataFrame(index=df_price.index, columns=df_price.columns)
        
        def broadcast_series(series, shape_df):
            return pd.DataFrame(np.tile(series.values[:, None], shape_df.shape[1]), 
                                index=shape_df.index, columns=shape_df.columns)
        
        # 使用线性插值计算无风险利率
        tenor_map = {'1年': 1, '2年': 2, '3年': 3, '5年': 5, '7年': 7, '10年': 10}
        # 确保按期限排序
        valid_tenors = sorted([t for t in tenor_map.items() if t[0] in yield_table.columns], key=lambda x: x[1])
        
        if valid_tenors:
            xp = [t[1] for t in valid_tenors]
            cols = [t[0] for t in valid_tenors]
            
            # 获取数值数组以提高效率
            yield_vals = yield_table[cols].values
            maturity_vals = df_maturity.values
            rf_vals = np.zeros_like(maturity_vals)
            
            # 按日遍历进行插值
            for i in range(len(yield_vals)):
                # np.interp 对于超出范围的值会使用端点值（Flat Extrapolation），这符合预期
                rf_vals[i, :] = np.interp(maturity_vals[i, :], xp, yield_vals[i, :])
            
            rf_df[:] = rf_vals
            
        rf_df = rf_df.fillna(0.02)
        
        # 保存缓存
        rf_df.to_csv(RF_CACHE_FILE)
        print("   利率数据已缓存。")

    except Exception as e:
        print(f"   获取利率失败，使用默认值 2%: {e}")
        rf_df = pd.DataFrame(0.02, index=df_price.index, columns=df_price.columns)

rf_df = rf_df.reindex(index=df_price.index, columns=df_price.columns).fillna(0.02)

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
    根据剩余期限矩阵计算信用利差矩阵
    """
    print("   正在根据剩余期限构建信用利差矩阵...")
    
    # 对每个元素应用插值函数
    df_spread = df_maturity.applymap(
        lambda x: get_credit_spread_by_maturity(x)
        if pd.notna(x) and x > 0 else 0.0072
    )
    
    return df_spread

# 计算利差矩阵 (基于剩余期限插值)
df_spread = get_credit_spread_matrix(df_maturity)


from numba import njit, prange

@cuda.jit(device=True)
def norm_cdf(x):
    """标准正态分布累积分布函数"""
    return 0.5 * (1 + math.erf(x / 1.41421356))

@cuda.jit(device=True)
def bs_call_price(S, K, r, T, sigma):
    """Black-Scholes 看涨期权定价公式"""
    if T <= 1e-5:
        return max(S - K, 0.0)
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)

@cuda.jit(device=True)
def find_optimal_X(S, r, sigma, T, target_val, BPS, X_curr):
    """
    寻找最优转股价 X，使得转债价值（简化为纯债+看涨期权）接近 target_val
    约束：BPS <= X <= X_curr
    使用二分法求解
    """
    # 简化的转债价值 = 纯债价值 (假设100) + 期权价值
    # 我们假设纯债价值约为 100 * exp(-rT)，这里为了简化直接用 BS call value 逼近 premium
    # 目标: BondFloor + BS_Call(S, X) = target_val
    # 即 BS_Call(S, X) = target_val - BondFloor
    
    # 估算 Bond Floor (使用无风险利率，忽略信用利差以简化计算，或者假设信用利差很小)
    # 在博弈时刻，通常 T 较短或公司信用尚可。这里使用简化处理。
    bond_floor = 100.0 * math.exp(-r * T)
    target_option_val = max(0.0, target_val - bond_floor)
    
    # 二分法范围
    low = BPS
    high = X_curr
    
    # 如果当前股价对应的期权价值已经很低（即便是 X=BPS 也很低），则直接返回 BPS
    # 或者如果 X=X_curr 时价值已经很高，则返回 X_curr
    
    for i in range(10): # 10次迭代足够精度
        mid = 0.5 * (low + high)
        call_val = bs_call_price(S, mid, r, T, sigma)
        
        if call_val > target_option_val:
            # 期权价值太高 -> 转股价太低 -> 提高转股价
            low = mid
        else:
            # 期权价值太低 -> 转股价太高 -> 降低转股价
            high = mid
            
    return high # 返回略保守的估计

@njit(parallel=True)
def zl_mc_core(S0, X0, r, credit_spread, sigma, T, BPS,
               redemption_price, put_price, put_barrier, put_start_idx,
               N, steps):
    """
    Z-L 模型蒙特卡洛核心算法 
    1. Path-based Parallelization: 路径并行
    2. On-the-fly Random Generation: 移除预生成随机数矩阵，大幅减少内存带宽消耗
    """
    dt = T / steps
    
    # 预计算常数
    drift = (r - 0.5 * sigma**2) * dt
    vol_sqrt_dt = sigma * np.sqrt(dt)
    
    # 用于累加所有路径的现值 (Parallel reduction)
    sum_pv = 0.0
    
    # 外层循环并行化：每个线程独立处理一条路径
    for i in prange(N):
        # 路径状态初始化
        S_curr = S0
        X_curr = X0
        put_count = 0
        
        path_end_time = T
        path_end_val = 0.0
        is_active = True
        
        # 时间步循环
        for t in range(1, steps + 1):
            # 1. 股价演变 (GBM)
            # 核心优化：直接在寄存器/L1缓存中生成随机数，无需内存存取
            z = np.random.standard_normal()
            S_curr = S_curr * np.exp(drift + vol_sqrt_dt * z)
            
            # 2. 条款检查与博弈
            
            # --- 判断回售触发条件 ---
            if S_curr < put_barrier * X_curr:
                put_count += 1
            else:
                put_count = 0
            
            # --- 发行人博弈 (Rational Down-fix) ---
            if put_count > 20:
                new_X = max(S_curr, BPS)
                # new_X = max(new_X, 100.0) # 错误：转股价不应受限于面值100，而是受限于BPS
                
                # 只有当新转股价低于当前转股价时才执行 (只能下修)
                if new_X < X_curr:
                    X_curr = new_X
                    if S_curr >= put_barrier * X_curr:
                        put_count = 0
            
            # --- 投资者博弈 (Execute Put) ---
            if put_count >= 30 and t >= put_start_idx:
                path_end_time = t * dt
                path_end_val = put_price
                is_active = False
                break 
            
            # --- 强赎检查 (Call) ---
            if S_curr > 1.3 * X_curr:
                path_end_time = t * dt
                path_end_val = S_curr * (100.0 / X_curr)
                is_active = False
                break 
        
        # 3. 到期结算
        if is_active:
            conv_val = 100.0 / X_curr * S_curr
            path_end_val = np.maximum(redemption_price, conv_val)
            
        # 4. 折现并累加
        pv = path_end_val * np.exp(-(r + credit_spread) * path_end_time)
        sum_pv += pv
    
    return sum_pv / N

@cuda.jit
def zl_mc_kernel_gpu(S0, X0, r, credit_spread, sigma, T, BPS,
                     redemption_price, put_price, put_barrier, put_start_idx,
                     dt, rng_states, out_pv):
    """
    Z-L 模型蒙特卡洛 GPU 内核函数
    每个线程计算一条路径
    """
    # 获取当前线程的绝对索引
    tid = cuda.grid(1)
    
    # 边界检查
    if tid < out_pv.shape[0]:
        # 预计算常数
        drift = (r - 0.5 * sigma**2) * dt
        vol_sqrt_dt = sigma * math.sqrt(dt)
        
        # 路径状态初始化
        S_curr = S0
        X_curr = X0
        put_count = 0
        
        path_end_time = T
        path_end_val = 0.0
        is_active = True
        
        # 获取 Steps (根据 dt 和 T 推算，或作为参数传入，这里通过 dt 隐式计算)
        # 注意：为了性能，这里假设 steps 是外部控制好的，但 kernel 里需要知道循环次数
        # 最好是直接传入 steps。这里我们重新计算一下 steps
        steps = int(round(T / dt))
        
        # 时间步循环
        for t in range(1, steps + 1):
            # 1. 股价演变 (GBM)
            # 使用 CUDA 随机数生成器
            z = xoroshiro128p_normal_float64(rng_states, tid)
            S_curr = S_curr * math.exp(drift + vol_sqrt_dt * z)
            
            # 2. 条款检查与博弈
            
            # --- 判断回售触发条件 ---
            if S_curr < put_barrier * X_curr:
                put_count += 1
            else:
                put_count = 0
            
            # --- 发行人博弈 (Rational Down-fix) ---
            if put_count > 20:
                # A点优化：基于 BS 公式寻找最优转股价，使转债价值略高于 100 (e.g. 100.5)
                # T_remain = T - t * dt
                T_remain = max(1e-4, T - t * dt)
                target_value = 100.5 
                
                # 寻找新的 X
                optimal_X = find_optimal_X(S_curr, r, sigma, T_remain, target_value, BPS, X_curr)
                
                new_X = max(optimal_X, BPS) # 再次确保不低于 BPS
                
                # 只有当新转股价低于当前转股价时才执行 (只能下修)
                if new_X < X_curr:
                    X_curr = new_X
                    if S_curr >= put_barrier * X_curr:
                        put_count = 0
            
            # --- 投资者博弈 (Execute Put) ---
            if put_count >= 30 and t >= put_start_idx:
                path_end_time = t * dt
                path_end_val = put_price
                is_active = False
                break 
            
            # --- 强赎检查 (Call) ---
            if S_curr > 1.3 * X_curr:
                path_end_time = t * dt
                # B点优化：回报取 max(转股价值, 赎回价)
                path_end_val = max(S_curr * (100.0 / X_curr), redemption_price)
                is_active = False
                break 
        
        # 3. 到期结算
        if is_active:
            conv_val = 100.0 / X_curr * S_curr
            path_end_val = max(redemption_price, conv_val)
            
        # 4. 折现并写入输出数组
        pv = path_end_val * math.exp(-(r + credit_spread) * path_end_time)
        out_pv[tid] = pv

@cuda.jit
def zl_mc_kernel_gpu_batch(S0_arr, X0_arr, r_arr, credit_spread_arr, sigma_arr, T_arr, BPS_arr,
                           redemption_price_arr, put_price_arr, put_barrier_arr, put_start_idx_arr,
                           dt_arr, rng_states, out_pv, N):
    """
    Z-L 模型蒙特卡洛 GPU 批量内核函数
    支持同时计算多只转债 (Batch Processing)
    Grid 布局: 1D Grid
    Thread ID -> (Bond ID, Path ID)
    """
    # 获取当前线程的绝对索引
    tid = cuda.grid(1)
    
    # 计算当前线程负责哪只转债的哪条路径
    # tid = bond_idx * N + path_idx
    bond_idx = tid // N
    
    # 边界检查: bond_idx 必须在有效范围内
    if bond_idx < S0_arr.shape[0]:
        # 获取该转债的参数
        S0 = S0_arr[bond_idx]
        X0 = X0_arr[bond_idx]
        r = r_arr[bond_idx]
        credit_spread = credit_spread_arr[bond_idx]
        sigma = sigma_arr[bond_idx]
        T = T_arr[bond_idx]
        BPS = BPS_arr[bond_idx]
        redemption_price = redemption_price_arr[bond_idx]
        put_price = put_price_arr[bond_idx]
        put_barrier = put_barrier_arr[bond_idx]
        put_start_idx = put_start_idx_arr[bond_idx]
        
        # C点优化：使用固定的 dt = 1/252，并在 kernel 内部计算 steps
        dt = 1.0 / 252.0 
        
        # 预计算常数
        drift = (r - 0.5 * sigma**2) * dt
        vol_sqrt_dt = sigma * math.sqrt(dt)
        
        # 路径状态初始化
        S_curr = S0
        X_curr = X0
        put_count = 0
        
        path_end_time = T
        path_end_val = 0.0
        is_active = True
        
        # C点优化：根据 T 和 dt=1/252 动态计算 steps
        steps = int(math.ceil(T * 252.0))
        
        # 时间步循环
        for t in range(1, steps + 1):
            # 1. 股价演变 (GBM)
            z = xoroshiro128p_normal_float64(rng_states, tid)
            S_curr = S_curr * math.exp(drift + vol_sqrt_dt * z)
            
            # 2. 条款检查与博弈
            if S_curr < put_barrier * X_curr:
                put_count += 1
            else:
                put_count = 0
            
            # --- 发行人博弈 (Rational Down-fix) ---
            if put_count > 20:
                # A点优化：基于 BS 公式寻找最优转股价
                T_remain = max(1e-4, T - t * dt)
                target_value = 100.5 
                
                optimal_X = find_optimal_X(S_curr, r, sigma, T_remain, target_value, BPS, X_curr)
                new_X = max(optimal_X, BPS)
                
                if new_X < X_curr:
                    X_curr = new_X
                    if S_curr >= put_barrier * X_curr:
                        put_count = 0
            
            if put_count >= 30 and t >= put_start_idx:
                path_end_time = t * dt
                path_end_val = put_price
                is_active = False
                break 
            
            if S_curr > 1.3 * X_curr:
                path_end_time = t * dt
                # B点优化：回报取 max(转股价值, 赎回价)
                path_end_val = max(S_curr * (100.0 / X_curr), redemption_price)
                is_active = False
                break 
        
        # 3. 到期结算
        if is_active:
            conv_val = 100.0 / X_curr * S_curr
            path_end_val = max(redemption_price, conv_val)
            
        # 4. 折现并写入输出数组
        pv = path_end_val * math.exp(-(r + credit_spread) * path_end_time)
        out_pv[tid] = pv

def zl_mc_pricing_batch(S0_arr, X0_arr, r_arr, credit_spread_arr, sigma_arr, T_arr, BPS_arr,
                        redemption_price_arr, put_price_arr, put_barrier_arr, put_start_idx_arr,
                        dt_arr, N=10000):
    """
    Z-L 模型批量定价函数 (GPU Only)
    """
    if not cuda.is_available():
        raise RuntimeError("GPU Batch mode requires CUDA")

    num_bonds = len(S0_arr)
    total_threads = num_bonds * N
    
    # 1. 准备输出数组 (Flattened: [Bond0_Path0...PathN, Bond1_Path0...])
    out_pv_device = cuda.device_array(total_threads, dtype=np.float64)
    
    # 2. 准备随机数状态
    rng_states = create_xoroshiro128p_states(total_threads, seed=42)
    
    # 3. 传输参数到 GPU
    # 注意：为了性能，最好在外部就把参数转为 numpy array (float64, continuous)
    # 这里假设输入已经是 numpy array
    d_S0 = cuda.to_device(S0_arr)
    d_X0 = cuda.to_device(X0_arr)
    d_r = cuda.to_device(r_arr)
    d_cs = cuda.to_device(credit_spread_arr)
    d_sigma = cuda.to_device(sigma_arr)
    d_T = cuda.to_device(T_arr)
    d_BPS = cuda.to_device(BPS_arr)
    d_redem = cuda.to_device(redemption_price_arr)
    d_put = cuda.to_device(put_price_arr)
    d_barrier = cuda.to_device(put_barrier_arr)
    d_start_idx = cuda.to_device(put_start_idx_arr)
    d_dt = cuda.to_device(dt_arr)
    
    # 4. 配置网格
    threads_per_block = 256
    blocks_per_grid = (total_threads + (threads_per_block - 1)) // threads_per_block
    
    # 5. 调用内核
    zl_mc_kernel_gpu_batch[blocks_per_grid, threads_per_block](
        d_S0, d_X0, d_r, d_cs, d_sigma, d_T, d_BPS,
        d_redem, d_put, d_barrier, d_start_idx,
        d_dt, rng_states, out_pv_device, N
    )
    
    # 6. 处理结果
    out_pv_host = out_pv_device.copy_to_host()
    
    # reshape 并求均值 -> (num_bonds, N) -> mean(axis=1) -> (num_bonds,)
    results = out_pv_host.reshape(num_bonds, N).mean(axis=1)
    return results


# 结果存储
results = []

calc_dates = df_price.index # 全历史回测

print(f"即将开始计算: 共 {len(calc_dates)} 个交易日, {len(df_price.columns)} 只转债")

# 创建结果 DataFrame
df_zl_model = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_zl_error = pd.DataFrame(index=df_price.index, columns=df_price.columns)
df_diff_pct = pd.DataFrame(index=df_price.index, columns=df_price.columns)

if os.path.exists(SUMMARY_FILE):
    print(f"   发现已存在的汇总文件: {SUMMARY_FILE}")
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
        print(f"   历史结果读取失败，改为全量计算: {e}")

pending_mask = df_price.notna() & df_zl_model.isna()
pending_dates = pending_mask.any(axis=1)
calc_dates_to_run = calc_dates[pending_dates]
calc_dates_to_run = calc_dates_to_run[calc_dates_to_run.notnull()]

print(f"增量待计算交易日: {len(calc_dates_to_run)}")
use_gpu_batch = cuda.is_available()
print(f"GPU Batch 可用: {use_gpu_batch}")

# 遍历每一天 (使用 tqdm 显示进度)
date_error_count = 0
for date in tqdm(calc_dates_to_run, desc="ZL Model Backtest"):
    try:
        row_price = df_price.loc[date]
        row_cv = df_cv.loc[date]
        row_bps = df_bps.loc[date]
        row_rf = rf_df.loc[date]
        row_vol = df_volatility.loc[date]
        row_mat = df_maturity.loc[date]
        row_stock_price = df_stock_price.loc[date]
        row_spread = df_spread.loc[date]

        batch_codes = []
        p_S0, p_X0, p_r, p_cs, p_sigma, p_T, p_BPS = [], [], [], [], [], [], []
        p_redem, p_put, p_barrier, p_start_idx, p_dt = [], [], [], [], []

        market_prices = []

        for bond_code in df_price.columns:
            if pd.notna(df_zl_model.loc[date, bond_code]):
                continue

            market_price = pd.to_numeric(row_price[bond_code], errors='coerce')
            cv = pd.to_numeric(row_cv[bond_code], errors='coerce')
            bps = pd.to_numeric(row_bps[bond_code], errors='coerce')
            r = pd.to_numeric(row_rf[bond_code], errors='coerce')
            sigma = pd.to_numeric(row_vol[bond_code], errors='coerce')
            T = pd.to_numeric(row_mat[bond_code], errors='coerce')
            s_real = pd.to_numeric(row_stock_price[bond_code], errors='coerce')
            cs = pd.to_numeric(row_spread[bond_code], errors='coerce')

            if pd.isna(market_price) or pd.isna(cv) or pd.isna(T) or T <= 0:
                continue

            if pd.isna(sigma): sigma = 0.4
            if pd.isna(r): r = 0.02
            if pd.isna(bps): bps = 0
            if pd.isna(cs): cs = 0.015

            S0_sim = cv
            X0_sim = 100.0

            if pd.notna(s_real) and s_real > 0:
                BPS_sim = bps * cv / s_real
            else:
                BPS_sim = 0.0

            redeem_price = 106.0

            if bond_code in redemption_map:
                redeem_price = redemption_map[bond_code]
            else:
                for suffix in ['.SH', '.SZ']:
                    code_suffix = f"{bond_code}{suffix}"
                    if code_suffix in redemption_map:
                        redeem_price = redemption_map[code_suffix]
                        break

            if redeem_price < 100.0:
                redeem_price = DEFAULT_REDEMPTION_PRICE

            batch_codes.append(bond_code)
            market_prices.append(market_price)

            p_S0.append(S0_sim)
            p_X0.append(X0_sim)
            p_r.append(r)
            p_cs.append(cs)
            p_sigma.append(sigma)
            p_T.append(T)
            p_BPS.append(BPS_sim)
            p_redem.append(redeem_price)
            p_put.append(100.0)
            p_barrier.append(0.7)

            put_start_year_remain = 2.0
            steps = 2000
            dt = T / steps
            p_dt.append(dt)

            if T <= put_start_year_remain:
                p_start_idx.append(0)
            else:
                time_to_start = T - put_start_year_remain
                p_start_idx.append(int(time_to_start / dt))

        if batch_codes:
            S0_arr = np.ascontiguousarray(p_S0, dtype=np.float64)
            X0_arr = np.ascontiguousarray(p_X0, dtype=np.float64)
            r_arr = np.ascontiguousarray(p_r, dtype=np.float64)
            cs_arr = np.ascontiguousarray(p_cs, dtype=np.float64)
            sigma_arr = np.ascontiguousarray(p_sigma, dtype=np.float64)
            T_arr = np.ascontiguousarray(p_T, dtype=np.float64)
            BPS_arr = np.ascontiguousarray(p_BPS, dtype=np.float64)
            redem_arr = np.ascontiguousarray(p_redem, dtype=np.float64)
            put_arr = np.ascontiguousarray(p_put, dtype=np.float64)
            barrier_arr = np.ascontiguousarray(p_barrier, dtype=np.float64)
            start_idx_arr = np.ascontiguousarray(p_start_idx, dtype=np.int32)
            dt_arr = np.ascontiguousarray(p_dt, dtype=np.float64)

            if use_gpu_batch:
                try:
                    model_prices = zl_mc_pricing_batch(
                        S0_arr, X0_arr, r_arr, cs_arr, sigma_arr, T_arr, BPS_arr,
                        redem_arr, put_arr, barrier_arr, start_idx_arr, dt_arr, N=10000
                    )

                    for i, code in enumerate(batch_codes):
                        mp = model_prices[i]
                        df_zl_model.loc[date, code] = mp
                        df_zl_error.loc[date, code] = mp - market_prices[i]
                except Exception as e:
                    print(f"Batch GPU failed on {date}, falling back to loop: {e}")
                    use_gpu_batch = False

            if not use_gpu_batch:
                for i, code in enumerate(batch_codes):
                    try:
                        steps_cpu = int(round(p_T[i] / p_dt[i]))

                        mp = zl_mc_core(
                            p_S0[i], p_X0[i], p_r[i], p_cs[i], p_sigma[i], p_T[i], p_BPS[i],
                            p_redem[i], p_put[i], p_barrier[i], p_start_idx[i],
                            10000, steps_cpu
                        )
                        df_zl_model.loc[date, code] = mp
                        df_zl_error.loc[date, code] = mp - market_prices[i]
                    except Exception:
                        continue

    except Exception as e:
        date_error_count += 1
        if date_error_count <= 5:
            print(f"Date-level error on {date}: {e}")

print(f"日期级异常数量: {date_error_count}")

# 计算偏差 (理论价 - 实际价)
df_diff = df_zl_error

safe_price = df_price.replace(0, np.nan)
df_diff_pct = df_diff / safe_price

df_zl_model.to_csv(os.path.join(BASE_DIR, "ZL_Model_Prices.csv"))
df_price.to_csv(os.path.join(BASE_DIR, "Market_Prices.csv"))
df_diff.to_csv(os.path.join(BASE_DIR, "ZL_Model_Deviation_Abs.csv"))
df_diff_pct.to_csv(os.path.join(BASE_DIR, "ZL_Model_Deviation_Pct.csv"))

print("计算完成！")
print("结果已保存:")
print("1. 理论价格: 'ZL_Model_Prices.csv'")
print("2. 市场价格: 'ZL_Market_Prices.csv'")
print("3. 绝对偏差 (Model - Market): 'ZL_Model_Deviation_Abs.csv'")
print("4. 相对偏差 (Model - Market)/Market: 'ZL_Model_Deviation_Pct.csv'")

with pd.ExcelWriter(SUMMARY_FILE) as writer:
    df_zl_model.to_excel(writer, sheet_name="理论价格")
    df_price.to_excel(writer, sheet_name="市场价格")
    df_diff.to_excel(writer, sheet_name="绝对偏差")
    df_diff_pct.to_excel(writer, sheet_name="相对偏差")
print(f"5. 汇总 Excel: '{SUMMARY_FILE}'")

# 简单的误差展示
# 1. 平均误差 (Mean Error / Bias)
mean_error = df_diff.mean().mean()
# 2. 平均绝对误差 (MAE)
mae = df_diff.abs().mean().mean()
# 3. 均方根误差 (RMSE)
rmse = np.sqrt((df_diff**2).mean().mean())
# 4. 平均相对误差 (MAPE) - 注意 df_diff_pct 是带符号的相对误差，这里取绝对值
mape = df_diff_pct.abs().mean().mean() * 100
# 5. 对称平均绝对百分比误差 (SMAPE)
# SMAPE = mean( |model - market| / ((|model| + |market|) / 2) ) * 100

# 定义 calc_price (用于后续计算和对齐)
if 'df_price_loaded' in locals():
    # 尝试使用文件中的市场价格
    calc_price = df_price_loaded.copy()
else:
    calc_price = df_price.copy()

# 数据清洗函数
def clean_dataframe(df):
    # 1. 转换索引为日期，强制 Coerce
    df.index = pd.to_datetime(df.index, errors='coerce')
    # 2. 去除无效索引 (NaT)
    df = df[df.index.notnull()]
    # 3. 强制转换数据为数值
    df = df.apply(pd.to_numeric, errors='coerce')
    return df

df_zl_model_clean = clean_dataframe(df_zl_model.copy())
calc_price_clean = clean_dataframe(calc_price.copy())

# 确保列名一致
df_zl_model_clean.columns = df_zl_model_clean.columns.astype(str)
calc_price_clean.columns = calc_price_clean.columns.astype(str)

# 对齐数据
model_aligned, market_aligned = df_zl_model_clean.align(calc_price_clean, join='inner', axis=None)

# 计算 SMAPE
numerator = (model_aligned - market_aligned).abs()
denominator = (model_aligned.abs() + market_aligned.abs()) / 2.0

# 避免分母为 0
smape_matrix = numerator / denominator.replace(0, np.nan)

# 使用 stack() 展平并自动忽略 NaN，然后求均值
smape = smape_matrix.stack().mean() * 100

if np.isnan(smape):
    print("Warning: SMAPE calculation resulted in NaN (possibly empty overlap or invalid data).")
    # 尝试 fallback: 使用 df_diff 和 calc_price 反推 model
    # df_diff = model - market => model = market + df_diff
    try:
        print("   Attempting fallback calculation using df_diff and calc_price...")
        # 清洗 df_diff
        df_diff_clean = clean_dataframe(df_diff.copy())
        
        # 对齐 df_diff 和 calc_price
        diff_aligned, market_aligned_fb = df_diff_clean.align(calc_price_clean, join='inner', axis=None)
        
        # 反推 model
        model_recovered = market_aligned_fb + diff_aligned
        
        # 计算 SMAPE
        numerator_fb = diff_aligned.abs() # |model - market| = |df_diff|
        denominator_fb = (model_recovered.abs() + market_aligned_fb.abs()) / 2.0
        
        smape_matrix_fb = numerator_fb / denominator_fb.replace(0, np.nan)
        smape = smape_matrix_fb.stack().mean() * 100
        print(f"   Fallback SMAPE: {smape:.4f} %")
        
        # 修复 df_zl_model 用于后续绘图 (如果原始数据无效)
        print("   Repairing df_zl_model for plotting using recovered data...")
        # 重构 df_zl_model = market + diff
        # 注意: 这里使用 cleaned versions 进行计算，以确保数值有效
        df_zl_model = calc_price_clean.add(df_diff_clean, fill_value=0)
        
    except Exception as e:
        print(f"   Fallback failed: {e}")
        pass

print("-" * 30)
print("模型整体误差指标:")
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

# 对齐数据用于绘图 (防止因数据清洗导致的长度不一致)
common_plot_idx = daily_market_avg.index.intersection(daily_model_avg.index).intersection(daily_err_pct.index)
daily_market_avg = daily_market_avg.loc[common_plot_idx]
daily_model_avg = daily_model_avg.loc[common_plot_idx]
daily_err_pct = daily_err_pct.loc[common_plot_idx]

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
plt.savefig(os.path.join(BASE_DIR, "Fig1_ZL_Price_Time_Series.png"), dpi=300)
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
plt.savefig(os.path.join(BASE_DIR, "Fig2_ZL_Moneyness.png"), dpi=300)
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
plt.savefig(os.path.join(BASE_DIR, "Fig3_ZL_Maturity.png"), dpi=300)
plt.close()

# ==========================================
# 图4: 错误定价与评级的关系
# ==========================================
try:
    print("   正在绘制图4: 错误定价与评级的关系...")
    # 读取评级数据预览以确定Header
    df_rating_preview = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_RATING, header=None, nrows=10, engine='openpyxl')
    
    header_idx = 0
    for idx, row in df_rating_preview.iterrows():
        matches = row.astype(str).str.contains(r'\d{6}\.(SH|SZ)').sum()
        if matches > 5: 
            header_idx = idx
            break
            
    df_rating = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_RATING, header=header_idx, engine='openpyxl')
    
    # 识别日期列
    date_col = None
    for col in df_rating.columns[:5]:
        sample = df_rating[col].dropna().iloc[:10]
        if len(sample) == 0: continue
        try:
            # 尝试转换
            dates = pd.to_datetime(sample, errors='coerce')
            
            # 检查有效日期比例
            if dates.notnull().mean() < 0.5:
                continue
                
            # 检查日期范围是否合理 (例如: 2000年以后)
            # 排除被误判为日期的数字 (如 0 -> 1970-01-01)
            valid_dates = dates[dates.notnull()]
            if valid_dates.min().year < 2000:
                continue
                
            date_col = col
            break
        except:
            continue
            
    if date_col is None and len(df_rating.columns) > 2:
        # 尝试检查第3列 (索引2)，即使前面的逻辑没过
        col_candidate = df_rating.columns[2]
        sample = df_rating[col_candidate].dropna().iloc[:10]
        try:
             dates = pd.to_datetime(sample, errors='coerce')
             if dates.notnull().any() and dates.max().year >= 2000:
                 date_col = col_candidate
        except:
             pass
            
    if date_col:
        print(f"   使用评级日期列: {date_col}")
        df_rating[date_col] = pd.to_datetime(df_rating[date_col], errors='coerce')
        df_rating = df_rating.dropna(subset=[date_col])
        df_rating = df_rating.set_index(date_col)
        df_rating = df_rating.sort_index()
        
        # 1. 筛选 2019 年以后的数据
        start_date = '2019-01-01'
        print(f"   正在筛选 {start_date} 以来的数据进行回测统计...")
        
        # 2. 对齐数据
        # 以模型数据的索引（交易日）为基准
        model_idx_2019 = df_diff_pct.index[df_diff_pct.index >= pd.to_datetime(start_date)]
        
        if len(model_idx_2019) == 0:
            print("   错误: 模型数据中没有 2019 年以后的数据")
            # raise ValueError("No model data after 2019")
        else:
            # 仅保留在模型列中的评级列 (转债代码)
            valid_rating_cols = df_rating.columns.intersection(df_diff_pct.columns)
            
            if len(valid_rating_cols) == 0:
                print("   错误: 评级数据与模型数据没有重叠的转债代码")
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
                
                print(f"   统计区间: {df_rating_aligned.index.min().date()} 至 {df_rating_aligned.index.max().date()}")
                print(f"   样本交易日天数: {len(df_rating_aligned)}")
                print(f"   包含转债数量: {len(valid_rating_cols)}")
                
                # 3. 展平并合并
                # stack() 会自动过滤掉 NaN
                s_rating_flat = df_rating_aligned.stack()
                s_mispricing_flat = df_mispricing_aligned.stack()
                
                s_rating_flat.name = 'Rating'
                s_mispricing_flat.name = 'Mispricing'
                
                # 按照 (Date, Bond) 索引对齐合并
                df_plot4 = pd.concat([s_mispricing_flat, s_rating_flat], axis=1, join='inner')
                
                print(f"   总样本点数: {len(df_plot4)}")
                
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
                plt.title(f'图4 ZL模型错误定价与评级的关系 (2019年以来平均)')
                plt.ylabel('平均错误定价 (%)')
                plt.xlabel('信用评级')
                plt.axhline(0, color='k', linewidth=0.8)
                plt.grid(axis='y', linestyle='--', alpha=0.3)
                
                plt.savefig(os.path.join(BASE_DIR, "Fig4_ZL_Rating.png"), dpi=300)
                plt.close()
                print("4. Fig4_ZL_Rating.png")
        
    else:
        print("   无法识别评级数据的日期列，跳过图4绘制。")

except Exception as e:
    print(f"   绘制失败: {e}")

print("绘图完成！")
print("1. Fig1_ZL_Price_Time_Series.png")
print("2. Fig2_ZL_Moneyness.png")
print("3. Fig3_ZL_Maturity.png")
print("4. Fig4_ZL_Rating.png")

print("Z-L 模型计算完成！")
