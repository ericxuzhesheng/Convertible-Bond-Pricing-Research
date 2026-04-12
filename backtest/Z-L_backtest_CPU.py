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
from numba import njit

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
# 转债 list Sheet 结构：
# Row 0: 正股代码
# Row 1: 转债代码
# Row 2: 转债名称
df_list_raw = pd.read_excel(DATA_FILE, sheet_name=0, header=None, engine='openpyxl')

def normalize_code(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip().upper()
    if s == "" or s == "NAN":
        return np.nan
    return s

# 提取正股代码行和转债代码行
stock_codes_row = df_list_raw.iloc[0, 1:].map(normalize_code)
bond_codes_row = df_list_raw.iloc[1, 1:].map(normalize_code)

# 构建映射：转债代码 -> 正股代码
df_map = pd.DataFrame({"bond": bond_codes_row, "stock": stock_codes_row}).dropna()
df_map = df_map.drop_duplicates(subset=["bond"], keep="first")
bond_to_stock = dict(zip(df_map["bond"], df_map["stock"]))
stock_code_to_bond = dict(zip(df_map["stock"], df_map["bond"]))

# 尝试读取转债名称行（如果有）
stock_name_to_bond = {}
if len(df_list_raw) > 2:
    stock_names_row = df_list_raw.iloc[2, 1:]
    stock_names_row = stock_names_row.astype(str).str.strip()
    stock_names_row = stock_names_row[(stock_names_row != "") & (stock_names_row != "NAN")]
    
    df_name_map = pd.DataFrame({"name": stock_names_row, "bond": bond_codes_row.iloc[:len(stock_names_row)]})
    df_name_map = df_name_map.dropna(subset=["bond"]).drop_duplicates(subset=["name"], keep="first")
    stock_name_to_bond = dict(zip(df_name_map["name"], df_name_map["bond"]))

print(f"   已加载 {len(bond_to_stock)} 条转债映射关系")

# 2.2 读取每股净资产 (BPS)
# BPS Sheet 结构：
# Row 0: 开始日期
# Row 1: 截止日期
# Row 2: 每股净资产 BPS
# Row 3: 日期（中文）
# Row 4: Date（英文，代码行）- 作为 header
# 数据从第 5 行开始
df_bps_raw = pd.read_excel(DATA_FILE, sheet_name='每股净资产', header=4, index_col=0, engine='openpyxl')

# 第一行是代码，需要剔除
df_bps_raw = df_bps_raw.iloc[1:]

df_bps_raw.index = pd.to_datetime(df_bps_raw.index, errors='coerce')
df_bps_raw = df_bps_raw[~df_bps_raw.index.duplicated(keep='first')]
# 必须对索引排序才能使用 method='ffill' 进行重索引
df_bps_raw = df_bps_raw.sort_index()

# 重命名列：正股代码 -> 转债代码
# BPS 列名是正股代码格式：002241.SZ
# 使用 stock_code_to_bond 映射
mapped_cols = {}
for col in df_bps_raw.columns:
    if col in stock_code_to_bond:
        bond_code = stock_code_to_bond[col]
        mapped_cols[col] = bond_code

if not mapped_cols:
    print("   警告：BPS 列名无法映射到转债代码，请检查转债 list 与每股净资产格式")
else:
    print(f"   成功映射 {len(mapped_cols)} 个 BPS 列")

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
    df_volatility = df_volatility.reindex(df_price.index)
    
    df_stock_price = pd.read_csv(PRICE_CACHE_FILE, index_col=0, parse_dates=True)
    df_stock_price = df_stock_price.reindex(df_price.index)
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

print("   股票数据准备完成。")

# ==========================================
# 4. 获取无风险利率 (Akshare)
# ==========================================
print("4. 获取国债收益率...")
RF_CACHE_FILE = "rf_yield_cache.csv"

if os.path.exists(RF_CACHE_FILE):
    print("   发现利率缓存，正在读取...")
    rf_df = pd.read_csv(RF_CACHE_FILE, index_col=0, parse_dates=True)
    rf_df = rf_df.reindex(df_price.index).fillna(0.02)
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
        
        # 线性插值替代分段赋值
        tenor_map = {'1年': 1.0, '2年': 2.0, '3年': 3.0, '5年': 5.0, '7年': 7.0, '10年': 10.0}
        available_tenors_str = [c for c in tenor_cols if c in yield_table.columns]
        available_tenors_val = np.array([tenor_map[c] for c in available_tenors_str])

        print(f"   正在进行利率期限结构线性插值 (期限点: {available_tenors_val})...")

        # 逐日插值
        for date in tqdm(df_maturity.index, desc="Interpolating Yield Curve"):
            if date not in yield_table.index:
                continue

            # 获取当天的收益率曲线 Y 值
            daily_yields = yield_table.loc[date, available_tenors_str].values.astype(float)

            # 获取当天的剩余期限 X 值 (处理 NaN 为 0，避免插值错误)
            daily_maturities = df_maturity.loc[date].fillna(0).values.astype(float)

            # 执行线性插值
            # np.interp 对于超出 [min, max] 范围的 x，默认取端点值 (left, right)
            interp_rates = np.interp(daily_maturities, available_tenors_val, daily_yields)

            rf_df.loc[date] = interp_rates
            
        rf_df = rf_df.fillna(0.02)
        
        # 保存缓存
        rf_df.to_csv(RF_CACHE_FILE)
        print("   利率数据已缓存。")

    except Exception as e:
        print(f"   获取利率失败，使用默认值 2%: {e}")
        rf_df = pd.DataFrame(0.02, index=df_price.index, columns=df_price.columns)

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

# ==========================================
# 5.1 获取到期赎回价 (从 Excel 读取)
# ==========================================
DEFAULT_REDEMPTION_PRICE = 110.0

def get_redemption_prices_from_excel(file_path):
    """
    从 Excel 的 '到期赎回价' Sheet 读取赎回价
    """
    print("   正在从 Excel 读取到期赎回价...")
    try:
        # 根据截图:
        # 第一行 (Header=0) 是中文列名：代码，名称，到期赎回价
        # 第二行是英文列名：Code, Name, callprice (需要剔除)
        # 数据从第三行开始
        
        # 读取 Header=0 (中文)
        df = pd.read_excel(file_path, sheet_name='到期赎回价', header=0, engine='openpyxl')
        
        # 剔除第二行 (英文名)
        df = df.iloc[1:]
        
        # 清洗列名 (去除空格)
        df.columns = [c.strip() for c in df.columns]
        
        # 提取需要的列：'代码' 和 '到期赎回价'
        # 截图显示列名可能是 "代码 " 或 "到期赎回价 " (带空格?) 
        # 我们尝试模糊匹配
        code_col = None
        price_col = None
        
        for col in df.columns:
            if "代码" in col: code_col = col
            if "到期赎回价" in col: price_col = col
            
        if not code_col or not price_col:
            print(f"   未找到对应的列名，当前列：{df.columns}")
            return {}
            
        # 清洗数据
        # 代码列：118065.SH -> 118065 (如果需要去后缀)
        # 但我们之前保留了后缀，所以直接用
        
        # 价格列：转为 numeric
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
        
        # 转换为字典 {code: price}
        redemption_map = df.set_index(code_col)[price_col].dropna().to_dict()
        
        print(f"   成功读取 {len(redemption_map)} 条赎回价数据")
        return redemption_map
        
    except Exception as e:
        print(f"   Excel 读取失败：{e}")
        return {}

# 获取赎回价字典
redemption_map = get_redemption_prices_from_excel(DATA_FILE)


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
                  N=10000, steps=2000):
    """
    Z-L 模型蒙特卡洛定价 (Numba JIT 优化版本)
    使用 JIT 编译加速计算，速度提升 10-100 倍
    """
    # 计算回售开始的时间步
    put_start_time = max(0.0, T - put_start_year)
    dt = T / steps
    put_start_idx = int(put_start_time / dt)

    # 预生成随机数矩阵
    np.random.seed(42)
    random_matrix = np.random.normal(0, 1, (N, steps))
    
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
SUMMARY_FILE = os.path.join(BASE_DIR, "ZL_Model_Summary.xlsx")
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
            
            model_price = zl_mc_pricing(
                S0=S0_sim, X0=X0_sim, r=r, credit_spread=cs, sigma=sigma, T=T, BPS=BPS_sim,
                redemption_price=redeem_price, 
                put_price=100.0,    # 假设回售价为 100 + 利息 (这里简化为100)
                put_barrier=0.7,    # 回售触发比例
                put_start_year=2.0, # 回售期通常为最后2年
                N=10000, 
                steps=sim_steps
            )
            
            df_zl_model.loc[date, bond_code] = model_price
            df_zl_error.loc[date, bond_code] = model_price - market_price
            
    except Exception as e:
        date_error_count += 1
        if date_error_count <= 5:
            print(f"Date-level error on {date}: {e}")

print(f"日期级异常数量：{date_error_count}")

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

print("绘图完成！")
print("1. Fig1_ZL_Price_Time_Series.png")
print("2. Fig2_ZL_Moneyness.png")
print("3. Fig3_ZL_Maturity.png")

# ==========================================
# 图 4: 错误定价与评级的关系
# ==========================================
try:
    print("   正在绘制图 4: 错误定价与评级的关系...")
    # 读取评级数据预览以确定 Header
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
                
            # 检查日期范围是否合理 (例如：2000 年以后)
            # 排除被误判为日期的数字 (如 0 -> 1970-01-01)
            valid_dates = dates[dates.notnull()]
            if valid_dates.min().year < 2000:
                continue
                
            date_col = col
            break
        except:
            continue
            
    if date_col is None and len(df_rating.columns) > 2:
        # 尝试检查第 3 列 (索引 2)，即使前面的逻辑没过
        col_candidate = df_rating.columns[2]
        sample = df_rating[col_candidate].dropna().iloc[:10]
        try:
             dates = pd.to_datetime(sample, errors='coerce')
             if dates.notnull().any() and dates.max().year >= 2000:
                 date_col = col_candidate
        except:
             pass
            
    if date_col:
        print(f"   使用评级日期列：{date_col}")
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
                
                plt.savefig(os.path.join(BASE_DIR, "Fig4_ZL_Rating.png"), dpi=300)
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
