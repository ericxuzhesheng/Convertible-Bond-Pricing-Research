import pandas as pd
import numpy as np
import tushare as ts
import akshare as ak
from scipy.stats import norm
from tqdm import tqdm
import warnings
import time
import os
import matplotlib.pyplot as pd_plt
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns

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
EXCEL_REDEMPTION_PATH = os.path.join(BASE_DIR, "转债错误定价数据.xlsx")

# 定义 Sheet 名称
SHEET_PRICE = "可转债价格"
SHEET_CV = "转换价值"
SHEET_FLOOR = "纯债价值"
SHEET_MATURITY = "剩余期限"
SHEET_STOCK_MAP = "正股市值"
SHEET_REDEMPTION = "到期赎回价"
SHEET_RATING = "信用评级"

def load_data(file_path, sheet_name):
    # 读取并解析索引为日期
    # 使用 openpyxl 引擎读取 Excel
    df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=0, engine='openpyxl')
    df.index = pd.to_datetime(df.index, errors='coerce')
    df = df.dropna(how='all')
    return df.apply(pd.to_numeric, errors='coerce')

print("1. 正在读取 Excel 静态数据...")
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
# 2. 建立 [转债代码 -> 正股代码] 映射
# ==========================================
print("2. 正在解析正股代码映射...")
# 注意：正股市值 sheet 的结构比较特殊，前几行是元数据
# 我们读取前几行，假设第2行(Index 1)是正股代码，第4行(Index 3)是转债代码
try:
    df_meta = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_STOCK_MAP, header=None, nrows=10, engine='openpyxl')
    stock_row = df_meta.iloc[1, 1:].values  # 正股代码行
    bond_row = df_meta.iloc[3, 1:].values   # 转债代码行
    
    # 建立字典: {转债代码: 正股代码}
    bond_to_stock = {}
    for b, s in zip(bond_row, stock_row):
        if pd.notna(b) and pd.notna(s):
            bond_to_stock[str(b).strip()] = str(s).strip()
    
    print(f"   成功建立 {len(bond_to_stock)} 对转债-正股映射关系")
except Exception as e:
    print(f"   映射解析失败: {e}")
    bond_to_stock = {}

# ==========================================
# 2.5 获取到期赎回价并构建 K 值矩阵
# ==========================================
print("2.5 正在读取并构建到期赎回价矩阵 (K)...")
try:
    # 读取静态数据
    df_redemption_static = pd.read_excel(EXCEL_REDEMPTION_PATH, sheet_name=SHEET_REDEMPTION, engine='openpyxl')
    
    # 尝试识别列名 (包含 "代码" 和 "赎回价")
    col_code = next((c for c in df_redemption_static.columns if "代码" in str(c)), None)
    col_price = next((c for c in df_redemption_static.columns if "赎回价" in str(c)), None)
    
    if col_code and col_price:
        # 建立映射 {code: price}
        # 确保 code 格式一致 (转字符串, strip)
        df_redemption_static[col_code] = df_redemption_static[col_code].astype(str).str.strip()
        
        # 转换为字典
        redemption_map = df_redemption_static.set_index(col_code)[col_price].to_dict()
        
        print(f"   成功读取 {len(redemption_map)} 条赎回价数据")
    else:
        print("   未找到 '代码' 或 '赎回价' 相关列，使用默认值 100")
        redemption_map = {}
        
except Exception as e:
    print(f"   读取赎回价 Sheet 失败 ({e})，使用默认值 100")
    redemption_map = {}

# 构建 df_k_strike (Broadcasting)
# 初始化为 100
df_k_strike = pd.DataFrame(100.0, index=df_price.index, columns=df_price.columns)

# 填充实际值
count_filled = 0
for col in df_k_strike.columns:
    # 尝试直接匹配
    val = redemption_map.get(col)
    # 尝试去后缀匹配 (e.g. 113050.SH -> 113050)
    if val is None:
            val = redemption_map.get(col.split('.')[0])
            
    if val is not None:
        try:
            val = float(val)
            if not np.isnan(val):
                df_k_strike[col] = val
                count_filled += 1
        except:
            pass

print(f"   已将 {count_filled} 只转债的赎回价填充到 K 值矩阵")

# ==========================================
# 3. Tushare 获取正股价格并计算波动率
# ==========================================
print("3. 开始通过 Tushare 获取正股历史波动率...")

# 初始化 Tushare (请替换为您自己的 Token)
ts.set_token('3a3346cc26a4ca6ff9b860a2c6863e4dd31dc497afb79f047696ada9') 

try:
    pro = ts.pro_api()
except Exception as e:
    print(f"Warning: Tushare 初始化失败，请检查 Token 设置。错误: {e}")
    pro = None

VOL_CACHE_FILE = "bs_volatility_cache.csv"

# 尝试读取缓存
if os.path.exists(VOL_CACHE_FILE):
    print("   发现波动率缓存，正在读取...")
    try:
        df_volatility = pd.read_csv(VOL_CACHE_FILE, index_col=0, parse_dates=True)
        # 确保索引和列与当前数据匹配
        df_volatility = df_volatility.reindex(index=df_price.index, columns=df_price.columns)
        print("   缓存读取成功。")
    except Exception as e:
        print(f"   缓存读取失败 ({e})，将重新计算。")
        df_volatility = None
else:
    df_volatility = None

if df_volatility is None:
    # 初始化波动率 DataFrame (结构同转债价格表)
    df_volatility = pd.DataFrame(index=df_price.index, columns=df_price.columns)

    # 遍历每一个转债代码
    for bond_code in tqdm(df_price.columns, desc="Fetching Stock Volatility"):
        stock_code_full = bond_to_stock.get(bond_code)
        
        if not stock_code_full:
            print(f"   跳过 {bond_code}: 未找到正股代码")
            continue
            
        # 处理代码格式：无需特殊转换，确保 stock_code_full 格式正确
        
        try:
            if pro is None:
                raise ValueError("Tushare API 未初始化")

            # 获取正股日线数据 (复权: qfq)
            start_dt = df_price.index.min().strftime("%Y%m%d")
            end_dt = df_price.index.max().strftime("%Y%m%d")
            
            # 使用 ts.pro_bar 获取复权行情
            # 注意: pro_bar 需要 internet 连接且受积分限制，速度可能受限
            df_stock = ts.pro_bar(ts_code=stock_code_full, adj='qfq', start_date=start_dt, end_date=end_dt)
            
            if df_stock is None or df_stock.empty:
                # 尝试不复权数据作为 fallback (虽然不推荐)
                # df_k = pro.daily(ts_code=stock_code_full, start_date=start_dt, end_date=end_dt)
                print(f"   获取 {stock_code_full} 数据为空")
                continue

            # Tushare 返回的数据通常是按日期降序排列的，需要转为升序
            df_stock['trade_date'] = pd.to_datetime(df_stock['trade_date'])
            df_stock = df_stock.sort_values('trade_date')
            df_stock.set_index('trade_date', inplace=True)
            
            # 计算对数收益率
            df_stock['log_ret'] = np.log(df_stock['close'] / df_stock['close'].shift(1))
            
            # 计算 250 日滚动波动率 (年化)
            # 核心公式: stdev * sqrt(250)
            # 修改：不足250天时，有多少数据算多少 (min_periods=2)
            df_stock['volatility'] = df_stock['log_ret'].rolling(window=250, min_periods=2).std() * np.sqrt(250)
            
            # 将计算好的波动率填入总表 (按日期对齐)
            # reindex 确保日期匹配，缺失值填为 NaN (后续处理)
            vol_series = df_stock['volatility'].reindex(df_price.index)
            df_volatility[bond_code] = vol_series
            
            # 避免请求过快
            time.sleep(0.05) 
            
        except Exception as e:
            print(f"   获取 {stock_code_full} 失败: {e}")
            
    # 保存缓存
    try:
        df_volatility.to_csv(VOL_CACHE_FILE)
        print(f"   波动率已缓存至: {VOL_CACHE_FILE}")
    except Exception as e:
        print(f"   缓存保存失败: {e}")

# 填充缺失波动率 (对于刚上市不足2天无法计算std的，或获取失败的，用 40% 填充)
df_volatility = df_volatility.fillna(0.40)
print("   波动率数据准备完成。")

# ==========================================
# 4. 获取无风险利率 (Akshare)
# ==========================================
print("4. 获取国债收益率...")
try:
    # 拉取更长的时间范围以覆盖回测
    df_yield = ak.bond_china_yield(start_date="20200101", end_date="20261231")
    target_curve = df_yield[df_yield['曲线名称']=='中债国债收益率曲线']
    
    target_curve['日期'] = pd.to_datetime(target_curve['日期'])
    target_curve.set_index('日期', inplace=True)
    
    # 定义需要的期限和对应的列名
    # 键: Akshare列名, 值: 期限(年)
    tenor_cols = ['1年', '2年', '3年', '5年', '7年', '10年']
    # 确保列存在
    available_cols = [c for c in tenor_cols if c in target_curve.columns]
    
    # 提取收益率数据 (Time x Tenors), 转换为小数
    yield_table = target_curve[available_cols] / 100.0
    
    # 对齐到回测的日期索引 (Forward Fill)
    yield_table = yield_table.reindex(df_price.index).fillna(method='ffill').fillna(0.02)
    
    # 初始化 rf_df
    rf_df = pd.DataFrame(index=df_price.index, columns=df_price.columns)
    
    # -------------------------------------------------------
    # 修改：使用线性插值 (Linear Interpolation) 匹配利率
    # -------------------------------------------------------
    # 1. 准备插值所需的 X 轴 (期限数值)
    tenor_map = {'1年': 1.0, '2年': 2.0, '3年': 3.0, '5年': 5.0, '7年': 7.0, '10年': 10.0}
    # available_cols 是 yield_table 的列名，确保 xp 是单调递增的
    xp = [tenor_map[c] for c in available_cols]
    
    # 2. 准备数据矩阵
    # yield_table: [Dates x Tenors]
    # df_maturity: [Dates x Bonds]
    yield_vals = yield_table.values
    maturity_vals = df_maturity.values
    
    # 3. 逐日插值
    # 结果矩阵初始化
    rf_matrix = np.zeros_like(maturity_vals)
    
    print(f"   正在对 {len(rf_matrix)} 个交易日进行收益率曲线线性插值...")
    for i in range(len(rf_matrix)):
        # 当天的 Yield Curve Y轴数据
        fp_row = yield_vals[i, :]
        # 当天各转债的剩余期限 X轴数据
        x_row = maturity_vals[i, :]
        
        # 执行插值
        # np.interp(x, xp, fp)
        # 规则：若 x < xp[0]，取 fp[0]；若 x > xp[-1]，取 fp[-1] (Flat Extrapolation)
        # 这里的 xp 和 fp_row 都是一维数组，对应当天的期限结构
        rf_matrix[i, :] = np.interp(x_row, xp, fp_row)
        
    # 4. 赋值给 rf_df
    rf_df = pd.DataFrame(rf_matrix, index=df_price.index, columns=df_price.columns)
        
    # 填充仍为空的
    rf_df = rf_df.fillna(0.02)
    print("   无风险利率期限结构匹配完成。")

except Exception as e:
    print(f"   获取利率失败，使用默认值 2%: {e}")
    rf_df = pd.DataFrame(0.02, index=df_price.index, columns=df_price.columns)

# ==========================================
# 5. 运行 B-S 模型
# ==========================================
def bs_call_price(S, K, T, r, sigma):
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)
    r = np.asarray(r, dtype=float)
    sigma = np.asarray(sigma, dtype=float)

    try:
        S, K, T, r, sigma = np.broadcast_arrays(S, K, T, r, sigma)
    except ValueError as e:
        raise ValueError(
            f"bs_call_price 输入无法广播: "
            f"S{S.shape}, K{K.shape}, T{T.shape}, r{r.shape}, sigma{sigma.shape}"
        ) from e

    # 防止除零错误
    T = np.maximum(T, 0.0001)
    sigma = np.maximum(sigma, 0.001)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

print("5. 计算 B-S 理论价格...")

# 核心公式变换：
# Call_Option_Value = Call(S=CV, K=100, T=Maturity, r=Rate, sigma=Stock_Vol)
# 这里的 K=100 是虚拟行权价，配合 S=CV 使用，数学上等价于真实转股权价值
# 修改: 使用真实的到期赎回价作为 K (Broadcasting)
expected_shape = df_price.shape
for _name, _df in [
    ("df_cv", df_cv),
    ("df_k_strike", df_k_strike),
    ("df_maturity", df_maturity),
    ("rf_df", rf_df),
    ("df_volatility", df_volatility),
]:
    if _df.shape != expected_shape:
        raise ValueError(f"{_name} shape={_df.shape} 与 df_price shape={expected_shape} 不一致")
    if not _df.index.equals(df_price.index) or not _df.columns.equals(df_price.columns):
        raise ValueError(f"{_name} 的 index/columns 未与 df_price 对齐")
option_values = bs_call_price(
    S=df_cv.values, 
    K=df_k_strike.values, 
    T=df_maturity.values, 
    r=rf_df.values, 
    sigma=df_volatility.values
)

df_option = pd.DataFrame(option_values, index=df_price.index, columns=df_price.columns)

# 转债理论价 = 纯债价值 + 期权价值
df_theoretical = df_floor + df_option

# 计算偏差 (理论价 - 实际价)
# > 0 表示理论价高于市场价 (市场低估)
# < 0 表示理论价低于市场价 (市场高估)
df_diff = df_theoretical - df_price
df_diff_pct = df_diff / df_price

# ==========================================
# 6. 结果输出
# ==========================================
# 保存结果
df_theoretical.to_csv("BS_Model_Prices.csv")
df_price.to_csv("Market_Prices.csv")
df_diff.to_csv("BS_Model_Deviation_Abs.csv")
df_diff_pct.to_csv("BS_Model_Deviation_Pct.csv")

print("计算完成！")
print("结果已保存:")
print("1. 理论价格: 'BS_Model_Prices.csv'")
print("2. 市场价格: 'Market_Prices.csv'")
print("3. 绝对偏差 (Model - Market): 'BS_Model_Deviation_Abs.csv'")
print("4. 相对偏差 (Model - Market)/Market: 'BS_Model_Deviation_Pct.csv'")

# 保存汇总 Excel
with pd.ExcelWriter("BS_Model_Summary.xlsx") as writer:
    df_theoretical.to_excel(writer, sheet_name="理论价格")
    df_price.to_excel(writer, sheet_name="市场价格")
    df_diff.to_excel(writer, sheet_name="绝对偏差")
    df_diff_pct.to_excel(writer, sheet_name="相对偏差")
print("5. 汇总 Excel: 'BS_Model_Summary.xlsx'")

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
smape = (df_diff.abs() / ((df_theoretical.abs() + df_price.abs()) / 2)).mean().mean() * 100

print("-" * 30)
print("模型整体误差指标:")
print(f"Mean Error (Bias): {mean_error:.4f} 元")
print(f"MAE (平均绝对误差): {mae:.4f} 元")
print(f"RMSE (均方根误差): {rmse:.4f} 元")
print(f"MAPE (平均绝对百分比误差): {mape:.4f} %")
print(f"SMAPE (对称平均绝对百分比误差): {smape:.4f} %")
print("-" * 30)

# ==========================================
# 7. 绘图 
# ==========================================
print("7. 正在绘制图表...")

# 图1: 转债平均价格走势 (市场价 vs 模型价 vs 定价错误)
plt.figure(figsize=(12, 6))
ax1 = plt.gca()
# 计算每日市场平均价和模型平均价
daily_market_avg = df_price.mean(axis=1)
daily_model_avg = df_theoretical.mean(axis=1)
# 计算每日平均相对误差 (%)
daily_err_pct = df_diff_pct.mean(axis=1) * 100

l1, = ax1.plot(daily_market_avg.index, daily_model_avg, 'k-', label='BS模型', linewidth=1.5)
l2, = ax1.plot(daily_market_avg.index, daily_market_avg, 'k--', label='市场价格', linewidth=1.5)
ax1.set_ylabel('转债平均价格 (元)')
ax1.set_xlabel('年份')

ax2 = ax1.twinx()
# 绘制误差面积图 (灰色填充)
ax2.fill_between(daily_err_pct.index, daily_err_pct, 0, color='gray', alpha=0.5, label='定价错误')
ax2.set_ylabel('平均定价错误 (%)')
# 设置右轴范围，使其看起来像论文中的下方分布
ax2.set_ylim(-30, 80) 

# 合并图例
lines = [l1, l2]
labels = [l.get_label() for l in lines]
# 添加填充图的图例代理
import matplotlib.patches as mpatches
patch = mpatches.Patch(color='gray', alpha=0.5, label='定价错误')
lines.append(patch)
labels.append('定价错误')
ax1.legend(lines, labels, loc='upper center')

plt.title('图1 BS模型定价结果与市场价格对比')
plt.savefig("Fig1_BS_Price_Time_Series.png", dpi=300)
plt.close()

# 图2: 定价结果与在值程度的关系 (Moneyness)
# 在值程度 = log(S/X)
# 我们需要对所有样本点进行 Moneyness 分组
# 展开数据
# 堆叠所有数据: price, model, moneyness
S_all = df_cv.values * df_price.values / 100.0 # CV = 100/X * S. 所以 S/X = CV/100.
# 在值程度 measure: ln(S/X) = ln(CV/100)
moneyness = np.log(df_cv / 100.0)

# 将 DataFrame 展平为 Series
s_market = df_price.stack()
s_model = df_theoretical.stack()
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
plt.plot(x_axis, grouped['model'], 'k:', marker='None', label='BS模型')
plt.plot(x_axis, grouped['market'], 'k.-', label='市场价格')

plt.xlabel('在值程度 ln(S/X)')
plt.ylabel('平均价格 (元)')
plt.legend()
plt.title('图2 BS模型定价结果与在值程度的关系')
plt.grid(True, linestyle='--', alpha=0.3)
plt.savefig("Fig2_BS_Moneyness.png", dpi=300)
plt.close()

# 图3: 定价结果与剩余期限的关系
# 按剩余期限分组 (天)
# 将 maturity (年) 转换为 天
maturity_days = df_maturity * 365
s_days = maturity_days.stack()

df_scatter_mat = pd.DataFrame({'market': s_market, 'model': s_model, 'days': s_days})
df_scatter_mat = df_scatter_mat.dropna()

# 自定义分组 (仿照论文: 10, 30, 50, 100, 200, 400, 600, 800, 1000...)
# 这里使用简单的等宽或分位数可能更方便，为了复现图3风格，我们使用特定区间
# 论文图3横轴是离散的类别，不是连续轴
bins_days = [0, 30, 50, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 2000]
labels_days = ['30', '50', '100', '200', '400', '600', '800', '1000', '1200', '1400', '1600', '>1600']
# 注意：pd.cut labels 数量要比 bins 少 1
# 修正 labels
labels_days = [str(b) for b in bins_days[1:-1]] + ['>1600'] # 简单起见，用右端点

df_scatter_mat['bin'] = pd.cut(df_scatter_mat['days'], bins=bins_days)
grouped_mat = df_scatter_mat.groupby('bin')[['market', 'model']].mean()

plt.figure(figsize=(10, 6))
# x轴用字符串标签
x_idx = range(len(grouped_mat))
plt.plot(x_idx, grouped_mat['model'], 'k:', label='BS模型')
plt.plot(x_idx, grouped_mat['market'], 'k.-', label='市场价格')

plt.xticks(x_idx, [str(int(i.right)) for i in grouped_mat.index], rotation=0)
plt.xlabel('剩余期限 (天)')
plt.ylabel('平均价格 (元)')
plt.legend()
plt.title('图3 BS模型定价结果与剩余期限的关系')
plt.grid(True, linestyle='--', alpha=0.3)
plt.savefig("Fig3_BS_Maturity.png", dpi=300)
plt.close()

print("绘图完成！")
print("1. Fig1_BS_Price_Time_Series.png")
print("2. Fig2_BS_Moneyness.png")
print("3. Fig3_BS_Maturity.png")

# ==========================================
# 图4: 错误定价与评级的关系
# ==========================================
try:
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
            raise ValueError("No model data after 2019")

        # 仅保留在模型列中的评级列 (转债代码)
        valid_rating_cols = df_rating.columns.intersection(df_diff_pct.columns)
        
        if len(valid_rating_cols) == 0:
            print("   错误: 评级数据与模型数据没有重叠的转债代码")
            raise ValueError("No overlapping bond codes")

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
        plt.title(f'图4 错误定价与评级的关系 (2019年以来平均)')
        plt.ylabel('平均错误定价 (%)')
        plt.xlabel('信用评级')
        plt.axhline(0, color='k', linewidth=0.8)
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        plt.savefig("Fig4_BS_Rating.png", dpi=300)
        plt.close()
        print("4. Fig4_BS_Rating")
        
    else:
        print("   无法识别评级数据的日期列，跳过图4绘制。")

except Exception as e:
    print(f"   绘制失败: {e}")
