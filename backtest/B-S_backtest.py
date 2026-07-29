import pandas as pd
import numpy as np
import tushare as ts
from scipy.stats import norm
from tqdm import tqdm
import warnings
import time
import os
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns

from market_data_contracts import (
    DataContractError,
    PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    build_active_market_mask,
    build_contractual_par_matrix,
    build_observed_volatility,
    build_risk_free_rate_matrix,
    load_rebuildable_matrix_cache,
    select_completed_weekly_dates,
    validate_pricing_coverage,
)
from token_loader import load_tushare_token

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 忽略计算警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置与数据读取 (基于 Tushare Pipeline CSV 缓存)
# ==========================================
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))  # backtest/ 目录
REBUILD_ALL = '--rebuild-all' in sys.argv
REFRESH_INPUT_CACHE = '--refresh-input-cache' in sys.argv
WEEKLY_ONLY = '--weekly' in sys.argv


def _load_csv(filename):
    path = os.path.join(PIPELINE_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Pipeline cache not found: {path}\n"
            "请先运行 python backtest/data_pipeline.py 生成数据缓存。"
        )
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, errors='coerce')
    df = df[df.index.notna()].apply(pd.to_numeric, errors='coerce')
    return df


print("1. 正在从 Tushare Pipeline CSV 读取数据...")
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
if WEEKLY_ONLY:
    weekly_dates = select_completed_weekly_dates(df_price.index)
    if len(weekly_dates) == 0:
        raise DataContractError("no completed weekly valuation date")
    df_price = df_price.loc[weekly_dates]
    df_cv = df_cv.loc[weekly_dates]
    df_floor = df_floor.loc[weekly_dates]
    df_maturity = df_maturity.loc[weekly_dates]
coverage_dates = df_price.index[-1:]

# ==========================================
# 2. 建立 [转债代码 -> 正股代码] 映射
# ==========================================
print("2. 正在解析正股代码映射...")
try:
    df_basic_info = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_basic_info.csv'))
    bond_to_stock = (
        df_basic_info.dropna(subset=['ts_code', 'stk_cd'])
        .set_index('ts_code')['stk_cd']
        .to_dict()
    )
    print(f"   成功建立 {len(bond_to_stock)} 对转债-正股映射关系（来源：cb_basic_info.csv）")
except Exception as e:
    print(f"   映射读取失败: {e}")
    bond_to_stock = {}

# ==========================================
# 2.5 读取契约面值并构建标准化期权行权价
# ==========================================
print("2.5 正在读取契约面值并构建 K 矩阵...")
df_basic_info = pd.read_csv(os.path.join(PIPELINE_DIR, 'cb_basic_info.csv'))
df_k_strike = build_contractual_par_matrix(
    dates=df_price.index,
    bonds=list(df_price.columns),
    cb_basic=df_basic_info,
)
print(f"   已加载 {len(df_k_strike.columns)} 只转债的真实契约面值")

# ==========================================
# 3. Tushare 获取正股价格并计算波动率
# ==========================================
print("3. 开始通过 Tushare 获取正股历史波动率...")

# 初始化 Tushare（token 从环境变量 TUSHARE_TOKEN 或 backtest/tushare_token.txt 读取）
try:
    pro = ts.pro_api(load_tushare_token())
except Exception as e:
    print(f"Warning: Tushare 初始化失败，请检查 Token 设置。错误: {e}")
    pro = None

VOL_CACHE_FILE = os.path.join(PIPELINE_DIR, "bs_volatility_cache.csv")


def _fetch_bond_volatility(bond_code):
    """拉取单只转债正股的 250 日滚动年化波动率，返回对齐 df_price.index 的 Series 或 None。"""
    stock_code_full = bond_to_stock.get(bond_code)
    if not stock_code_full or pro is None:
        return None
    start_dt = (
        df_price.index.min() - pd.Timedelta(days=500)
    ).strftime("%Y%m%d")
    end_dt = df_price.index.max().strftime("%Y%m%d")
    # 使用 ts.pro_bar 获取前复权行情（受积分/频率限制）
    df_stock = ts.pro_bar(
        ts_code=stock_code_full,
        adj='qfq',
        start_date=start_dt,
        end_date=end_dt,
        api=pro,
    )
    if df_stock is None or df_stock.empty:
        return None
    df_stock['trade_date'] = pd.to_datetime(df_stock['trade_date'])
    df_stock = df_stock.sort_values('trade_date').set_index('trade_date')
    return build_observed_volatility(
        adjusted_close=df_stock['close'],
        target_dates=df_price.index,
        window=250,
        min_observations=60,
    )


# 全量重建显式绕过历史缓存，避免旧版写入的 40% 常数无法识别。
df_volatility = load_rebuildable_matrix_cache(
    path=VOL_CACHE_FILE,
    index=df_price.index,
    columns=df_price.columns,
    refresh_cache=REFRESH_INPUT_CACHE,
)

# 增量补算: 有市场价但波动率缺失的债券（新债，或缓存生成后新增的交易日）
# 修复历史 bug: 旧逻辑缓存存在时直接 fillna(0.40)，导致所有新交易日永远用 40% 兜底波动率
input_check_dates = (
    df_price.index
    if REBUILD_ALL or REFRESH_INPUT_CACHE
    else coverage_dates
)
pending_vol = (
    df_price.loc[input_check_dates].notna()
    & df_volatility.loc[input_check_dates].isna()
).any()
pending_bonds = pending_vol[pending_vol].index.tolist()
if pending_bonds:
    print(f"   {len(pending_bonds)} 只转债的波动率存在缺口，开始增量补算 ...")
    updated_count = 0
    for bond_code in tqdm(pending_bonds, desc="Fetching Stock Volatility"):
        try:
            vol_series = _fetch_bond_volatility(bond_code)
            if vol_series is not None:
                df_volatility[bond_code] = vol_series
                updated_count += 1
            time.sleep(0.05)  # 避免请求过快
        except Exception as e:
            print(f"   获取 {bond_code} 波动率失败: {e}")
    if updated_count:
        try:
            df_volatility.to_csv(VOL_CACHE_FILE)
            print(f"   波动率已缓存至: {VOL_CACHE_FILE} (更新 {updated_count} 只)")
        except Exception as e:
            print(f"   缓存保存失败: {e}")

# 不再用 40% 覆盖缺失历史；不足 60 个观测的单元会被有效样本掩码排除。
print(f"   波动率数据准备完成，非空率 {df_volatility.notna().mean().mean():.1%}。")

# ==========================================
# 4. 获取无风险利率 (Akshare)
# ==========================================
print("4. 从 AkShare 国债曲线缓存匹配无风险利率...")
rf_cache_path = os.path.join(PIPELINE_DIR, 'rf_yield_cache.csv')
if not os.path.exists(rf_cache_path):
    raise DataContractError(
        "rf_yield_cache.csv 不存在，请先运行 data_pipeline.py 拉取 AkShare 国债曲线"
    )
yield_table = pd.read_csv(rf_cache_path, index_col=0, parse_dates=True)
yield_table.columns = [
    float(str(column).split('.')[0])
    if str(column).endswith('.0')
    else float(column)
    for column in yield_table.columns
]
yield_table = yield_table.T.groupby(level=0).first().T.sort_index(axis=1)
if WEEKLY_ONLY:
    curve_start = yield_table.dropna(how="all").index.min()
    supported_dates = df_price.index[df_price.index >= curve_start]
    if len(supported_dates) == 0:
        raise DataContractError("weekly dates are earlier than the yield curve")
    df_price = df_price.loc[supported_dates]
    df_cv = df_cv.loc[supported_dates]
    df_floor = df_floor.loc[supported_dates]
    df_maturity = df_maturity.loc[supported_dates]
    df_k_strike = df_k_strike.loc[supported_dates]
    df_volatility = df_volatility.loc[supported_dates]
    coverage_dates = df_price.index[-1:]
rf_df = build_risk_free_rate_matrix(
    curve=yield_table,
    maturity=df_maturity,
)
print("   无风险利率期限结构匹配完成。")

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
active_mask = build_active_market_mask(
    price=df_price,
    required_inputs=[
        df_cv,
        df_floor,
        df_maturity,
        rf_df,
        df_volatility,
        df_k_strike,
    ],
)
active_mask &= (
    (df_cv > 0)
    & (df_floor > 0)
    & (df_maturity > 0)
    & (df_volatility > 0)
    & (df_k_strike > 0)
)
df_theoretical = (df_floor + df_option).where(active_mask)
print(
    f"   有效配对样本: {int(active_mask.sum().sum()):,} / "
    f"{int(df_price.notna().sum().sum()):,}"
)
contract_validation_dates = (
    df_price.index if REBUILD_ALL and WEEKLY_ONLY else coverage_dates
)
validate_pricing_coverage(
    market_price=df_price,
    model_price=df_theoretical,
    dates=contract_validation_dates,
    min_coverage=float(os.environ.get("BS_MIN_PRICING_COVERAGE", "0.98")),
    min_count=int(os.environ.get("BS_MIN_PRICING_COUNT", "20")),
    label="BS weekly" if WEEKLY_ONLY else "BS latest",
    min_count_enforced_from=PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    historical_min_coverage=float(
        os.environ.get("BS_HISTORICAL_MIN_PRICING_COVERAGE", "0.975")
    ),
    min_coverage_enforced_from=pd.Timestamp(
        os.environ.get(
            "BS_MIN_PRICING_COVERAGE_ENFORCED_FROM",
            "2020-01-01",
        )
    ),
)

# 计算偏差 (理论价 - 实际价)
# > 0 表示理论价高于市场价 (市场低估)
# < 0 表示理论价低于市场价 (市场高估)
df_diff = df_theoretical - df_price
df_diff_pct = df_diff / df_price

# ==========================================
# 6. 结果输出
# ==========================================
# 保存结果
df_theoretical.to_csv(os.path.join(PIPELINE_DIR, "BS_Model_Prices.csv"))
df_price.to_csv(os.path.join(PIPELINE_DIR, "Market_Prices.csv"))
df_diff.to_csv(os.path.join(PIPELINE_DIR, "BS_Model_Deviation_Abs.csv"))
df_diff_pct.to_csv(os.path.join(PIPELINE_DIR, "BS_Model_Deviation_Pct.csv"))

print("计算完成！")
print("结果已保存:")
print("1. 理论价格: 'BS_Model_Prices.csv'")
print("2. 市场价格: 'Market_Prices.csv'")
print("3. 绝对偏差 (Model - Market): 'BS_Model_Deviation_Abs.csv'")
print("4. 相对偏差 (Model - Market)/Market: 'BS_Model_Deviation_Pct.csv'")

# 保存汇总 Excel
with pd.ExcelWriter(os.path.join(PIPELINE_DIR, "BS_Model_Summary.xlsx")) as writer:
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
labels = [line.get_label() for line in lines]
# 添加填充图的图例代理
patch = mpatches.Patch(color='gray', alpha=0.5, label='定价错误')
lines.append(patch)
labels.append('定价错误')
ax1.legend(lines, labels, loc='upper center')

plt.title('图1 BS模型定价结果与市场价格对比')
plt.savefig(os.path.join(PIPELINE_DIR, "Fig1_BS_Price_Time_Series.png"), dpi=300)
plt.close()

# 图2: 定价结果与在值程度的关系 (Moneyness)
# 在值程度 = log(S/X)
# 在值程度 measure: ln(S/X) = ln(CV/100)  (CV = 100/X * S, 所以 S/X = CV/100)
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
plt.savefig(os.path.join(PIPELINE_DIR, "Fig2_BS_Moneyness.png"), dpi=300)
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
plt.savefig(os.path.join(PIPELINE_DIR, "Fig3_BS_Maturity.png"), dpi=300)
plt.close()

print("绘图完成！")
print("1. Fig1_BS_Price_Time_Series.png")
print("2. Fig2_BS_Moneyness.png")
print("3. Fig3_BS_Maturity.png")

# ==========================================
# 图4: 错误定价与评级的关系
# ==========================================
try:
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
        plt.title('图4 错误定价与评级的关系 (2019年以来平均)')
        plt.ylabel('平均错误定价 (%)')
        plt.xlabel('信用评级')
        plt.axhline(0, color='k', linewidth=0.8)
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        plt.savefig(os.path.join(PIPELINE_DIR, "Fig4_BS_Rating.png"), dpi=300)
        plt.close()
        print("4. Fig4_BS_Rating")
        
    else:
        print("   无法识别评级数据的日期列，跳过图4绘制。")

except Exception as e:
    print(f"   绘制失败: {e}")
