"""
6因子复合策略回测脚本
计算相对于中证转债指数（市场等权）的超额收益

因子包括：
1. 定价偏差（BS模型相对偏差）- 取Max（做多模型高估）
2. 估值因子 - 取Min（做空高估值）
3. 流动性因子 - 取Max（做多高流动性）
4. 波动率因子 - 取Max（做多高波动）
5. 量价相关性因子 - 取Max（做多高相关性）
6. 动量因子 - 取Max（做多高动量）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
from datetime import datetime, timedelta
import glob, re

warnings.filterwarnings("ignore")

# 设置中文字体
plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
plt.rcParams["axes.unicode_minus"] = False


class MultiFactorBacktest:
    def __init__(self, data_dir):
        """初始化回测系统"""
        self.data_dir = Path(data_dir)
        self.factors = {}
        self.prices = None
        self.bs_deviation = None
        self.rebalance_dates = None
        self.bond_filters_data = None  # 转债筛选数据
        self.ic_history_df = None  # IC 历史数据
        self.aligned_factors = {}  # 对齐后的原始因子
        self.normalized_factors = {}  # 对齐且Z-Score后的因子

    def standardize_code(self, code):
        """
        统一代码格式: sh110067 -> 110067.SH, sz128145 -> 128145.SZ
        """
        if isinstance(code, str):
            if code.startswith("sh"):
                return code[2:] + ".SH"
            elif code.startswith("sz"):
                return code[2:] + ".SZ"
            elif "." in code:
                return code
        return code

    def load_factor_csv(self, filename, factor_name):
        """加载CSV格式的因子数据"""
        print(f"  加载 {factor_name}...")
        df = pd.read_csv(self.data_dir / filename)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        # 统一代码格式
        df.columns = [self.standardize_code(col) for col in df.columns]

        return df

    def load_data(self):
        """加载所有数据文件"""
        print("=" * 60)
        print("步骤 1: 加载数据文件")
        print("=" * 60)

        # 自动匹配最新日期的因子文件（后6位为日期）
        def latest_factor(pattern):
            files = glob.glob(str(self.data_dir / pattern))
            dates = [re.search(r"(\d{6})\.csv$", f) for f in files]
            dates = [m.group(1) for m in dates if m]
            if not dates:
                raise FileNotFoundError(f"未找到符合 {pattern} 的因子文件")
            latest_date = sorted(dates)[-1]
            return pattern.replace("*", latest_date)

        self.factors["liquidity"] = self.load_factor_csv(
            latest_factor("流动性因子等权和*.csv"), "流动性因子"
        )
        self.factors["volatility"] = self.load_factor_csv(
            latest_factor("波动率因子等权和*.csv"), "波动率因子"
        )
        self.factors["price_volume"] = self.load_factor_csv(
            latest_factor("量价相关性因子等权和*.csv"), "量价相关性因子"
        )
        self.factors["valuation"] = self.load_factor_csv(
            latest_factor("估值因子等权和*.csv"), "估值因子"
        )
        self.factors["momentum"] = self.load_factor_csv(
            latest_factor("动量因子等权和*.csv"), "动量因子"
        )
        # 加载BS模型数据
        print("  加载 BS模型数据...")
        bs_file = self.data_dir / "BS_Model_Summary.xlsx"

        # 相对偏差
        bs_dev = pd.read_excel(bs_file, sheet_name="相对偏差")
        bs_dev = bs_dev.rename(columns={"返回目录": "date"})
        bs_dev["date"] = pd.to_datetime(bs_dev["date"])
        bs_dev = bs_dev.set_index("date").iloc[1:]  # 跳过第一行（可能是标题）
        self.factors["bs_deviation"] = bs_dev

        # 市场价格
        prices = pd.read_excel(bs_file, sheet_name="市场价格")
        prices = prices.rename(columns={"返回目录": "date"})
        prices["date"] = pd.to_datetime(prices["date"])
        prices = prices.set_index("date").iloc[1:]  # 跳过第一行
        self.prices = prices

        # 加载基准指数数据 (000832.CSI)
        print("  加载基准指数数据 (000832.CSI)...")
        benchmark_file = self.data_dir / "000832_CSI_close_price.xlsx"
        if benchmark_file.exists():
            try:
                # header=5 对应第6行 (Date, close)
                df_bench = pd.read_excel(benchmark_file, header=5)
                df_bench["Date"] = pd.to_datetime(df_bench["Date"])
                df_bench = df_bench.set_index("Date")["close"]
                # 过滤掉0值
                df_bench = df_bench[df_bench != 0]
                self.benchmark_prices = df_bench
                print(f"  成功加载基准数据: {len(df_bench)} 条")
            except Exception as e:
                print(f"  加载基准数据失败: {e}")
                self.benchmark_prices = None
        else:
            print("  警告: 未找到基准数据文件，将使用市场等权平均")
            self.benchmark_prices = None

        print(f"\n数据加载完成！")
        print(f"  价格数据: {self.prices.shape[0]} 天 × {self.prices.shape[1]} 只转债")
        print(f"  日期范围: {self.prices.index.min()} 至 {self.prices.index.max()}")

    def check_factor_correlation(self):
        """
        检查因子相关性并绘制热力图
        """
        print("\n" + "=" * 60)
        print("检查因子相关性")
        print("=" * 60)

        # 1. 准备数据：将所有因子对齐到一个DataFrame中
        # 既然因子是 (Date, Bond) 的矩阵，我们将它们展开 (Stack) 成一维序列进行比较
        stacked_factors = {}

        for name, df in self.factors.items():
            # 展开并重命名
            # stack() 会产生 MultiIndex (Date, Code)
            stacked = df.stack()
            stacked.name = name
            stacked_factors[name] = stacked

        # 合并所有因子
        merged_df = pd.DataFrame(stacked_factors)

        # 去除空值
        merged_df = merged_df.dropna()

        if len(merged_df) == 0:
            print("  警告: 无法计算相关性，有效数据为空")
            return

        # 2. 计算相关性矩阵
        corr_matrix = merged_df.corr()

        print("  因子相关性矩阵:")
        print(corr_matrix)

        # 3. 检查高相关性
        high_corr_pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                val = corr_matrix.iloc[i, j]
                if abs(val) > 0.8:
                    pair = (corr_matrix.columns[i], corr_matrix.columns[j])
                    high_corr_pairs.append((pair, val))
                    print(f"  [警告] 因子高度相关: {pair[0]} - {pair[1]} = {val:.4f}")

        # 4. 绘制热力图
        plt.figure(figsize=(10, 8))
        sns.heatmap(corr_matrix, annot=True, cmap="RdBu_r", center=0, vmin=-1, vmax=1)
        plt.title("因子相关性热力图")

        save_path = self.data_dir / "BS_factor_correlation.png"
        plt.savefig(save_path)
        print(f"  相关性热力图已保存至: {save_path}")
        plt.close()

    def get_rebalance_dates(self, start_date="2019-01-01"):
        """获取月度再平衡日期（每月最后一个交易日）"""
        print("\n" + "=" * 60)
        print("步骤 2: 提取月度再平衡日期")
        print("=" * 60)

        # 使用价格数据的日期
        dates = self.prices.index
        monthly_last = dates.to_series().groupby([dates.year, dates.month]).last()
        all_dates = monthly_last.sort_values()

        # 过滤从指定日期开始
        start_date = pd.to_datetime(start_date)
        self.rebalance_dates = all_dates[all_dates >= start_date]

        print(f"  回测起始日期: {start_date.strftime('%Y-%m-%d')}")
        print(f"  再平衡日期数量: {len(self.rebalance_dates)}")
        print(f"  首次再平衡: {self.rebalance_dates.iloc[0]}")
        print(f"  最后再平衡: {self.rebalance_dates.iloc[-1]}")

        return self.rebalance_dates

    def load_bond_filters_data(self):
        """
        加载转债筛选所需的数据 (从Excel文件)
        """
        print("\n" + "=" * 60)
        print("加载转债筛选数据 (Excel)")
        print("=" * 60)

        excel_path = (
            self.data_dir / "【浙商固收】转债资产端特征数据库【周更新外发】.xlsx"
        )
        if not excel_path.exists():
            print(f"  错误: 未找到筛选数据文件 {excel_path}")
            self.bond_filters_data = None
            return

        try:
            print(f"  正在读取 {excel_path.name} ...")
            # 使用 pd.ExcelFile 避免重复打开文件
            xls = pd.ExcelFile(excel_path)

            self.bond_filters_data = {}

            # 1. 加载流动性 (可转债交易额)
            # Row 3 (index 3) is Codes. Data starts Row 5 (index 5). Date Col 0.
            print("  加载流动性数据...")
            df_turnover = pd.read_excel(xls, sheet_name="可转债交易额", header=None)
            # 确保日期列是 datetime 类型
            dates = pd.to_datetime(df_turnover.iloc[5:, 0].tolist(), errors="coerce")
            codes = df_turnover.iloc[3, 1:].tolist()
            data = df_turnover.iloc[5:, 1:].values
            # 创建DataFrame并设置索引
            df_turn = pd.DataFrame(data, index=dates, columns=codes)
            # 移除无效日期的行
            df_turn = df_turn[df_turn.index.notna()]
            self.bond_filters_data["turnover"] = df_turn

            # 2. 加载评级 (信用评级)
            # Row 2 (index 2) is Codes. Data starts Row 4 (index 4). Date Col 2. Codes start Col 3.
            print("  加载评级数据...")
            df_rating = pd.read_excel(xls, sheet_name="信用评级", header=None)
            dates = pd.to_datetime(df_rating.iloc[4:, 2].tolist(), errors="coerce")
            codes = df_rating.iloc[2, 3:].tolist()
            data = df_rating.iloc[4:, 3:].values
            df_rate = pd.DataFrame(data, index=dates, columns=codes)
            df_rate = df_rate[df_rate.index.notna()]
            self.bond_filters_data["rating"] = df_rate

            # 3. 加载剩余期限
            # Row 0 (index 0) is Codes. Data starts Row 2 (index 2). Date Col 0. Codes start Col 1.
            print("  加载剩余期限数据...")
            df_term = pd.read_excel(xls, sheet_name="剩余期限", header=None)
            dates = pd.to_datetime(df_term.iloc[2:, 0].tolist(), errors="coerce")
            codes = df_term.iloc[0, 1:].tolist()
            data = df_term.iloc[2:, 1:].values
            df_t = pd.DataFrame(data, index=dates, columns=codes)
            df_t = df_t[df_t.index.notna()]
            self.bond_filters_data["term"] = df_t

            # 4. 加载未转股余额
            # Row 3 (index 3) is Codes. Data starts Row 5 (index 5). Date Col 0. Codes start Col 1.
            print("  加载未转股余额数据...")
            df_balance = pd.read_excel(xls, sheet_name="可转债余额", header=None)
            dates = pd.to_datetime(df_balance.iloc[5:, 0].tolist(), errors="coerce")
            codes = df_balance.iloc[3, 1:].tolist()
            data = df_balance.iloc[5:, 1:].values
            df_bal = pd.DataFrame(data, index=dates, columns=codes)
            df_bal = df_bal[df_bal.index.notna()]
            self.bond_filters_data["balance"] = df_bal

            # 5. 计算上市时间 (第一次有数据往后推4周)
            # 使用余额数据判断
            print("  计算上市时间限制...")
            listing_check = {}
            # 确保余额数据是数值型
            df_bal = df_bal.apply(pd.to_numeric, errors="coerce")

            for code in df_bal.columns:
                # 找到第一个非NaN且大于0的日期
                valid_series = df_bal[code]
                valid_indices = valid_series[valid_series.notna()].index
                if len(valid_indices) > 0:
                    first_date = valid_indices[0]
                    # 往后推4周 (28天)
                    listing_check[code] = first_date + timedelta(days=28)

            self.bond_filters_data["listing_check"] = listing_check

            print("  数据加载完成")

        except Exception as e:
            print(f"  警告: 加载转债筛选数据失败: {e}")
            import traceback

            traceback.print_exc()
            self.bond_filters_data = None

    def rating_to_numeric(self, rating):
        """
        将评级转换为数值，用于比较
        AA-及以上返回True
        """
        if pd.isna(rating) or rating == "" or rating == "-":
            return False

        rating = str(rating).strip().upper()

        # 定义评级顺序（从高到低）
        rating_order = {
            "AAA": 100,
            "AAA-": 95,
            "AA+": 90,
            "AA": 85,
            "AA-": 80,
            "A+": 70,
            "A": 65,
            "A-": 60,
            "BBB+": 50,
            "BBB": 45,
            "BBB-": 40,
        }

        # AA-及以上为True
        return rating_order.get(rating, 0) >= 80

    def filter_bonds(self, date, available_bonds):
        """
        根据5个条件筛选转债

        筛选条件：
        1. 流动性：过去一周成交额 > 500万
        2. 评级：AA-及以上
        3. 剩余期限：> 1年
        4. 未转股余额：> 3000万
        5. 上市时间：超过1个月

        参数:
            date: 当前日期
            available_bonds: 可用转债列表 (格式: ['110067.SH', '128145.SZ'])

        返回:
            符合条件的转债列表
        """
        # 如果没有筛选数据，返回所有转债
        if self.bond_filters_data is None:
            return available_bonds

        # 获取各个DataFrame
        df_turnover = self.bond_filters_data["turnover"]
        df_rating = self.bond_filters_data["rating"]
        df_term = self.bond_filters_data["term"]
        df_balance = self.bond_filters_data["balance"]
        listing_check = self.bond_filters_data["listing_check"]

        filtered_bonds = []

        # 辅助函数：获取指定日期（或之前最近日期）的行数据
        def get_latest_data(df, target_date):
            if target_date in df.index:
                return df.loc[target_date]

            # 找到之前最近的日期
            valid_dates = df.index[df.index <= target_date]
            if len(valid_dates) > 0:
                return df.loc[valid_dates[-1]]
            return None

        # 获取当期数据
        turnover_row = get_latest_data(df_turnover, date)
        rating_row = get_latest_data(df_rating, date)
        term_row = get_latest_data(df_term, date)
        balance_row = get_latest_data(df_balance, date)

        for bond_code in available_bonds:
            # 1. 上市时间：超过1个月 (4周)
            # date > listing_check[code]
            if bond_code in listing_check:
                if date <= listing_check[bond_code]:
                    continue  # 上市时间不足
            else:
                # 如果在筛选数据库中完全找不到该转债，说明数据缺失，剔除
                continue

            # 2. 流动性：过去一周成交额 > 500万 (0.05亿)
            if turnover_row is not None and bond_code in turnover_row:
                val = turnover_row[bond_code]
                try:
                    val = float(val)
                    if pd.isna(val) or val <= 0.05:  # 单位是亿
                        continue
                except:
                    continue
            else:
                continue  # 没有成交额数据

            # 3. 评级：AA-及以上
            if rating_row is not None and bond_code in rating_row:
                val = rating_row[bond_code]
                if not self.rating_to_numeric(val):
                    continue
            else:
                continue  # 没有评级数据

            # 4. 剩余期限：> 1年
            if term_row is not None and bond_code in term_row:
                val = term_row[bond_code]
                try:
                    val = float(val)
                    if pd.isna(val) or val <= 1.0:
                        continue
                except:
                    continue
            else:
                continue

            # 5. 未转股余额：> 3000万 (0.3亿)
            if balance_row is not None and bond_code in balance_row:
                val = balance_row[bond_code]
                try:
                    val = float(val)
                    if pd.isna(val) or val <= 0.3:  # 单位是亿
                        continue
                except:
                    continue
            else:
                continue

            # 通过所有筛选条件
            filtered_bonds.append(bond_code)

        return filtered_bonds

    def zscore_normalize(self, data):
        """截面Z-Score标准化"""
        mean = data.mean()
        std = data.std()
        if std == 0 or pd.isna(std):
            return data * 0  # 如果标准差为0，返回0
        return (data - mean) / std

    def calculate_rank_ic(self, date, next_date):
        """
        计算指定周期的 Rank IC
        """
        # 1. 获取当期因子值 (原始值)
        factor_values = {}

        # 优先使用预处理过的对齐因子
        if self.aligned_factors:
            for name, df in self.aligned_factors.items():
                if date in df.index:
                    factor_values[name] = df.loc[date]
        else:
            for name, df in self.factors.items():
                if date in df.index:
                    factor_values[name] = df.loc[date]
                else:
                    # Forward fill logic
                    valid_dates = df.index[df.index <= date]
                    if len(valid_dates) > 0:
                        factor_values[name] = df.loc[valid_dates[-1]]
                    else:
                        continue

        if not factor_values:
            return None

        factors_df = pd.DataFrame(factor_values)

        # 2. 获取下期收益率
        if date not in self.prices.index or next_date not in self.prices.index:
            return None

        p0 = self.prices.loc[date]
        p1 = self.prices.loc[next_date]
        # 计算收益率
        returns = (p1 - p0) / p0
        returns.name = "return"

        # 3. 合并数据
        # 确保只计算都有数据的转债
        data = pd.concat([factors_df, returns], axis=1).dropna()

        if len(data) < 10:  # 样本太少
            return None

        # 4. 计算 Rank IC (Spearman correlation)
        ic = {}
        for name in self.factors.keys():
            if name in data.columns:
                ic[name] = data[name].corr(data["return"], method="spearman")
            else:
                ic[name] = 0.0  # 缺失因子 IC 设为 0

        return pd.Series(ic, name=date)

    def calculate_dynamic_weights(self, date):
        """
        根据过去 6 个月的 IC 计算权重
        Weight_i = IC_i / sum(|IC_i|)
        """
        if self.ic_history_df is None or len(self.ic_history_df) == 0:
            return None

        # 筛选 date 之前的记录 (不包含 date 本身，因为 date 的 IC 要等到 next_date 才知道)
        # 注意：self.ic_history_df 的 index 是计算 IC 的起始日期
        past_ics = self.ic_history_df[self.ic_history_df.index < date]

        if len(past_ics) == 0:
            return None

        # 取最近 6 期
        recent_ics = past_ics.tail(6)

        # 计算平均 IC
        mean_ic = recent_ics.mean()

        # 计算权重
        abs_sum = mean_ic.abs().sum()

        if abs_sum == 0:
            return None

        weights = mean_ic / abs_sum
        return weights.to_dict()

    def calculate_combined_signal(self, date, weights=None):
        """
        在指定日期计算综合信号

        weights: 因子权重字典。如果为 None，使用等权 (旧逻辑)。
        """
        signals = {}

        # 获取各因子在该日期的数据
        for name, df in self.factors.items():
            if date in df.index:
                signals[name] = df.loc[date]
            else:
                # 如果该日期没有数据，使用最近的数据
                valid_dates = df.index[df.index <= date]
                if len(valid_dates) > 0:
                    nearest_date = valid_dates[-1]
                    signals[name] = df.loc[nearest_date]
                else:
                    signals[name] = pd.Series(dtype=float)

        # 对齐所有因子的代码
        all_codes = set()
        for s in signals.values():
            all_codes.update(s.index)

        # 创建对齐的DataFrame
        aligned_signals = pd.DataFrame(index=sorted(all_codes))
        for name, s in signals.items():
            aligned_signals[name] = s

        # Z-Score标准化
        normalized = aligned_signals.apply(self.zscore_normalize, axis=0)

        if weights is None:
            # --- 等权模式 (旧逻辑) ---
            # 方向调整
            # 估值因子取负（做多低估值）
            if "valuation" in normalized.columns:
                normalized["valuation"] = -normalized["valuation"]

            # 等权合成
            combined = normalized.mean(axis=1)
        else:
            # --- 动态权重模式 ---
            # 权重已包含方向 (IC正负)，直接加权
            combined = pd.Series(0.0, index=normalized.index)
            valid_weights = 0

            for name, w in weights.items():
                if name in normalized.columns:
                    # 处理可能的 NaN
                    s = normalized[name].fillna(0)
                    combined += s * w
                    valid_weights += abs(w)

            # 如果没有有效权重，返回全0
            if valid_weights == 0:
                combined = pd.Series(np.nan, index=normalized.index)

        return combined

    def calculate_benchmark_return(self, date, next_date):
        """
        计算基准收益率
        只使用 000832.CSI 指数数据
        """
        # 尝试使用指数数据
        if hasattr(self, "benchmark_prices") and self.benchmark_prices is not None:
            # 辅助函数：获取指定日期（或之前最近日期）的数据
            def get_price(target_date):
                if target_date in self.benchmark_prices.index:
                    return self.benchmark_prices.loc[target_date]
                # 找到之前最近的日期
                valid_dates = self.benchmark_prices.index[
                    self.benchmark_prices.index <= target_date
                ]
                if len(valid_dates) > 0:
                    return self.benchmark_prices.loc[valid_dates[-1]]
                return None

            p0 = get_price(date)
            p1 = get_price(next_date)

            if p0 is not None and p1 is not None and p0 != 0:
                return (p1 - p0) / p0

        return np.nan

    def calculate_portfolio_return(self, date, next_date, holdings):
        """
        计算组合收益率
        holdings: 持仓列表
        """
        if date not in self.prices.index or next_date not in self.prices.index:
            return np.nan

        if len(holdings) == 0:
            return 0.0

        # 过滤出在价格数据中存在的转债
        available_holdings = [h for h in holdings if h in self.prices.columns]

        if len(available_holdings) == 0:
            return np.nan

        price_t0 = self.prices.loc[date, available_holdings]
        price_t1 = self.prices.loc[next_date, available_holdings]

        # 计算收益率
        returns = (price_t1 - price_t0) / price_t0

        # 去除NaN后等权平均
        valid_returns = returns.dropna()
        if len(valid_returns) == 0:
            return np.nan

        return valid_returns.mean()

    def preprocess_factors(self):
        """
        预处理因子：对齐数据并预计算Z-Score
        """
        print("\n" + "=" * 60)
        print("步骤 2.5: 预处理因子 (对齐 & Z-Score)")
        print("=" * 60)

        if self.prices is None:
            raise ValueError("价格数据未加载，无法对齐因子")

        # 以价格数据的索引和列为基准
        target_index = self.prices.index
        target_columns = self.prices.columns

        for name, df in self.factors.items():
            print(f"  处理因子: {name}")
            # 1. 对齐 (Reindex)
            # 这里的 reindex 会自动引入 NaN
            aligned = df.reindex(index=target_index, columns=target_columns)

            # 2. 填充缺失值 (Forward Fill)
            aligned = aligned.ffill()

            self.aligned_factors[name] = aligned

            # 3. 截面 Z-Score (Vectorized)
            # axis=1 表示对每一行 (每个日期) 进行标准化
            mean = aligned.mean(axis=1)
            std = aligned.std(axis=1)

            # (df - mean) / std
            # 使用 sub 和 div 方法配合 axis=0 来对齐 index (Date)
            zscored = aligned.sub(mean, axis=0).div(std, axis=0)

            # 处理 inf 和 NaN (std=0 或 数据缺失)
            zscored = zscored.replace([np.inf, -np.inf], 0).fillna(0)

            self.normalized_factors[name] = zscored

        print("  因子预处理完成")

    def calculate_rolling_volatility(self):
        """
        计算所有转债的滚动波动率 (用于波动率截断)
        过去 20 个交易日
        """
        if self.prices is None:
            return None

        returns = self.prices.pct_change()
        # 20日滚动年化波动率
        vol = returns.rolling(window=20).std() * np.sqrt(252)
        return vol

    def calculate_benchmark_ma(self, window=60):
        """
        计算基准的移动平均线 (用于趋势开关)
        """
        if self.benchmark_prices is None:
            # 如果没有基准数据，使用价格平均值作为代理
            bench = self.prices.mean(axis=1)
        else:
            bench = self.benchmark_prices

        ma = bench.rolling(window=window).mean()
        return bench, ma

    def calculate_risk_parity_weights(self, window=252):
        """
        计算因子的风险平价权重
        权重 = (1/vol) / sum(1/vol)
        vol 为因子多头组合(Top 20%)的过去 window 日波动率
        """
        print("\n" + "=" * 60)
        print("步骤 2.6: 计算因子风险平价权重")
        print("=" * 60)

        if self.prices is None:
            return None

        bond_returns = self.prices.pct_change()
        factor_returns_dict = {}

        print("  计算各因子每日收益率...")
        for name, df in self.normalized_factors.items():
            # 获取该因子的方向
            # 估值因子是越小越好 (Min)，其他是越大越好 (Max)
            # 在 normalized_factors 中，我们之前没有对 valuation 取负
            # 所以在选股时，valuation 应该选最小的 (Rank 前 20%)
            # 其他选最大的 (Rank 后 20%)

            # 为了统一方便计算，我们先处理方向
            # 如果是 valuation，我们取负，这样就都变成越大越好了
            factor_data = df.copy()
            if name == "valuation":
                factor_data = -factor_data

            # 计算每日排名前 20% 的掩码
            # rank(pct=True)
            ranks = factor_data.rank(axis=1, pct=True)
            # Top 20% -> rank > 0.8
            top_mask = ranks > 0.8

            # 计算因子组合收益 (等权)
            # 每天的收益 = (当日收益 * 掩码).sum() / 掩码.sum()
            # 注意：aligned_factors 已经对齐了 prices，所以可以直接乘
            # 但 bond_returns 的 index 可能比 factors 少一天 (因为 pct_change)

            # 对齐数据
            common_idx = bond_returns.index.intersection(top_mask.index)
            b_ret = bond_returns.loc[common_idx]
            mask = top_mask.loc[common_idx]

            # 向量化计算每日平均收益
            # 这种写法处理 NaN 比较麻烦，使用 stack 可能更稳，但慢
            # 也可以用 .mean(axis=1) on masked array

            masked_ret = b_ret.where(mask)
            daily_ret = masked_ret.mean(axis=1)

            factor_returns_dict[name] = daily_ret

        factor_ret_df = pd.DataFrame(factor_returns_dict)

        # 计算滚动波动率
        print(f"  计算滚动波动率 (Window={window})...")
        rolling_vol = factor_ret_df.rolling(window=window).std()

        # 计算权重
        # 避免除以0
        inv_vol = 1.0 / rolling_vol.replace(0, np.nan)
        sum_inv_vol = inv_vol.sum(axis=1)

        weights = inv_vol.div(sum_inv_vol, axis=0)

        # 填充 NaN (比如前期数据不足) 为等权
        # 或者设为0? 设为等权比较合理
        n_factors = len(self.normalized_factors)
        weights = weights.fillna(1.0 / n_factors)

        print("  风险平价权重计算完成")
        return weights

    def get_combined_signal_fast(self, date, weights=None):
        """
        快速获取综合信号 (使用预计算的数据)
        """
        if date not in self.prices.index:
            return pd.Series(dtype=float)

        # 收集该日期的所有因子值
        signals = {}
        for name, df in self.normalized_factors.items():
            # 此时 df 已经是 reindex 过的，列与 self.prices.columns 一致
            signals[name] = df.loc[date]

        signal_df = pd.DataFrame(signals)

        # 处理因子方向 (Min is Good -> 取负)
        # 估值因子
        if "valuation" in signal_df.columns:
            signal_df["valuation"] = -signal_df["valuation"]

        # 计算综合得分
        if weights is None:
            # --- 等权模式 ---
            combined = signal_df.mean(axis=1)
        else:
            # --- 动态权重模式 ---
            combined = pd.Series(0.0, index=signal_df.index)
            valid_weights = 0

            for name, w in weights.items():
                if name in signal_df.columns:
                    # normalized_factors 已经处理过 NaN (fillna(0))
                    # signal_df 已经处理过方向 (取负)
                    s = signal_df[name]
                    combined += s * w
                    valid_weights += abs(w)

            if valid_weights == 0:
                combined = pd.Series(np.nan, index=signal_df.index)

        return combined

    def run_backtest(self, top_pct=0.2, bottom_pct=0.2):
        print("\n" + "=" * 60)
        print("步骤 3: 执行回测 (单因子与等权，含双边0.06%交易成本)")
        print("=" * 60)

        self.preprocess_factors()
        results = []
        
        prev_holdings = {"ew": set()}
        for f in self.normalized_factors.keys():
            prev_holdings[f] = set()

        for i in range(len(self.rebalance_dates) - 1):
            date = self.rebalance_dates.iloc[i]
            next_date = self.rebalance_dates.iloc[i + 1]

            def get_strategy_return(sig):
                if len(sig) == 0:
                    return np.nan, []
                sig_sorted = sig.sort_values(ascending=False)
                n_top = max(1, int(len(sig_sorted) * top_pct))
                longs = sig_sorted.head(n_top).index.tolist()
                ret = self.calculate_portfolio_return(date, next_date, longs)
                return ret, longs

            # 六因子等权
            signal_ew = self.get_combined_signal_fast(date, weights=None)
            signal_ew = signal_ew.dropna()
            signal_ew = signal_ew[signal_ew.index.isin(self.prices.columns)]
            eligible_bonds_ew = self.filter_bonds(date, signal_ew.index.tolist())
            signal_ew = signal_ew[signal_ew.index.isin(eligible_bonds_ew)]
            
            # 单因子测试
            factor_returns = {}
            for f in self.normalized_factors.keys():
                sig_f = self.get_combined_signal_fast(date, weights={f: 1.0})
                sig_f = sig_f.dropna()
                sig_f = sig_f[sig_f.index.isin(self.prices.columns)]
                el_f = self.filter_bonds(date, sig_f.index.tolist())
                sig_f = sig_f[sig_f.index.isin(el_f)]
                
                ret_f, longs_f = get_strategy_return(sig_f)
                
                # 双边0.06%对应单边0.0006
                turnover = len(set(longs_f) - prev_holdings[f]) / len(longs_f) if len(longs_f) > 0 else 0
                ret_f_gross = ret_f
                if not pd.isna(ret_f):
                    ret_f -= turnover * 0.0006
                prev_holdings[f] = set(longs_f)
                
                factor_returns[f"{f}_return"] = ret_f
                factor_returns[f"{f}_return_gross"] = ret_f_gross
                factor_returns[f"{f}_turnover"] = turnover

            long_ret_ew, longs_ew = get_strategy_return(signal_ew)
            turnover_ew = len(set(longs_ew) - prev_holdings["ew"]) / len(longs_ew) if len(longs_ew) > 0 else 0
            long_ret_ew_gross = long_ret_ew
            if not pd.isna(long_ret_ew):
                long_ret_ew -= turnover_ew * 0.0006
            prev_holdings["ew"] = set(longs_ew)

            benchmark_ret = self.calculate_benchmark_return(date, next_date)

            res_item = {
                "date": next_date,
                "rebalance_date": date,
                "benchmark_return": benchmark_ret,
                "long_return_ew": long_ret_ew,
                "long_return_ew_gross": long_ret_ew_gross,
                "ew_turnover": turnover_ew
            }
            res_item.update(factor_returns)
            results.append(res_item)

            if (i + 1) % 12 == 0:
                print(f"  已处理 {i + 1}/{len(self.rebalance_dates) - 1} 个再平衡期")

        self.results_df = pd.DataFrame(results)
        self.results_df = self.results_df.set_index("date")

        print(f"\n回测完成！共 {len(self.results_df)} 期")
        return self.results_df

    def calculate_metrics(self):
        print("\n" + "=" * 60)
        print("步骤 4: 计算累计净值")
        print("=" * 60)

        df = self.results_df
        df["benchmark_nav"] = (1 + df["benchmark_return"]).cumprod()
        df["long_nav"] = (1 + df["long_return_ew"]).cumprod()
        df["long_nav_gross"] = (1 + df["long_return_ew_gross"]).cumprod()
        
        for f in self.normalized_factors.keys():
            df[f"{f}_nav"] = (1 + df[f"{f}_return"]).cumprod()
            df[f"{f}_nav_gross"] = (1 + df[f"{f}_return_gross"]).cumprod()

    def plot_results(self, save_path=None):
        print("\n" + "=" * 60)
        print("步骤 5: 绘制三组曲线 (含费净值、无费净值、换手率)")
        print("=" * 60)

        import matplotlib.pyplot as plt
        import pandas as pd

        df = self.results_df
        plt.style.use("seaborn-v0_8-white" if "seaborn-v0_8-white" in plt.style.available else "seaborn-white")
        plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
        plt.rcParams["axes.unicode_minus"] = False

        factor_names_cn = {
            "valuation": "估值因子",
            "momentum": "动量因子",
            "price_volume": "量价相关性因子",
            "volatility": "波动率因子",
            "liquidity": "流动性因子",
            "bs_deviation": "BS定价偏差因子"
        }
        
        colors = {"valuation": "#0000FF",  # 蓝色
                  "momentum": "#008000",   # 绿色
                  "price_volume": "#FF0000", # 红色
                  "volatility": "#800080",   # 紫色
                  "liquidity": "#FFA500",    # 橙色
                  "bs_deviation": "#00FFFF"} # 青色

        # 1. 绘制含交易成本净值曲线
        fig1, ax1 = plt.subplots(figsize=(12, 6))
        lines1, labels1 = [], []
        
        for f in self.normalized_factors.keys():
            col_name = f"{f}_nav"
            if col_name in df.columns:
                c = colors.get(f, "#17becf")
                l, = ax1.plot(df.index, df[col_name], label=factor_names_cn.get(f, f), color=c, linewidth=1.5)
                lines1.append(l)
                labels1.append(factor_names_cn.get(f, f))
                
        l_ew1, = ax1.plot(df.index, df["long_nav"], label="五因子等权" if len(self.normalized_factors) == 5 else "六因子等权", color="brown", linewidth=2)
        lines1.append(l_ew1)
        labels1.append("五因子等权" if len(self.normalized_factors) == 5 else "六因子等权")
        
        l_bench1, = ax1.plot(df.index, df["benchmark_nav"], label="中证转债指数(基准)", color="black", linestyle="--", linewidth=1.5)
        lines1.append(l_bench1)
        labels1.append("中证转债指数(基准)")

        ax1.set_ylabel("累计净值 (含交易成本)")
        ax1.grid(True, linestyle="-", alpha=0.3)
        ax1.legend(handles=lines1, labels=labels1, loc="upper left", fontsize=10, frameon=True)
        plt.title("含交易成本净值曲线 (双边0.06%)", fontsize=14, pad=15)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"  含交易成本净值图已保存至: {save_path}")
        plt.close(fig1)

        # 2. 绘制无交易成本净值曲线
        fig2, ax2 = plt.subplots(figsize=(12, 6))
        lines2, labels2 = [], []
        
        for f in self.normalized_factors.keys():
            col_name = f"{f}_nav_gross"
            if col_name in df.columns:
                c = colors.get(f, "#17becf")
                l, = ax2.plot(df.index, df[col_name], label=factor_names_cn.get(f, f), color=c, linewidth=1.5)
                lines2.append(l)
                labels2.append(factor_names_cn.get(f, f))
                
        l_ew2, = ax2.plot(df.index, df["long_nav_gross"], label="五因子等权" if len(self.normalized_factors) == 5 else "六因子等权", color="brown", linewidth=2)
        lines2.append(l_ew2)
        labels2.append("五因子等权" if len(self.normalized_factors) == 5 else "六因子等权")
        
        l_bench2, = ax2.plot(df.index, df["benchmark_nav"], label="中证转债指数(基准)", color="black", linestyle="--", linewidth=1.5)
        lines2.append(l_bench2)
        labels2.append("中证转债指数(基准)")

        ax2.set_ylabel("累计净值 (无交易成本)")
        ax2.grid(True, linestyle="-", alpha=0.3)
        ax2.legend(handles=lines2, labels=labels2, loc="upper left", fontsize=10, frameon=True)
        plt.title("无交易成本净值曲线", fontsize=14, pad=15)
        plt.tight_layout()

        if save_path:
            nofee_path = str(save_path).replace(".png", "_nofee.png")
            plt.savefig(nofee_path, dpi=300, bbox_inches="tight")
            print(f"  无交易成本净值图已保存至: {nofee_path}")
        plt.close(fig2)

        # 3. 绘制组合换手率曲线
        fig3, ax3 = plt.subplots(figsize=(12, 6))
        lines3, labels3 = [], []
        
        for f in self.normalized_factors.keys():
            col_name = f"{f}_turnover"
            if col_name in df.columns:
                c = colors.get(f, "#17becf")
                l, = ax3.plot(df.index, df[col_name], label=factor_names_cn.get(f, f), color=c, linewidth=1.5, alpha=0.8)
                lines3.append(l)
                labels3.append(factor_names_cn.get(f, f))

        l_ew3, = ax3.plot(df.index, df["ew_turnover"], label="五因子等权" if len(self.normalized_factors) == 5 else "六因子等权", color="brown", linewidth=2)
        lines3.append(l_ew3)
        labels3.append("五因子等权" if len(self.normalized_factors) == 5 else "六因子等权")

        ax3.set_ylabel("换手率")
        ax3.grid(True, linestyle="-", alpha=0.3)
        ax3.legend(handles=lines3, labels=labels3, loc="upper left", fontsize=10, frameon=True)
        plt.title("组合换手率曲线", fontsize=14, pad=15)
        plt.tight_layout()

        if save_path:
            turnover_path = str(save_path).replace(".png", "_turnover.png")
            plt.savefig(turnover_path, dpi=300, bbox_inches="tight")
            print(f"  换手率曲线图已保存至: {turnover_path}")
        plt.close(fig3)

    def print_performance_summary(self):
        """
        计算并打印含交易费用下的策略性能指标 (仿照用户指定格式)
        列: 因子名称, 累计超额收益率, 年化超额收益率, 年化波动率, 夏普比率, 最大回撤, 平均换手率
        """
        print("\n" + "=" * 60)
        print("步骤 7: 策略性能指标统计 (含交易费用)")
        print("=" * 60)

        df = self.results_df
        if df is None or len(df) == 0:
            print("无回测结果数据")
            return

        factor_names_cn = {
            "valuation": "估值因子",
            "momentum": "动量因子",
            "price_volume": "量价相关性因子",
            "volatility": "波动率因子",
            "liquidity": "流动性因子",
            "bs_deviation": "BS定价偏差因子"
        }

        # 准备遍历的策略列表
        # 格式: (显示名称, 收益率列名, 换手率列名)
        strategy_list = []
        
        # 1. 各单因子
        for f in self.normalized_factors.keys():
            cn_name = factor_names_cn.get(f, f)
            strategy_list.append((cn_name, f"{f}_return", f"{f}_turnover"))
            
        # 2. 多因子等权
        strategy_list.append(("六因子等权合成因子", "long_return_ew", "ew_turnover"))

        # 获取基准收益率
        if "benchmark_return" not in df.columns:
            print("错误: 结果中缺少基准收益率数据")
            return
        
        bench_ret_series = df["benchmark_return"]
        
        # 计算基准的指标 (用于计算超额)
        start_date = self.rebalance_dates.iloc[0]
        end_date = df.index[-1]
        total_days = (end_date - start_date).days
        
        if len(df) > 1:
            avg_days = total_days / len(df)
            annual_factor = 365.25 / avg_days
        else:
            annual_factor = 12

        rf = 0.02  # 无风险利率

        def get_ann_ret(ret_series):
            nav = (1 + ret_series).cumprod()
            total = nav.iloc[-1] - 1
            if total_days > 0:
                return (1 + total) ** (365.25 / total_days) - 1
            return 0

        def get_total_ret(ret_series):
            nav = (1 + ret_series).cumprod()
            return nav.iloc[-1] - 1

        bench_ann_ret = get_ann_ret(bench_ret_series)
        bench_total_ret = get_total_ret(bench_ret_series)

        metrics_data = []

        for name, ret_col, turn_col in strategy_list:
            if ret_col not in df.columns:
                continue

            strat_ret_series = df[ret_col]
            
            # 1. 基础指标
            strat_total_ret = get_total_ret(strat_ret_series)
            strat_ann_ret = get_ann_ret(strat_ret_series)
            strat_vol = strat_ret_series.std() * np.sqrt(annual_factor)
            
            # 2. 超额指标 (算术差)
            cum_excess_ret = strat_total_ret - bench_total_ret
            ann_excess_ret = strat_ann_ret - bench_ann_ret
            
            # 3. 夏普比率 (基于策略总收益)
            if strat_vol != 0:
                sharpe = (strat_ann_ret - rf) / strat_vol
            else:
                sharpe = 0
                
            # 4. 最大回撤
            nav = (1 + strat_ret_series).cumprod()
            nav_with_start = pd.concat([pd.Series([1.0], index=[start_date]), nav])
            rolling_max = nav_with_start.cummax()
            drawdown = (nav_with_start - rolling_max) / rolling_max
            max_dd = drawdown.min()
            
            # 5. 平均换手率
            avg_turnover = df[turn_col].mean() if turn_col in df.columns else 0

            metrics_data.append({
                "因子名称": name,
                "累计超额收益率": f"{cum_excess_ret:.2%}",
                "年化超额收益率": f"{ann_excess_ret:.2%}",
                "年化波动率": f"{strat_vol:.2%}",
                "夏普比率": f"{sharpe:.2f}",
                "最大回撤": f"{max_dd:.2%}",
                "平均换手率": f"{avg_turnover:.2%}"
            })

        # 转为DataFrame
        metrics_df = pd.DataFrame(metrics_data)
        
        # 打印
        print(metrics_df.to_string(index=False))

        # 保存到CSV
        save_path = self.data_dir / "B-S_alpha_strategy_metrics.csv"
        metrics_df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"\n指标统计已保存至: {save_path}")

    def save_results(self, csv_path):
        print("\n" + "=" * 60)
        print("步骤 6: 保存回测结果")
        print("=" * 60)
        self.results_df.to_csv(csv_path, encoding="utf-8-sig")


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("6因子复合策略回测系统")  
    print("=" * 60)

    # 初始化
    data_dir = r"d:\Python\浙商证券固收\转债错误定价"
    backtest = MultiFactorBacktest(data_dir)

    # 加载数据
    backtest.load_data()

    # 检查因子相关性
    backtest.check_factor_correlation()

    # 加载转债筛选数据
    backtest.load_bond_filters_data()

    # 获取再平衡日期
    backtest.get_rebalance_dates()

    # 运行回测
    backtest.run_backtest(top_pct=0.2, bottom_pct=0.2)

    # 计算指标
    backtest.calculate_metrics()

    # 打印性能统计
    backtest.print_performance_summary()

    # 绘图
    backtest.plot_results(save_path=Path(data_dir) / "B-S_alpha_strategy_chart.png")

    # 保存结果
    backtest.save_results(Path(data_dir) / "B-S_alpha_strategy_results.csv")

    print("\n" + "=" * 60)
    print("回测完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
