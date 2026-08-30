import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

# 设置绘图风格
try:
    plt.style.use('seaborn-v0_8')
except Exception:
    plt.style.use('seaborn')
plt.rcParams['font.sans-serif'] = ['SimHei']  # 解决中文显示问题
plt.rcParams['axes.unicode_minus'] = False

# ── 路径解析: 输入文件优先在仓库内查找，兼容旧外部目录 ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
BACKTEST_DIR = os.path.join(REPO_ROOT, "backtest")
MIN_DAILY_TURNOVER_WAN = 500.0
MIN_OUTSTANDING_BALANCE_WAN = 3_000.0
sys.path.insert(0, BACKTEST_DIR)

from market_data_contracts import (  # noqa: E402
    observed_average_risk_free_rate,
)


def _resolve(filename, *dirs):
    """按候选目录顺序查找文件；找不到时返回首候选（让报错信息带出期望路径）。"""
    for d in dirs:
        p = os.path.join(d, filename)
        if os.path.exists(p):
            return p
    return os.path.join(dirs[0], filename)


def align_observed_strategy_inputs(
    *,
    common_index,
    ratings,
    remaining_term,
    balance,
    turnover,
    prices,
):
    """Align source matrices without manufacturing market observations."""

    frames = {
        "ratings": ratings,
        "remaining_term": remaining_term,
        "balance": balance,
        "turnover": turnover,
        "prices": prices,
    }
    missing = [name for name, frame in frames.items() if frame is None]
    if missing:
        raise ValueError(f"missing strategy source matrices: {missing}")
    return {
        name: frame.reindex(common_index)
        for name, frame in frames.items()
    }


def mark_missing_exit_prices(
    *,
    end_prices,
    observed_daily_prices,
    held_codes,
    start_date,
    end_date,
):
    """Mark missing holding exits to the last observed close in the period."""

    marked = end_prices.copy()
    missing_codes = [
        code
        for code in held_codes
        if code in marked.index and pd.isna(marked.loc[code])
    ]
    if not missing_codes or observed_daily_prices is None:
        return marked

    available_codes = [
        code for code in missing_codes if code in observed_daily_prices.columns
    ]
    if not available_codes:
        return marked

    period = observed_daily_prices.loc[
        (observed_daily_prices.index >= start_date)
        & (observed_daily_prices.index <= end_date),
        available_codes,
    ]
    for code in available_codes:
        observed = period[code].dropna()
        if not observed.empty:
            marked.loc[code] = observed.iloc[-1]
    return marked


class CBStrategy:
    def __init__(self, data_dir=None):
        # data_dir 显式给定时沿用旧行为；默认在仓库内解析（模型/特征在 backtest/，基准在本目录）
        if data_dir:
            self.model_dir = data_dir
            self.bench_file = os.path.join(data_dir, "000832_CSI_close_price.csv")
        else:
            self.model_dir = BACKTEST_DIR
            self.bench_file = _resolve(
                "000832_CSI_close_price.csv", SCRIPT_DIR
            )
        self.select_ratio = 0.2  # 筛选比例：Top 20% 做多，Bottom 20% 做空
        
        # 数据存储
        self.relative_deviation = None
        self.ratings = None
        self.remaining_term = None
        self.balance = None
        self.turnover = None
        self.prices = None
        self.observed_daily_prices = None
        self.listing_dates = None
        self.returns_data = None
        self.benchmark_prices = None
        self.benchmark_returns_daily = None
        self.risk_free_curve = None
        self.results = {}
        
        self.rating_map = {'AAA': 4, 'AA+': 3, 'AA': 2, 'AA-': 1}
        
    def _clean_ts_data(self, df):
        date_col = df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        start_idx = df[date_col].first_valid_index()
        if start_idx is None:
            return None
        clean_df = df.iloc[start_idx:].copy()
        clean_df.set_index(date_col, inplace=True)
        # 确保所有列为数值型
        clean_df = clean_df.apply(pd.to_numeric, errors='coerce')
        # 移除全空行
        clean_df = clean_df.dropna(how='all')
        return clean_df

    def load_data(self, model_file_name="ZL_Model_Summary.xlsx"):
        print(f"开始加载数据 (模型文件: {model_file_name})... ")
        model_path = os.path.join(self.model_dir, model_file_name)
        
        # 1. 加载相对偏差数据
        print(f"加载相对偏差数据: {model_file_name}")
        df_rd = pd.read_excel(model_path, sheet_name='相对偏差')
        self.relative_deviation = self._clean_ts_data(df_rd)
        
        # 2. 加载特征数据库 (自 CSV 缓存)
        print("加载特征数据库 (CSV 缓存)...")

        def _load_csv_cache(filename, name):
            path = _resolve(filename, self.model_dir, BACKTEST_DIR)
            if not os.path.exists(path):
                print(f"  错误: 未找到 {name} 缓存文件 {path}")
                return None
            print(f"  加载 {name} 数据 ({os.path.basename(path)})...")
            df = pd.read_csv(path, index_col=0)
            df.index = pd.to_datetime(df.index, errors="coerce")
            df = df[df.index.notna()]
            valid_cols = [c for c in df.columns if str(c).endswith((".SH", ".SZ"))]
            return df[valid_cols]

        df_rating = _load_csv_cache("cb_rating_cache.csv", "信用评级")
        if df_rating is not None:
            self.ratings = df_rating
            if hasattr(self.ratings, "map"):
                self.ratings = self.ratings.map(lambda x: self.rating_map.get(str(x).strip(), 0))
            else:
                self.ratings = self.ratings.applymap(lambda x: self.rating_map.get(str(x).strip(), 0))

        self.remaining_term = _load_csv_cache("cb_maturity_cache.csv", "剩余期限")
        self.balance = _load_csv_cache("cb_balance_cache.csv", "可转债余额")
        self.turnover = _load_csv_cache("cb_amount_cache.csv", "可转债交易额")
        self.prices = _load_csv_cache("cb_price_cache.csv", "可转债价格")
        self.observed_daily_prices = self.prices.copy()
        risk_free_path = _resolve(
            "rf_yield_cache.csv", self.model_dir, BACKTEST_DIR
        )
        if not os.path.exists(risk_free_path):
            raise FileNotFoundError(
                f"缺少 AkShare 国债收益率缓存: {risk_free_path}"
            )
        self.risk_free_curve = pd.read_csv(
            risk_free_path,
            index_col=0,
            parse_dates=True,
        )

        # 计算 listing_dates
        if self.balance is not None:
            df_bal_num = self.balance.apply(pd.to_numeric, errors="coerce")
            listing_dict = {}
            for code in df_bal_num.columns:
                valid_series = df_bal_num[code]
                valid_indices = valid_series[valid_series.notna() & (valid_series > 0)].index
                if len(valid_indices) > 0:
                    listing_dict[code] = valid_indices[0]
            self.listing_dates = pd.Series(listing_dict)

        # 3. 从本地文件加载 000832.CSI 中证转债指数
        print("从本地 CSV 加载中证转债指数 (000832_CSI_close_price.csv)...")
        try:
            bench_file = self.bench_file
            if not os.path.exists(bench_file):
                raise FileNotFoundError(f"未找到基准文件: {bench_file}")

            df_bench = pd.read_csv(bench_file)
            if df_bench is not None and not df_bench.empty:
                if "Date" not in df_bench.columns or "close" not in df_bench.columns:
                    df_bench.columns = [str(c).strip() for c in df_bench.columns]
                    if "Date" not in df_bench.columns or "close" not in df_bench.columns:
                        raise ValueError(f"基准文件格式错误，缺少 Date 或 close 列。现有列: {df_bench.columns.tolist()}")

                df_bench["Date"] = pd.to_datetime(df_bench["Date"])
                df_bench.set_index("Date", inplace=True)
                df_bench.sort_index(inplace=True)

                self.benchmark_prices = df_bench["close"]
                self.benchmark_returns_daily = self.benchmark_prices.pct_change(
                    fill_method=None
                )
                print("本地基准数据加载成功。")
            else:
                raise ValueError("本地基准数据为空")
        except Exception as e:
            print(f"从本地加载基准数据失败: {e}")
            self.benchmark_returns_daily = None

        print("数据加载与清洗完成。")
        
        # 统一索引并填充缺失值
        print("对齐数据索引...")
        common_index = self.relative_deviation.index
        aligned = align_observed_strategy_inputs(
            common_index=common_index,
            ratings=self.ratings,
            remaining_term=self.remaining_term,
            balance=self.balance,
            turnover=self.turnover,
            prices=self.prices,
        )
        self.ratings = aligned["ratings"]
        self.remaining_term = aligned["remaining_term"]
        self.balance = aligned["balance"]
        self.turnover = aligned["turnover"]
        self.prices = aligned["prices"]
        
        # 加载周涨跌幅作为收益率参考 (可选，如果价格数据不准)
        # 发现“可转债周涨跌幅”数据在2025年后存在严重异常（如800%收益或-600%收益），因此禁用该数据源，改用价格计算
        self.returns_data = None
        # try:
        #     df_ret_sheet = xl_feat.parse('可转债周涨跌幅', header=None)
        #     codes_ret = df_ret_sheet.iloc[0, 1:].values
        #     dates_ret = pd.to_datetime(df_ret_sheet.iloc[3:, 0], errors='coerce')
        #     data_ret = df_ret_sheet.iloc[3:, 1:] / 100.0 # 转换为小数
        #     # 清洗异常数据：收益率不能小于 -1 (即跌幅不能超过 100%)
        #     data_ret = data_ret.where(data_ret >= -1, np.nan)
        #     # 清洗异常数据：收益率不能大于 1 (即涨幅不能超过 100%，防止数据错误导致的爆仓)
        #     data_ret = data_ret.where(data_ret <= 1, np.nan)
        #     self.returns_data = pd.DataFrame(data_ret.values, index=dates_ret, columns=codes_ret)
        #     self.returns_data = self.returns_data.reindex(common_index).fillna(0)
        #     print("成功加载周涨跌幅数据。")
        # except Exception as e:
        #     print(f"未能加载周涨跌幅数据: {e}，将继续使用价格计算收益。")
        #     self.returns_data = None

        print("数据对齐完成。")

    def get_first_layer_universe(self, date):
        """
        实现第一层硬约束筛选:
        1. 流动性: 日成交额 > 500万元
        2. 评级: >= AA- (映射值为1)
        3. 剩余期限: > 1年
        4. 未转股余额: > 3000万元
        5. 上市时间: > 1个月 (30天)
        """
        # 获取该日期的截面数据
        try:
            r_at_date = self.ratings.loc[date]
            t_at_date = self.remaining_term.loc[date]
            b_at_date = self.balance.loc[date]
            v_at_date = self.turnover.loc[date]
        except KeyError:
            return pd.Index([])

        # 1. 评级筛选
        valid_rating = r_at_date[r_at_date >= 1].index
        
        # 2. 期限筛选
        valid_term = t_at_date[t_at_date > 1].index
        
        # 3. 余额筛选（Tushare cb_share，经管道统一为万元）
        valid_balance = b_at_date[
            b_at_date > MIN_OUTSTANDING_BALANCE_WAN
        ].index
        
        # 4. 流动性筛选（Tushare cb_daily.amount，单位万元）
        valid_turnover = v_at_date[
            v_at_date > MIN_DAILY_TURNOVER_WAN
        ].index
        
        # 5. 上市时间筛选
        valid_listing = self.listing_dates[date - self.listing_dates > pd.Timedelta(days=30)].index
        
        # 取五项硬约束的交集
        universe = valid_rating.intersection(valid_term) \
                              .intersection(valid_balance) \
                              .intersection(valid_turnover) \
                              .intersection(valid_listing)
        
        # 确保在相对偏差和价格数据中也存在
        universe = universe.intersection(self.relative_deviation.columns) \
                          .intersection(self.prices.columns)
        
        return universe

    def run_backtest(self):
        print("开始回测 (多空原始版)...")
        # 筛选 2019 年及以后的日期
        # 使用价格数据的日期来确定月末 (与 B-S 模型保持一致)
        # 必须确保所选日期在 relative_deviation 和 prices 中都有数据 (或至少 prices 有)
        # 但为了对齐，我们先取交集
        valid_dates = sorted(self.relative_deviation.index.intersection(self.prices.index))
        valid_dates_series = pd.Series(valid_dates)
        
        # 按年月分组，取最后一个日期 (月末)
        monthly_last = valid_dates_series.groupby([valid_dates_series.dt.year, valid_dates_series.dt.month]).last()
        self.rebalance_dates = monthly_last[monthly_last.dt.year >= 2019].tolist()
        
        portfolio_returns = []
        portfolio_long_returns = []
        portfolio_short_returns = []
        benchmark_returns_list = []
        actual_rebalance_dates = []
        operations = []
        
        for i in range(len(self.rebalance_dates) - 1):
            date = self.rebalance_dates[i]
            next_date = self.rebalance_dates[i+1]
            
            # 第一层约束筛选
            universe = self.get_first_layer_universe(date)
            
            if i % 12 == 0:
                print(f"调仓日期: {date.date()}, 第一层硬约束筛选后标的数量: {len(universe)}")
            
            strategy_ret = np.nan
            long_ret = np.nan
            short_ret = np.nan
            bench_ret = np.nan
            
            if len(universe) > 10: 
                # 2. 估值与相对偏差筛选
                rd_series = self.relative_deviation.loc[date, universe].dropna().sort_values()
                if len(rd_series) >= 5:
                    n_select = max(1, int(len(rd_series) * self.select_ratio))
                    
                    # 修改为：做多模型高估 (RD最高，即模型价远高于市场价)，做空模型低估 (RD最低)
                    current_long = rd_series.tail(n_select).index.tolist()
                    current_short = rd_series.head(n_select).index.tolist()
                    
                    operations.append({
                        'date': date,
                        'long': current_long,
                        'short': current_short
                    })
                    
                    # 3. 计算收益 (等权 100% Long - 100% Short)
                    if self.returns_data is not None:
                        mask = (self.returns_data.index > date) & (self.returns_data.index <= next_date)
                        if mask.any():
                            # 累积该时段内的收益
                            l_rets_raw = (1 + self.returns_data.loc[mask, current_long]).prod() - 1
                            s_rets_raw = (1 + self.returns_data.loc[mask, current_short]).prod() - 1
                            
                            long_ret = l_rets_raw.mean()
                            short_ret = s_rets_raw.mean()
                            strategy_ret = long_ret - short_ret
                        else:
                            strategy_ret = np.nan
                    else:
                        p_start = self.prices.loc[date]
                        p_end = self.prices.loc[next_date]
                        p_end = mark_missing_exit_prices(
                            end_prices=p_end,
                            observed_daily_prices=self.observed_daily_prices,
                            held_codes=current_long + current_short,
                            start_date=date,
                            end_date=next_date,
                        )
                        valid_long = [c for c in current_long if pd.notnull(p_start[c]) and pd.notnull(p_end[c])]
                        valid_short = [c for c in current_short if pd.notnull(p_start[c]) and pd.notnull(p_end[c])]
                        if (
                            len(valid_long) == len(current_long)
                            and len(valid_short) == len(current_short)
                        ):
                            l_ret = (p_end[valid_long] / p_start[valid_long] - 1).mean()
                            s_ret = (p_end[valid_short] / p_start[valid_short] - 1).mean()
                            long_ret = l_ret
                            short_ret = s_ret
                            strategy_ret = l_ret - s_ret
                else:
                    print(f"警告: {date.date()} 候选池有效标的不足。")
            else:
                if i % 12 == 0:
                    print(f"警告: {date.date()} 第一层约束后候选池过小 ({len(universe)})。")
                
            if not np.isfinite(strategy_ret):
                raise RuntimeError(
                    f"strategy return unavailable for {date.date()} -> "
                    f"{next_date.date()}"
                )
            portfolio_returns.append(strategy_ret)
            portfolio_long_returns.append(long_ret)
            portfolio_short_returns.append(-short_ret)
            
            # 基准收益：使用价格直接计算，逻辑与 B-S 模型保持一致
            if self.benchmark_prices is None:
                raise RuntimeError("benchmark return unavailable: no benchmark")
            if (
                date not in self.benchmark_prices.index
                or next_date not in self.benchmark_prices.index
            ):
                raise RuntimeError(
                    "benchmark return unavailable: exact rebalance date missing"
                )
            p0 = self.benchmark_prices.loc[date]
            p1 = self.benchmark_prices.loc[next_date]
            if pd.isna(p0) or pd.isna(p1) or p0 == 0:
                raise RuntimeError(
                    "benchmark return unavailable: invalid benchmark price"
                )
            bench_ret = (p1 - p0) / p0
            benchmark_returns_list.append(bench_ret)
            actual_rebalance_dates.append(next_date)

                
        # 计算累计收益
        cum_strategy = (1 + pd.Series(portfolio_returns)).cumprod() - 1
        cum_long = (1 + pd.Series(portfolio_long_returns)).cumprod() - 1
        cum_short = (1 + pd.Series(portfolio_short_returns)).cumprod() - 1
        cum_benchmark = (1 + pd.Series(benchmark_returns_list)).cumprod() - 1
        
        full_dates = [self.rebalance_dates[0]] + actual_rebalance_dates
        cum_strategy = pd.Series([0.0] + list(cum_strategy), index=full_dates)
        cum_long = pd.Series([0.0] + list(cum_long), index=full_dates)
        cum_short = pd.Series([0.0] + list(cum_short), index=full_dates)
        cum_benchmark = pd.Series([0.0] + list(cum_benchmark), index=full_dates)
        
        self.results = {
            'strategy_cum_ret': cum_strategy,
            'long_cum_ret': cum_long,
            'short_cum_ret': cum_short,
            'benchmark_cum_ret': cum_benchmark,
            'strategy_monthly_returns': pd.Series(portfolio_returns, index=actual_rebalance_dates),
            'long_monthly_returns': pd.Series(portfolio_long_returns, index=actual_rebalance_dates),
            'short_monthly_returns': pd.Series(portfolio_short_returns, index=actual_rebalance_dates),
            'benchmark_monthly_returns': pd.Series(benchmark_returns_list, index=actual_rebalance_dates),
            'operations': operations
        }
        print("回测完成。")

    def analyze_results(self, plot=True):
        s_rets = self.results['strategy_monthly_returns']
        l_rets = self.results['long_monthly_returns']
        sh_rets = self.results['short_monthly_returns']
        b_rets = self.results['benchmark_monthly_returns']
        
        s_cum = self.results['strategy_cum_ret']
        l_cum = self.results['long_cum_ret']
        sh_cum = self.results['short_cum_ret']
        b_cum = self.results['benchmark_cum_ret']
        if self.risk_free_curve is None:
            raise ValueError("缺少 AkShare 国债收益率曲线，无法计算夏普比率")
        risk_free_rate = observed_average_risk_free_rate(
            curve=self.risk_free_curve,
            start=self.rebalance_dates[0],
            end=max(s_cum.index),
            tenor_years=1.0,
        )
        
        def get_metrics(r, c):
            # 1. 年化收益率 (使用实际日历天数计算，更准确)
            # 使用 self.rebalance_dates[0] 作为起始日期，确保包含第一期的时间
            if hasattr(self, 'rebalance_dates') and len(self.rebalance_dates) > 0:
                start_date = self.rebalance_dates[0]
                days = (c.index[-1] - start_date).days
            else:
                days = (c.index[-1] - c.index[0]).days
                
            if days <= 0:
                return 0, 0, 0, 0
            ann_ret = (1 + c.iloc[-1]) ** (365.25 / days) - 1
            
            # 2. 年化波动率 (基于月度收益率序列)
            ann_vol = r.std() * np.sqrt(12)
            
            # 3. 夏普比率（使用回测期 AkShare 1Y 国债收益率均值）
            sharpe = (
                (ann_ret - risk_free_rate) / ann_vol
                if ann_vol != 0
                else 0
            )
            
            # 4. 最大回撤
            roll_max = (1 + c).cummax()
            dd = (1 + c) / roll_max - 1
            mdd = dd.min()
            return ann_ret, ann_vol, sharpe, mdd

        s_ann, s_vol, s_sharpe, s_mdd = get_metrics(s_rets, s_cum)
        l_ann, l_vol, l_sharpe, l_mdd = get_metrics(l_rets, l_cum)
        sh_ann, sh_vol, sh_sharpe, sh_mdd = get_metrics(sh_rets, sh_cum)
        b_ann, b_vol, b_sharpe, b_mdd = get_metrics(b_rets, b_cum)
        
        # 结果打包返回用于比较
        metrics = {
            'strategy': (s_ann, s_vol, s_sharpe, s_mdd),
            'long': (l_ann, l_vol, l_sharpe, l_mdd),
            'short': (sh_ann, sh_vol, sh_sharpe, sh_mdd),
            'benchmark': (b_ann, b_vol, b_sharpe, b_mdd)
        }
        
        print("\n" + "="*80)
        print(f"{'指标':<10} | {'策略(多空)':<10} | {'多头':<10} | {'空头':<10} | {'基准(000832)':<10}")
        print("-" * 80)
        print(f"{'年化收益':<10} | {s_ann:>10.2%} | {l_ann:>10.2%} | {sh_ann:>10.2%} | {b_ann:>10.2%}")
        print(f"{'年化波动':<10} | {s_vol:>10.2%} | {l_vol:>10.2%} | {sh_vol:>10.2%} | {b_vol:>10.2%}")
        print(f"{'夏普比率':<10} | {s_sharpe:>10.2f} | {l_sharpe:>10.2f} | {sh_sharpe:>10.2f} | {b_sharpe:>10.2f}")
        print(f"{'最大回撤':<10} | {s_mdd:>10.2%} | {l_mdd:>10.2%} | {sh_mdd:>10.2%} | {b_mdd:>10.2%}")
        print("="*80)
        
        # 输出操作记录到CSV
        ops_df = []
        for op in self.results['operations']:
            date = op['date'].date()
            for bond_code in op['long']:
                ops_df.append(
                    {'日期': date, '方向': '做多', '标的': bond_code}
                )
            for bond_code in op['short']:
                ops_df.append(
                    {'日期': date, '方向': '做空', '标的': bond_code}
                )
        
        pd.DataFrame(ops_df).to_csv(os.path.join(SCRIPT_DIR, 'strategy_operations.csv'), index=False, encoding='utf_8_sig')
        print("调仓记录已保存为 strategy_operations.csv")
        
        # 计算累计超额收益
        excess_cum = (1 + s_cum) / (1 + b_cum) - 1

        if plot:
            # 绘图
            plt.figure(figsize=(12, 6))
            plt.plot(s_cum, label='多空策略')
            plt.plot(l_cum, label='多头策略')
            plt.plot(sh_cum, label='空头策略')
            plt.plot(b_cum, label='基准 (中证转债)')
            plt.title('转债多空策略累计收益率')
            plt.xlabel('日期')
            plt.ylabel('累计收益率')
            plt.legend()
            plt.grid(True)
            plt.savefig(os.path.join(SCRIPT_DIR, 'strategy_performance.png'))
            print("\n收益率曲线已保存为 strategy_performance.png")
            # plt.show()
        
        return {
            'metrics': metrics,
            'data': {
                'strategy_cum': s_cum,
                'long_cum': self.results['long_cum_ret'],
                'short_cum': self.results['short_cum_ret'],
                'benchmark_cum': b_cum,
                'excess_cum': excess_cum
            }
        }

if __name__ == "__main__":
    models = [
        "BS_Model_Summary.xlsx",
        "ZL_Model_Summary.xlsx",
        "LSM_Model_Summary.xlsx",
    ]
    all_metrics = {}
    all_data = {}

    for model in models:
        print(f"\n\n{'#'*20} 正在运行模型: {model} {'#'*20}")
        strategy = CBStrategy()
        strategy.load_data(model_file_name=model)
        strategy.run_backtest()
        results = strategy.analyze_results(plot=False)
        metrics = results['metrics']
        all_metrics[model] = metrics
        benchmark_metrics = metrics['benchmark'] # 基准数据理论上两个模型跑出来是一样的
        all_data[model] = results['data']
    
    # 打印最终对比表
    print("\n\n" + "="*128)
    print(f"{'模型对比汇总':^128}")
    print("="*128)
    print(f"{'指标':<8} | {'BS多空':<8} | {'BS多头':<8} | {'BS空头':<8} | {'ZL多空':<8} | {'ZL多头':<8} | {'ZL空头':<8} | {'LSM多空':<8} | {'LSM多头':<8} | {'LSM空头':<8} | {'基准':<8}")
    print("-" * 128)
    
    m_names = ['年化收益', '年化波动', '夏普比率', '最大回撤']
    for i, name in enumerate(m_names):
        bs_s = all_metrics["BS_Model_Summary.xlsx"]['strategy'][i]
        bs_l = all_metrics["BS_Model_Summary.xlsx"]['long'][i]
        bs_sh = all_metrics["BS_Model_Summary.xlsx"]['short'][i]
        zl_s = all_metrics["ZL_Model_Summary.xlsx"]['strategy'][i]
        zl_l = all_metrics["ZL_Model_Summary.xlsx"]['long'][i]
        zl_sh = all_metrics["ZL_Model_Summary.xlsx"]['short'][i]
        lsm_s = all_metrics["LSM_Model_Summary.xlsx"]['strategy'][i]
        lsm_l = all_metrics["LSM_Model_Summary.xlsx"]['long'][i]
        lsm_sh = all_metrics["LSM_Model_Summary.xlsx"]['short'][i]
        bm_val = benchmark_metrics[i]
        
        if name in ['年化收益', '年化波动', '最大回撤']:
            print(f"{name:<8} | {bs_s:>8.2%} | {bs_l:>8.2%} | {bs_sh:>8.2%} | {zl_s:>8.2%} | {zl_l:>8.2%} | {zl_sh:>8.2%} | {lsm_s:>8.2%} | {lsm_l:>8.2%} | {lsm_sh:>8.2%} | {bm_val:>8.2%}")
        else:
            print(f"{name:<8} | {bs_s:>8.2f} | {bs_l:>8.2f} | {bs_sh:>8.2f} | {zl_s:>8.2f} | {zl_l:>8.2f} | {zl_sh:>8.2f} | {lsm_s:>8.2f} | {lsm_l:>8.2f} | {lsm_sh:>8.2f} | {bm_val:>8.2f}")
    print("="*128)

    # === 分别绘制每个模型的明细图（总、多头、空头、基准） ===
    print("\n开始绘制各模型明细对比图...")
    
    # 设置绘图风格
    plt.style.use(
        "seaborn-v0_8-white"
        if "seaborn-v0_8-white" in plt.style.available
        else "seaborn-white"
    )
    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
    plt.rcParams["axes.unicode_minus"] = False

    # 获取基准数据 (取第一个模型的)
    first_model = models[0]
    b_cum = all_data[first_model]['benchmark_cum']
    
    model_labels = {
        'BS_Model_Summary.xlsx': 'BS',
        'ZL_Model_Summary.xlsx': 'ZL',
        'LSM_Model_Summary.xlsx': 'LSM',
    }

    for model in models:
        fig, ax = plt.subplots(figsize=(14, 8))
        label_prefix = model_labels.get(model, model.split('_')[0])
        
        # 1. 绘制基准净值
        ax.plot(b_cum.index, b_cum, color='#000000', linestyle=':', label='基准收益率 (000832)', linewidth=2)
        
        # 2. 绘制该模型总策略净值、多头净值、空头净值
        s_cum = all_data[model]['strategy_cum']
        l_cum = all_data[model]['long_cum']
        sh_cum = all_data[model]['short_cum']
        
        ax.plot(s_cum.index, s_cum, color='#D32F2F', linestyle='-', label=f'{label_prefix} 多空收益率', linewidth=2.5)
        ax.plot(l_cum.index, l_cum, color='#1976D2', linestyle='-', label=f'{label_prefix} 多头收益率', linewidth=2)
        ax.plot(sh_cum.index, sh_cum, color='#388E3C', linestyle='-', label=f'{label_prefix} 空头收益率', linewidth=2)
        
        ax.set_xlabel('年份', fontsize=15)
        ax.set_ylabel('累计收益率', fontsize=15)
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.tick_params(axis="y", labelsize=13)
        ax.tick_params(axis="x", labelsize=13)
        
        # 3. 绘制多空净值的超额收益 (策略总净值 - 基准净值) 到右轴
        ax2 = ax.twinx()
        # 由于 b_cum 和 s_cum 结构一致，直接相减
        # 我们用 (1+strategy)/(1+benchmark) - 1 来算百分比超额
        e_cum = (1 + s_cum) / (1 + b_cum) - 1
        e_cum_pct = e_cum * 100
        ax2.plot(e_cum_pct.index, e_cum_pct, color='#8E24AA', linestyle='-.', label=f'{label_prefix} 策略超额收益（右轴）', linewidth=2, alpha=0.8)
        ax2.set_ylabel('累计超额 (%)', fontsize=15)
        ax2.tick_params(axis="y", labelsize=13)

        # 4. 设置坐标轴刻度与范围，确保网格对齐
        # 左轴配置：起始 -0.5，间隔 0.5
        l_base = -0.5
        l_step = 0.5
        # 获取左轴数据范围 (最大值)
        v1_max = max(b_cum.max(), s_cum.max(), l_cum.max(), sh_cum.max())
        
        # 右轴配置：起始 -10，间隔 20
        r_base = -20
        r_step = 20
        # 获取右轴数据范围 (最大值)
        v2_max = e_cum_pct.max()
        
        # 计算需要的步数 (向上取整)
        steps_1 = np.ceil((v1_max - l_base) / l_step)
        steps_2 = np.ceil((v2_max - r_base) / r_step)
        max_steps = int(max(steps_1, steps_2)) 
        
        # 如果最高点离数据太近，可以再加一格
        # 这里直接使用计算出的步数，通常 ceil 已经保证了 > max
        
        # 生成刻度
        l_ticks = [l_base + i * l_step for i in range(max_steps + 1)]
        r_ticks = [r_base + i * r_step for i in range(max_steps + 1)]
        
        ax.set_yticks(l_ticks)
        ax.set_ylim(l_ticks[0], l_ticks[-1])
        
        ax2.set_yticks(r_ticks)
        ax2.set_ylim(r_ticks[0], r_ticks[-1])
        
        # 合并两个轴的图例
        lines_1, labels_1 = ax.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax.legend(
            lines_1 + lines_2,
            labels_1 + labels_2,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.05),
            ncol=5,
            fontsize=13,
            frameon=True,
            facecolor="white",
            edgecolor="lightgray",
            columnspacing=3,  # 增加列间距
            handletextpad=0.5 # 增加图标与文字间距
        )
        
        plt.title(f'{label_prefix} 模型策略效果：总净值、多空与基准超额', fontsize=17, fontweight="bold", pad=25)
        plt.tight_layout()
        
        save_name = os.path.join(SCRIPT_DIR, f'{label_prefix}_model_performance.png')
        plt.savefig(save_name, dpi=300, bbox_inches="tight")
        print(f"{label_prefix} 模型对比图已保存为 {save_name}")
