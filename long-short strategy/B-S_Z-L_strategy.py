import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.ticker as ticker

# 设置绘图风格
try:
    plt.style.use('seaborn-v0_8')
except:
    plt.style.use('seaborn')
plt.rcParams['font.sans-serif'] = ['SimHei']  # 解决中文显示问题
plt.rcParams['axes.unicode_minus'] = False

class CBStrategy:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.features_file = os.path.join(data_dir, "【浙商固收】转债资产端特征数据库【周更新外发】.xlsx")
        self.select_ratio = 0.2  # 筛选比例：Top 20% 做多，Bottom 20% 做空
        
        # 数据存储
        self.relative_deviation = None
        self.ratings = None
        self.remaining_term = None
        self.balance = None
        self.turnover = None
        self.prices = None
        self.listing_dates = None
        self.returns_data = None
        self.benchmark_prices = None
        self.benchmark_returns_daily = None
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
        model_path = os.path.join(self.data_dir, model_file_name)
        
        # 1. 加载相对偏差数据
        print(f"加载相对偏差数据: {model_file_name}")
        df_rd = pd.read_excel(model_path, sheet_name='相对偏差')
        self.relative_deviation = self._clean_ts_data(df_rd)
        
        # 2. 加载特征数据库
        print("加载特征数据库...")
        xl_feat = pd.ExcelFile(self.features_file)
        
        # 评级 (Row 2 codes, Row 4+ dates/data)
        df_rating = xl_feat.parse('信用评级', header=None)
        codes_rating = df_rating.iloc[2, 3:].values
        dates_rating = pd.to_datetime(df_rating.iloc[4:, 2], errors='coerce')
        data_rating = df_rating.iloc[4:, 3:]
        self.ratings = pd.DataFrame(data_rating.values, index=dates_rating, columns=codes_rating)
        self.ratings = self.ratings[self.ratings.index.notnull()]
        if hasattr(self.ratings, 'map'):
            self.ratings = self.ratings.map(lambda x: self.rating_map.get(str(x).strip(), 0))
        else:
            self.ratings = self.ratings.applymap(lambda x: self.rating_map.get(str(x).strip(), 0))

        # 剩余期限 (Row 0 codes, Row 2+ dates/data)
        df_term = xl_feat.parse('剩余期限', header=None)
        codes_term = df_term.iloc[0, 1:].values
        dates_term = pd.to_datetime(df_term.iloc[2:, 0], errors='coerce')
        data_term = df_term.iloc[2:, 1:]
        self.remaining_term = pd.DataFrame(data_term.values, index=dates_term, columns=codes_term)
        self.remaining_term = self.remaining_term.apply(pd.to_numeric, errors='coerce')
        self.remaining_term = self.remaining_term[self.remaining_term.index.notnull()]

        # 可转债余额 (Row 3 codes, Row 2 listing_dates, Row 5+ dates/data)
        df_balance = xl_feat.parse('可转债余额', header=None)
        codes_bal = df_balance.iloc[3, 1:].values
        self.listing_dates = pd.Series(df_balance.iloc[2, 1:].values, index=codes_bal)
        self.listing_dates = pd.to_datetime(self.listing_dates, errors='coerce')
        
        dates_bal = pd.to_datetime(df_balance.iloc[5:, 0], errors='coerce')
        data_bal = df_balance.iloc[5:, 1:]
        self.balance = pd.DataFrame(data_bal.values, index=dates_bal, columns=codes_bal)
        self.balance = self.balance.apply(pd.to_numeric, errors='coerce')
        self.balance = self.balance[self.balance.index.notnull()]

        # 可转债交易额 (Row 3 codes, Row 5+ dates/data)
        df_turnover = xl_feat.parse('可转债交易额', header=None)
        codes_turn = df_turnover.iloc[3, 1:].values
        dates_turn = pd.to_datetime(df_turnover.iloc[5:, 0], errors='coerce')
        data_turn = df_turnover.iloc[5:, 1:]
        self.turnover = pd.DataFrame(data_turn.values, index=dates_turn, columns=codes_turn)
        self.turnover = self.turnover.apply(pd.to_numeric, errors='coerce')
        self.turnover = self.turnover[self.turnover.index.notnull()]

        # 价格数据 (Row 0 codes, Row 2+ dates/data)
        df_prices = xl_feat.parse('可转债价格', header=None)
        codes_px = df_prices.iloc[0, 1:].values
        dates_px = pd.to_datetime(df_prices.iloc[2:, 0], errors='coerce')
        data_px = df_prices.iloc[2:, 1:]
        self.prices = pd.DataFrame(data_px.values, index=dates_px, columns=codes_px)
        self.prices = self.prices.apply(pd.to_numeric, errors='coerce')
        self.prices = self.prices[self.prices.index.notnull()]
        
        # 3. 从本地文件加载 000832.CSI 中证转债指数
        print("从本地 Excel 加载中证转债指数 (000832_CSI_close_price.xlsx)...")
        try:
            bench_file = os.path.join(self.data_dir, "000832_CSI_close_price.xlsx")
            if not os.path.exists(bench_file):
                raise FileNotFoundError(f"未找到基准文件: {bench_file}")
            
            # 根据探索发现，数据从第6行开始 (skiprows=5)
            df_bench = pd.read_excel(bench_file, skiprows=5)
            
            if df_bench is not None and not df_bench.empty:
                # 确保包含必要列
                if 'Date' not in df_bench.columns or 'close' not in df_bench.columns:
                    # 尝试处理可能的列名差异
                    df_bench.columns = [str(c).strip() for c in df_bench.columns]
                    if 'Date' not in df_bench.columns or 'close' not in df_bench.columns:
                        raise ValueError(f"基准文件格式错误，缺少 Date 或 close 列。现有列: {df_bench.columns.tolist()}")

                df_bench['Date'] = pd.to_datetime(df_bench['Date'])
                df_bench.set_index('Date', inplace=True)
                df_bench.sort_index(inplace=True)
                
                self.benchmark_prices = df_bench['close']
                # 使用收盘价计算日收益率
                self.benchmark_returns_daily = self.benchmark_prices.pct_change().fillna(0)
                print("本地基准数据加载成功。")
            else:
                raise ValueError("本地基准数据为空")
        except Exception as e:
            print(f"从本地加载基准数据失败: {e}，将尝试从特征数据库回退。")
            # 回退逻辑 (从特征数据库的 '收益率走势' 页签读取)
            try:
                df_bench = xl_feat.parse('收益率走势')
                self.benchmark_returns_raw = self._clean_ts_data(df_bench)
                if '中证转债' in self.benchmark_returns_raw.columns:
                    self.benchmark_prices = self.benchmark_returns_raw['中证转债']
                else:
                    self.benchmark_prices = self.benchmark_returns_raw.iloc[:, 0]
                self.benchmark_returns_daily = self.benchmark_prices.pct_change().fillna(0)
                print("从特征数据库回退加载基准成功。")
            except Exception as e2:
                print(f"所有基准加载方式均失败: {e2}")
                self.benchmark_returns_daily = None

        print("数据加载与清洗完成。")
        
        # 统一索引并填充缺失值
        print("对齐数据索引...")
        common_index = self.relative_deviation.index
        self.ratings = self.ratings.reindex(common_index).ffill()
        self.remaining_term = self.remaining_term.reindex(common_index).ffill()
        self.balance = self.balance.reindex(common_index).ffill()
        self.turnover = self.turnover.reindex(common_index).fillna(0)
        self.prices = self.prices.reindex(common_index).ffill()
        
        # 信号平滑 (4周移动平均，减少噪音)
        self.rd_smoothed = self.relative_deviation.rolling(window=4, min_periods=1).mean()
        
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
        1. 流动性: 周成交额 > 500万 (0.05亿元)
        2. 评级: >= AA- (映射值为1)
        3. 剩余期限: > 1年
        4. 未转股余额: > 3000万 (0.3亿元)
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
        
        # 3. 余额筛选 (单位: 亿元)
        valid_balance = b_at_date[b_at_date > 0.3].index
        
        # 4. 流动性筛选 (单位: 亿元, 假设表单中为周成交额)
        valid_turnover = v_at_date[v_at_date > 0.05].index
        
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
            
            strategy_ret = 0
            long_ret = 0
            short_ret = 0
            bench_ret = 0
            
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
                            strategy_ret = 0
                    else:
                        p_start = self.prices.loc[date]
                        p_end = self.prices.loc[next_date]
                        valid_long = [c for c in current_long if pd.notnull(p_start[c]) and pd.notnull(p_end[c])]
                        valid_short = [c for c in current_short if pd.notnull(p_start[c]) and pd.notnull(p_end[c])]
                        if valid_long and valid_short:
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
                
            portfolio_returns.append(strategy_ret)
            portfolio_long_returns.append(long_ret)
            portfolio_short_returns.append(-short_ret)
            
            # 基准收益：使用价格直接计算，逻辑与 B-S 模型保持一致
            if self.benchmark_prices is not None:
                def get_price(target_date):
                    if target_date in self.benchmark_prices.index:
                        return self.benchmark_prices.loc[target_date]
                    valid_dates = self.benchmark_prices.index[self.benchmark_prices.index <= target_date]
                    if len(valid_dates) > 0:
                        return self.benchmark_prices.loc[valid_dates[-1]]
                    return None
                
                p0 = get_price(date)
                p1 = get_price(next_date)
                
                if p0 is not None and p1 is not None and p0 != 0:
                    bench_ret = (p1 - p0) / p0
                else:
                    bench_ret = 0
            else:
                # 回退：计算 universe 中所有标的的等权平均收益
                if self.returns_data is not None:
                    mask = (self.returns_data.index > date) & (self.returns_data.index <= next_date)
                    if mask.any():
                        u_rets = (1 + self.returns_data.loc[mask, universe.intersection(self.returns_data.columns)]).prod() - 1
                        bench_ret = u_rets.mean()
                    else:
                        bench_ret = 0
                else:
                    if len(universe) > 0:
                        p_start = self.prices.loc[date]
                        p_end = self.prices.loc[next_date]
                        valid_u = [c for c in universe if pd.notnull(p_start[c]) and pd.notnull(p_end[c])]
                        bench_ret = (p_end[valid_u] / p_start[valid_u] - 1).mean() if valid_u else 0
                    else:
                        bench_ret = 0
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
        
        def get_metrics(r, c):
            # 1. 年化收益率 (使用实际日历天数计算，更准确)
            # 使用 self.rebalance_dates[0] 作为起始日期，确保包含第一期的时间
            if hasattr(self, 'rebalance_dates') and len(self.rebalance_dates) > 0:
                start_date = self.rebalance_dates[0]
                days = (c.index[-1] - start_date).days
            else:
                days = (c.index[-1] - c.index[0]).days
                
            if days <= 0: return 0, 0, 0, 0
            ann_ret = (1 + c.iloc[-1]) ** (365.25 / days) - 1
            
            # 2. 年化波动率 (基于月度收益率序列)
            ann_vol = r.std() * np.sqrt(12)
            
            # 3. 夏普比率 (假设无风险利率为0)
            sharpe = ann_ret / ann_vol if ann_vol != 0 else 0
            
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
            for ticker in op['long']:
                ops_df.append({'日期': date, '方向': '做多', '标的': ticker})
            for ticker in op['short']:
                ops_df.append({'日期': date, '方向': '做空', '标的': ticker})
        
        pd.DataFrame(ops_df).to_csv('strategy_operations.csv', index=False, encoding='utf_8_sig')
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
            plt.savefig('strategy_performance.png')
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
    data_path = r"d:\Python\浙商证券固收\转债错误定价"
    models = ["BS_Model_Summary.xlsx", "ZL_Model_Summary.xlsx"]
    all_metrics = {}
    all_data = {}
    
    for model in models:
        print(f"\n\n{'#'*20} 正在运行模型: {model} {'#'*20}")
        strategy = CBStrategy(data_path)
        strategy.load_data(model_file_name=model)
        strategy.run_backtest()
        results = strategy.analyze_results(plot=False)
        metrics = results['metrics']
        all_metrics[model] = metrics
        benchmark_metrics = metrics['benchmark'] # 基准数据理论上两个模型跑出来是一样的
        all_data[model] = results['data']
    
    # 打印最终对比表
    print("\n\n" + "="*90)
    print(f"{'模型对比汇总':^90}")
    print("="*90)
    print(f"{'指标':<8} | {'BS多空':<8} | {'BS多头':<8} | {'BS空头':<8} | {'ZL多空':<8} | {'ZL多头':<8} | {'ZL空头':<8} | {'基准':<8}")
    print("-" * 90)
    
    m_names = ['年化收益', '年化波动', '夏普比率', '最大回撤']
    for i, name in enumerate(m_names):
        bs_s = all_metrics["BS_Model_Summary.xlsx"]['strategy'][i]
        bs_l = all_metrics["BS_Model_Summary.xlsx"]['long'][i]
        bs_sh = all_metrics["BS_Model_Summary.xlsx"]['short'][i]
        zl_s = all_metrics["ZL_Model_Summary.xlsx"]['strategy'][i]
        zl_l = all_metrics["ZL_Model_Summary.xlsx"]['long'][i]
        zl_sh = all_metrics["ZL_Model_Summary.xlsx"]['short'][i]
        bm_val = benchmark_metrics[i]
        
        if name in ['年化收益', '年化波动', '最大回撤']:
            print(f"{name:<8} | {bs_s:>8.2%} | {bs_l:>8.2%} | {bs_sh:>8.2%} | {zl_s:>8.2%} | {zl_l:>8.2%} | {zl_sh:>8.2%} | {bm_val:>8.2%}")
        else:
            print(f"{name:<8} | {bs_s:>8.2f} | {bs_l:>8.2f} | {bs_sh:>8.2f} | {zl_s:>8.2f} | {zl_l:>8.2f} | {zl_sh:>8.2f} | {bm_val:>8.2f}")
    print("="*90)

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
    
    model_labels = {'BS_Model_Summary.xlsx': 'BS', 'ZL_Model_Summary.xlsx': 'ZL'}

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
        
        save_name = f'{label_prefix}_model_performance.png'
        plt.savefig(save_name, dpi=300, bbox_inches="tight")
        print(f"{label_prefix} 模型对比图已保存为 {save_name}")
