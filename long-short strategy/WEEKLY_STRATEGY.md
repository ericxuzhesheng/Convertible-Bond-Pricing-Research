# 周度三模型均价策略

> 历史相对排名研究：本页10bp、无正负号限制的规则仅用于旧版。当前独立策略为[周度均价安全边际策略](WEEKLY_VALUE_STRATEGY.md)，入口 weekly_value_strategy.py，单边5bp、扣费后2%门槛，结果保存到 weekly_value_results。

本历史版本执行入口为 BS_ZL_LSM_strategy.py，同时输出多头和理论多空。旧三模型月度研究通过 --legacy-monthly 显式运行；旧 CSV、图和六因子 IC 不被新策略覆盖。

## 每日数据与周度调仓

每个交易日保存开、高、低、收、成交量、成交额及基准日线，逐日维护头寸、现金、应收应付、费用、风险提示和净值。周度只指选券和常规再平衡。已知财务恶化、ST 或强赎提示在获知后下一交易日尝试退出，未成交头寸继续保留。

已完成交易周最后交易日生成信号，下一交易日开盘模拟成交，假期按交易日历顺延。日线开盘报价和当天成交证据只是模拟条件，保守排除单一价格的交易日；不能保证实际开盘可成交。使用连续份额记账，尚未模拟交易手数及容量，不是券商订单。

财务按报告期、公告日和版本保留。只有日期的公告下一交易日生效，同一报告后续更正以发现时点生效。首次 API 历史不代表原始发布版本，历史研究仍有修订偏差风险。银行按发行人身份识别，不能以今天的正股名称判断过去ST。

## 固定规则

- V=(BS+ZL+LSM)/3；S=V/P-1。三模型齐全、同日且对应市价一致。
- 先筛选再排名，最高和最低各取 floor(N×20%)，并列按代码。N<5不新建仓，不额外限制S正负号。
- 评级≥AA、价格≤150元、纯债溢价≤40%、剩余期限≥半年、余额≥3亿元、上市超过30天。
- 20交易日平均成交额≥1000万元，至少15日有观测，信号日成交额为正；不能使用20周代替20日。
- 净资产、最近已公告EPS及扣非净利润均为正。非银行资产负债率≤75%、经营现金流为正；银行豁免后两项。
- 跟踪版要求已知非ST、强赎状态清楚、没有已满足/提示/实施强赎或当时已知半年内退市事件。未知不算通过。
- 多头等权最多100%；多空每腿最多100%；每只绝对目标权重≤10%，不足留空。上限仅约束调仓目标，周内权重可以自然漂移。
- 默认单边摩擦10bp，借券年化3%按自然日计提，闲置现金收益0。敏感性对照5/20bp及借券0%/8%。账本预留费用，不能用融券卖出所得扩大多头。

## 初始化与运行

在仓库根目录执行，所有脚本用自身路径定位输入输出：

~~~powershell
# 首次仅补策略数据，不重算BS/ZL/LSM
python backtest/strategy_data.py --initialize-history --start 2019-01-01

# 正常增量更新
python backtest/strategy_data.py
python "long-short strategy/BS_ZL_LSM_strategy.py"

# 固定规则比较频率、模型、基本面和成本
python "long-short strategy/BS_ZL_LSM_strategy.py" --compare
~~~

参数包括 --start、--end、--model mean/BS/ZL/LSM、--frequency weekly/monthly、--line both/research/tracking、--mode both/long_only/long_short、--transaction-bps、--borrow-rate、--without-fundamentals、--input-dir、--output-dir。不同配置须用不同输出目录。

每日行情保存在 backtest/strategy_inputs/daily/YYYYMMDD.csv。按2个交易日有界请求、三个I/O线程共用限速器，碰到API行数上限自动拆分，每日独立验证保存并支持续传。缺行情、接口失败和没有合格券是三种不同状态，不能混淆。

周更新任务现经 rebuild_research_outputs.py 接入数据和独立的新安全边际策略，仍在失败时阻止发布。首次无缓存时须显式初始化，不会在周任务中自动补全历史。

## 研究与跟踪

weekly_results/research 是“估值＋基本面研究版”，保留历史事件/ST和财报版本限制。tracking 只接受成交前冻结的信号；9.11快照不能倒用于以前日期，成交后取得的快照不能生成当时交易。

信号冻结后复用，新增日账本先验证历史一致性，无变化不重写、不重复交易。最新信号尚无后续行情时，只生成待执行目标。

输出有 rankings、coverage、targets、每类组合的 daily_nav/trades/holdings/issues、metrics、windows、周报和日净值图。日收益用于252日年化波动和夏普，最大回撤包含周内变化，同时展示现金、集中度、银行暴露和滚动一年指标。

## 现金流及限制

现金流文件 cashflow_events.csv 的字段为 event_id、ts_code、kind（coupon或redemption）、record_date、ex_date、payment_date、amount_per_bond、verified、source。登记日在除息日前，支付日在除息日或其后，金额为每张支付额，赎回包含本金。

登记日收盘头寸决定权益及空头义务，除息日起形成应收应付，支付日转现金。赎回注销债券头寸，不重复保留本金。缺失收盘价可用上次真实价格暂估，但记录为未验证，不能充当卖出。

票面利率区间不能证明实际登记、除息、支付日期；未完成全部历史付息核验的结果不能称为完整总收益。已补历史赎回公告仍不覆盖全部触发计数和不赎回承诺；原始财报版本、实际借券及开盘可成交性等限制随输出展示。收益提高不是验收前提，不按表现反复调参。

来源：[日行情](https://tushare.pro/document/2?doc_id=187)、[财务](https://tushare.pro/document/2?doc_id=79)、[赎回](https://tushare.pro/document/2?doc_id=269)、[票面利率](https://tushare.pro/document/2?doc_id=305)。


本项目LSM输出包含ZL价值下限，三模型不是三份独立证据。均价仍按用户指定等权，分歧单独展示。频率对照从共同首个信号日起算，避免比较不同时段。

## 多来源补充（2026-09-12）

AkShare 已实测东方财富转债详情、新浪历史日线、集思录强赎、巨潮发行及公告查询。东方财富返回1,052只转债、813条赎回相关公告字段（其中混有不赎回公告；v2分类后710条具备赎回类型及执行证据），独立保存到 redemption_notices.csv 和 redemption_candidates.csv；只有公告可见后才影响筛选和每日退出。后续发现的旧公告以实际发现时间生效，不倒填跟踪记录。接口返回的空字段不代表历史安全。

通威、圆通和曙光的赎回价、登记日及到账日已与巨潮原公告逐项核验并录入现金流。其余候选的执行日不直接视为到账日。公告索引补取命令如下，可重复运行复用已缓存年份：

~~~powershell
python backtest/strategy_announcements.py --start 2019-01-01 --end 2026-09-11
~~~

coupon_announcement_index.csv 保留原公告链接，未核对原文的记录不能计入现金流。出现无法解释的持仓收盘价缺口时，正式年化收益、波动、夏普、回撤、周胜率留空，仅保留 diagnostic_* 诊断估计；不将陈旧估值作为有效优化结论。持仓的 mark_date、quote_age_days 表示估值来源日期及陈旧天数。

月度对照使用每个已完成自然月内最后一个共同模型截面；三模型历史本身是周度截面，月末若没有模型值不新造估值。所有频率对照使用同一起始信号和每日成交核算区间。

接口依据：[AkShare官方实现](https://github.com/akfamily/akshare/blob/main/akshare/bond/bond_zh_cov.py)、[巨潮公告查询实现](https://github.com/akfamily/akshare/blob/main/akshare/stock_feature/stock_disclosure_cninfo.py)。原公告核验：[通威](https://static.cninfo.com.cn/finalpage/2020-03-14/1207371821.PDF)、[圆通](https://static.cninfo.com.cn/finalpage/2020-03-18/1207380352.PDF)、[曙光](https://static.cninfo.com.cn/finalpage/2020-03-13/1207368445.PDF)。

截至本次补取，2019年至2026-09-11共保存2,360条含“付息”的可转债公告索引；这不是全部现金流覆盖率。已核验现金流目前为3笔赎回和节能转债2022/2023两笔付息，采用税前每张金额，不计投资者个体税负。节能2024年公告摘要和正文的兑息年份不一致，单独列入待核验，不自动修正。

通裕转债2025-04-30和05-06的缺失报价已由停复牌公告和新浪日线交叉解释，保存于 suspensions.csv。账本允许按上次真实价格暂估，记 stale_mark_count/quote_age_days；它们不充当可成交价，也不算来源不明的行情缺口。这一规则只解释估值，不用于提前预测停牌或复牌。不同输出配置和事件版本会触发重算；跟踪账本仍先校验已确认历史，冲突时停止。

调仓使用扣除最坏情形实际交易费用后的净资产下界确定可分配额度，确保新建和可调整仓位扣费后仍不超过10%；两腿完全换仓也计入费用预留。锁定头寸无法卖出导致的漂移另行保留。comparison_metrics.csv 的 variant 列标明对照组。
