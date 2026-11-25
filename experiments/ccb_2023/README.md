# CCB 2023：实际利率曲线扩展与同样本比较

本实验使用仓库最早可用的周度市场截面，估值区间为 2017-01-06 至 2026-11-20，共 506 周。四模型共同样本从 BS、ZL、LSM 最早可用的 2019-01-04 起算。结果见 [基础报告](results/REPORT.md) 和 [明确不赎回承诺补充](results_waivers_2023/REPORT.md)。

2017 和 2018 年补入 503 条中债国债观测。原始曲线通过 AkShare 的 `bond_china_yield` 按自然年提取，保留 1、3、5、7、10 年期，百分数除以 100 后保存。 [中债历史查询](https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery?startDate=2017-01-01&endDate=2017-12-31&gjqx=0&qxId=ycqx&locale=cn_ZH) · [定价输入](inputs/chinabond_yields_2017_2018.csv)

## 经济设定

按国盛证券2023年3月2日报告第5—9、30—31页的基础 CCB 假设：保护期为
实际转股开始日剩余期限与1/12年的较大值；之后连续触及赎回线立即转股终止；
省略回售和未来下修。`results/`保留基础保护期结果；`results_waivers_2023/`
进一步纳入按估值时点可知的明确不赎回期限，保护期取上述基础期限与承诺剩余期限的较大值。
没有使用2024版CCBA的默认期限或未赎回行为推断，也不声称历史公告已完整覆盖。

用户进一步要求优先使用实际数据，因此主结果使用估值日观测国债期限曲线、
合同赎回阈值、合同现金流和实际最后转股日。信用利差只由观测债底校准，
不由转债市价校准；缺乏逐券信用期限曲线时仅用可识别的平坦利差。
波动率与原三模型保持相同历史窗口。股息率0的转股价值过程假设同原基线。
主结果不是研报固定参数的逐项复刻。

`ccb.py` 保留常利率公式的确定性积分参考，`curve_pde.py` 是实际曲线的主求解器。
数值精度采用中等容差：抽样网格加密差异须低于0.05元/张；该容差不是定价偏差阈值。

## 数学实现

令 `D(t)` 为输入曲线的折现因子，`c_i` 为合同现金流，`s` 为债底校准利差。
与研报一致，信用折减后的现金流为 `c_i exp(-s t_i)`。
到期转股与现金选择的阈值也是折减后的本息，不是未折减面值。

零股息假设下，停止后的贴现转股价值是鞅，因此可写为：

`CCB = 当前平价 + 未被赎回路径上的到期看跌支付现值 + 存续路径上的票息现值`。

常利率时，对保护期结束时的对数价格分布积分，使用吸收障碍的反射密度，
得到与原报告二元正态公式等价的值。实际曲线时，对 `W=CCB-平价` 解倒向 PDE：

`W_t + (f(t)-sigma^2/2) W_x + sigma^2/2 W_xx - f(t) W = 0`，

其中 `f(t)=-d log D(t)/dt`。保护期之后障碍处 `W=0`，票息日在存续区域加入现金流。
实际转股截止早于兑付日时，把之后现金流折现到最后转股选择日。
采用 Crank–Nicolson、事件后的两个隐式半步和精确事件节点，合并浮点精度内重复节点。

## 更新与验证

Windows 定时周更通过 `backtest/ccb_weekly.py` 增量更新生产 CCB，截止日取 BS、ZL、LSM 中最早的已验证截止日。生产 CCB 使用四模型自 2019 年开始的共同周度日历。以下独立研究命令重算自 2017 年开始的 CCB 专项估值和报告。

```powershell
python -X utf8 experiments/ccb_2023/run_history.py --workers 4
python -X utf8 experiments/ccb_2023/summarize.py
python -X utf8 experiments/ccb_2023/verify_results.py
python -X utf8 experiments/ccb_2023/waiver_data.py --download
python -X utf8 experiments/ccb_2023/waiver_terms.py
python -X utf8 experiments/ccb_2023/run_waivers.py
python -X utf8 experiments/ccb_2023/verify_waivers.py
python -X utf8 -m pytest -q tests/test_ccb_2023.py tests/test_ccb_waivers_2023.py
```

2026-10-29对原194份待核文件的专项复核见
[复核报告](results_waivers_2023/REVIEW_20261029.md)。`inputs/waivers/review_20261029/`
保存原队列、原结果、逐项决定及支持材料。`waiver_terms.py`会校验原文和支持材料的时点后应用已复核记录；
被替代的旧稿只有在更新版可用后才退出冲突判断。该次专项复核的数据与结论以存档为准。

2026-11-25 核对了贵广和佳禾的更正稿及历史待核公告。 [复核结果与原始资料](inputs/waivers/review_20261125/README.md)

巨潮检索保留分页响应、原公告PDF和提取文字；期限表记录逐条来源、可用日期、
证据及解析规则。当前最新公告期限未解决的观测从增强版共同样本中排除，
基础CCB也在完全相同的样本上重新计算误差指标。未检索到公告不等于已确认没有承诺。

重查历史输入缺口时，可用 `prepare_inputs.py --fetch` 拉取 Tushare 数据，再用 `supplement_floor.py` 查询独立债底和 `diagnose_volatility.py` 核查停牌。取数需要 requests 和 Tushare 凭据，并保存接口参数、获取时点和修复明细。东方财富债底在 CCB 中单独标记，四模型共同样本限用已有生产债底。

## 文件

- `inputs/repairs.csv`：新增数据与来源；`missing_before.csv`、`missing_after.csv`：字段级缺口。
- `inputs/alternative_floor_overlap.csv`：债底不同来源的历史重叠差异，不是模型误差。
- `inputs/volatility_gap_diagnosis.csv`：停牌核查；`inputs/raw/`：原始取数证据。
- `inputs/snapshot/`：实际使用的周度输入、合同、曲线和基线来源 manifest，供复核。
- `results/CCB_observations.csv`：全部有效CCB观测及可用的现有三模型价格。
- `results/common_sample.csv`：四模型、市场报价均有效的严格共同样本。
- `results/comparison_*.csv`：整体、年度、平价分组指标。
- `results/numerical_validation*`、`observed_grid_convergence.csv`：独立数值验证与实际样本收敛记录。
- `results/manifest.json`：设定、来源提交、精度和覆盖记录。

运行开始时复用现有三模型的完整性检查。比较样本逐券日对齐，不能把CCB新增覆盖
与其他模型原始覆盖直接作非配对统计。价格误差衡量的是市场拟合，不代表已知公允价值误差。
