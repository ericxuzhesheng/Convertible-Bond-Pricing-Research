# CCB 2023：实际利率曲线扩展与同样本比较

结果见 [基础报告](results/REPORT.md) 和 [2023版明确不赎回承诺补充](results_waivers_2023/REPORT.md)。独立研究目录；不会修改已有
BS/ZL/LSM 定价、manifest、策略或冻结账本。覆盖2019-01-04至2026-10-23。

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

## 运行

使用项目相同的 Python/pandas/numpy/scipy/numba 环境；取数还需 requests 和现有 Tushare 凭据。
所有路径由脚本位置解析，可从仓库根目录运行：

```powershell
python -X utf8 experiments/ccb_2023/prepare_inputs.py --fetch
python -X utf8 experiments/ccb_2023/supplement_floor.py
python -X utf8 experiments/ccb_2023/diagnose_volatility.py
python -X utf8 experiments/ccb_2023/validate.py
python -X utf8 experiments/ccb_2023/run_history.py --workers 4
python -X utf8 experiments/ccb_2023/summarize.py
python -X utf8 experiments/ccb_2023/verify_results.py
python -X utf8 -m pytest -q tests/test_ccb_2023.py
```

明确承诺补充在基础结果完成后独立运行：

```powershell
python -X utf8 experiments/ccb_2023/waiver_data.py --download
python -X utf8 experiments/ccb_2023/waiver_terms.py
python -X utf8 experiments/ccb_2023/run_waivers.py
python -X utf8 experiments/ccb_2023/verify_waivers.py
python -X utf8 -m pytest -q tests/test_ccb_waivers_2023.py
```

巨潮检索保留分页响应、原公告PDF和提取文字；期限表记录逐条来源、可用日期、
证据及解析规则。当前最新公告期限未解决的观测从增强版共同样本中排除，
基础CCB也在完全相同的样本上重新计算误差指标。未检索到公告不等于已确认没有承诺。

取数脚本缓存响应并保留接口参数、获取时间和逐项修复记录；不补常数、不做历史插值。
`prepare_inputs.py` 重新整理原始缺失，随后 `supplement_floor.py` 才添加独立来源。
后者也可复用已有缓存。东方财富债底与Tushare方法不等价，单独标记；
四模型比较程序拒绝混入独立债底补充样本。

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
