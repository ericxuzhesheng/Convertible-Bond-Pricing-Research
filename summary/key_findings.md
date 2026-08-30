# Key Findings | 核心结论

- Traditional relative valuation fails under high valuation + crowded trading regime.
- BS model acts as an offensive pricing anchor (captures equity beta and volatility).
- ZL model acts as a defensive anchor (captures bond floor and clause constraints).
- LSM adds voluntary early-conversion and continuation-value decisions; the combined value is `max(ZL, LSM)` to avoid double-counting.
- Mispricing factors are highly orthogonal to traditional factors (liquidity, momentum, etc.).
- Backtest (2019-01-25 to 2026-08-28, 91 return observations):
  - BS equal-weight six-factor long portfolio: 12.25% annualized excess return (Sharpe 0.91; max drawdown -28.96%)
  - ZL equal-weight six-factor long portfolio: 13.40% annualized excess return (Sharpe 0.94; max drawdown -29.75%)
  - LSM equal-weight six-factor long portfolio: 13.30% annualized excess return (Sharpe 0.92; max drawdown -31.04%)
  - CSI Convertible Bond Index benchmark: 7.08% annual return

- 在高估值与拥挤交易并存的市场环境下，传统相对估值体系明显失效。
- BS 模型可作为进攻型定价锚（捕捉正股 beta 与波动率）。
- ZL 模型可作为防守型定价锚（刻画债底与条款约束）。
- LSM 补充自愿提前转股与继续价值决策；组合价格取 `max(ZL, LSM)` 以避免重复计价。
- 错误定价因子与传统因子（流动性、动量等）呈显著正交。
- 回测结果（2019-01-25 至 2026-08-28，共 91 个收益观察期）：
  - BS 六因子等权多头组合：年化超额收益 12.25%（夏普 0.91；最大回撤 -28.96%）
  - ZL 六因子等权多头组合：年化超额收益 13.40%（夏普 0.94；最大回撤 -29.75%）
  - LSM 六因子等权多头组合：年化超额收益 13.30%（夏普 0.92；最大回撤 -31.04%）
  - 同期中证转债指数基准：年化收益 7.08%

## Update Contract | 更新约束

- Routine weekly runs are incremental only: ZL and LSM have separate verified manifests, and BS/ZL/LSM price only later trading weeks.
- 日常周更新只跑增量：ZL 与 LSM 分别使用独立已验证 Manifest，BS/ZL/LSM 仅计算后续新增交易周，不回算历史。

## Methodology | 方法框架

### 1. Absolute Pricing Framework | 绝对定价框架

- BS model: closed-form option pricing (equity-driven)
- ZL model: Monte Carlo simulation with embedded clauses
- LSM model: quadratic Longstaff-Schwartz continuation regression with antithetic paths

- BS 模型：闭式期权定价（由权益端驱动）
- ZL 模型：含条款触发机制的蒙特卡洛模拟
- LSM 模型：对偶路径与二次基函数的 Longstaff--Schwartz 继续价值回归

### 2. Mispricing Signal | 错误定价信号

Mispricing = Model Price - Market Price

错误定价 = 模型价格 - 市场价格

### 3. Strategy | 策略

- Long bottom 20% (undervalued)
- Short top 20% (overvalued)

- 做多低估值后 20%
- 做空高估值前 20%

### 4. Constraints | 约束条件

- Liquidity filter
- Rating >= AA-
- Maturity > 1 year

- 流动性过滤
- 评级 >= AA-
- 剩余期限 > 1 年

## Alpha Source | Alpha 来源

The mispricing factor shows low correlation with traditional factors, indicating it captures non-linear option misvaluation rather than market style exposure.

错误定价因子与传统风格因子相关性较低，说明其主要捕捉的是期权非线性错配，而非一般市场风格暴露。
