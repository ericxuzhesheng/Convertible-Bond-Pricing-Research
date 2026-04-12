# Key Findings | 核心结论

- Traditional relative valuation fails under high valuation + crowded trading regime.
- BS model acts as an offensive pricing anchor (captures equity beta and volatility).
- ZL model acts as a defensive anchor (captures bond floor and clause constraints).
- Mispricing factors are highly orthogonal to traditional factors (liquidity, momentum, etc.).
- Backtest (2019-):
  - BS long portfolio: ~19.3% annual return
  - ZL long portfolio: ~15.3% annual return with lower drawdown

- 在高估值与拥挤交易并存的市场环境下，传统相对估值体系明显失效。
- BS 模型可作为进攻型定价锚（捕捉正股 beta 与波动率）。
- ZL 模型可作为防守型定价锚（刻画债底与条款约束）。
- 错误定价因子与传统因子（流动性、动量等）呈显著正交。
- 回测结果（2019 年至今）：
  - BS 多头组合：年化收益约 19.3%
  - ZL 多头组合：年化收益约 15.3%，且回撤更低

## Methodology | 方法框架

### 1. Absolute Pricing Framework | 绝对定价框架

- BS model: closed-form option pricing (equity-driven)
- ZL model: Monte Carlo simulation with embedded clauses

- BS 模型：闭式期权定价（由权益端驱动）
- ZL 模型：含条款触发机制的蒙特卡洛模拟

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
