# Research Brief | 研究摘要

## 中文

### 研究问题

可转债同时具有债券现金流、股票期权和路径依赖条款。单一相对估值指标很难分辨价格偏离来自基本价值、条款约束还是市场风格。本项目因此提出一个可检验的问题。不同复杂度的绝对定价模型能否识别具有投资信息的错误定价，并在计入交易成本后形成稳定的横截面信号？

### 研究设计与贡献

项目在统一的数据和回测口径下比较三类模型。BS 提供透明的闭式基准，ZL 通过蒙特卡罗路径纳入赎回、回售与下修条款，LSM 用继续价值回归刻画自愿提前转股。为避免重复计算同一转股权，最终 LSM 组合价值取 `max(ZL, LSM)`。

仓库把定价公式接入市场数据、模型估值、错误定价因子、月度组合和自动发布，并用独立 Manifest 锁定 ZL 与 LSM 的历史边界。历史初始化完成后，日常任务只计算新增交易周，已有结果须通过输入指纹与输出哈希验证。

### 主要结果

截至 2026-08-28，BS 有 149,694 个有效定价样本，ZL 与 LSM 各有 138,820 个。LSM 的 MAPE 为 9.23%，低于 BS 的 9.76% 和 ZL 的 9.49%；其平均定价偏差为 -2.32 元，也明显小于 ZL 的 -12.85 元。

在 2019-01-25 至 2026-08-28 的 91 个收益观察期内，扣除交易成本后的六因子等权多头组合结果如下。

| Model | Annualized excess return | Sharpe | Max drawdown |
| --- | ---: | ---: | ---: |
| BS | 12.25% | 0.91 | -28.96% |
| ZL | 13.40% | 0.94 | -29.75% |
| LSM | 13.30% | 0.92 | -31.04% |

同期中证转债指数年化收益为 7.08%。定价拟合改善没有机械地转化为更强的策略表现。LSM 的价格误差更低，但其组合未稳定超越 ZL，最大回撤也更高。这一区分避免用单一回测指标为模型复杂度背书。

### 结论边界

定价偏差与流动性、波动率、量价、传统估值及动量因子的线性相关性较低，说明它提供了不同于常见风格指标的信息，但这不等于因果独立。结果仍受波动率与信用利差估计、条款行为假设、样本内选择、交易容量和卖空约束影响。当前证据支持把三种模型视为互补的估值工具，不支持将其解释为无风险套利。

## English

### Research question

A convertible bond combines fixed-income cash flows, an equity option, and path-dependent contractual clauses. A single relative-valuation ratio cannot readily distinguish fundamental value, clause effects, and market-style exposure. This project asks whether absolute-pricing models of different complexity can identify investable mispricing after transaction costs.

### Design and contribution

The study compares three models under one data and backtest protocol. BS provides a transparent closed-form baseline. ZL incorporates call, put, and reset clauses through Monte Carlo paths. LSM estimates continuation value to represent voluntary early conversion. The final LSM combination uses `max(ZL, LSM)` so that the same conversion option is not counted twice.

The contribution extends beyond formula replication. The repository connects market data, model valuation, mispricing factors, monthly portfolios, and automated publication. Separate manifests certify the historical boundaries of ZL and LSM. Once the historical baseline is initialized, routine jobs process only new trading weeks and verify the prior inputs and published outputs before continuing.

### Main findings

Through 2026-08-28, the sample contains 149,694 valid BS valuations and 138,820 observations for each of ZL and LSM. LSM records a 9.23% MAPE, compared with 9.76% for BS and 9.49% for ZL. Its mean pricing bias is -2.32 CNY, substantially narrower than ZL's -12.85 CNY.

Across 91 return observations from 2019-01-25 to 2026-08-28, the net-of-cost equal-weight six-factor long portfolios produce the results shown above. The CSI Convertible Bond Index returns 7.08% annualized over the same period.

The most informative result is that better pricing fit does not mechanically produce a stronger investment strategy. LSM reduces valuation error, yet it does not consistently outperform ZL and experiences a larger drawdown. This distinction prevents model complexity from being justified by a single favorable metric.

### Interpretation limits

Mispricing has low linear correlation with liquidity, volatility, price-volume, conventional valuation, and momentum factors. It therefore adds information beyond common style measures, but low correlation does not establish causal independence. The findings remain sensitive to volatility and credit-spread estimates, clause-behavior assumptions, in-sample selection, capacity, and short-sale constraints. The evidence supports complementary valuation anchors, not a claim of risk-free arbitrage.

## Update Contract | 更新约束

- Routine weekly runs are incremental only. ZL and LSM have separate verified manifests, and BS, ZL, and LSM price only later trading weeks.
- 日常周更新只跑增量。ZL 与 LSM 分别使用独立的已验证 Manifest，BS、ZL 与 LSM 仅计算后续新增交易周。

## Reproduction Notes | 复现说明

### Absolute Pricing Framework | 绝对定价框架

- BS model: closed-form option pricing (equity-driven)
- ZL model: Monte Carlo simulation with embedded clauses
- LSM model: quadratic Longstaff-Schwartz continuation regression with antithetic paths

- BS 模型：闭式期权定价（由权益端驱动）
- ZL 模型：含条款触发机制的蒙特卡罗模拟
- LSM 模型：对偶路径与二次基函数的 Longstaff-Schwartz 继续价值回归

### Mispricing Signal | 错误定价信号

Mispricing = Model Price - Market Price

错误定价 = 模型价格 - 市场价格

### Strategy | 策略

- Long bottom 20% (undervalued)
- Short top 20% (overvalued)

- 做多低估值后 20%
- 做空高估值前 20%

### Constraints | 约束条件

- Liquidity filter
- Rating >= AA-
- Maturity > 1 year

- 流动性过滤
- 评级 >= AA-
- 剩余期限 > 1 年
