<div align="center">

# Convertible Bond Pricing Research (China Market)

---

## 中国市场可转债定价模型研究 | Convertible Bond Pricing Research (China Market)

[![LANGUAGE 中文](https://img.shields.io/badge/LANGUAGE-%E4%B8%AD%E6%96%87-e74c3c?style=for-the-badge)](#简体中文)
[![LANGUAGE ENGLISH](https://img.shields.io/badge/LANGUAGE-ENGLISH-2f7de1?style=for-the-badge)](#english-version)

</div>

---

## 简体中文

**当前语言：中文 | [Switch to English](#english-version)**

👉 面试官建议先看摘要：[summary/key_findings.md](summary/key_findings.md)  
👉 See concise summary: [summary/key_findings.md](summary/key_findings.md)

<a id="english-version"></a>

---

## 📌 Overview | 项目概述

- This project develops an **absolute pricing framework** for Chinese convertible bonds using two core models. | 本项目构建了一个基于两类核心模型的**可转债绝对定价框架**。
- Core objective: remove sentiment-driven valuation bias and convert mispricing into **tradable alpha signals**. | 核心目标：剥离情绪驱动估值偏差，并将错误定价转化为**可交易 Alpha 信号**。

Core models | 核心模型：

- Black-Scholes (BS) Model | Black-Scholes（BS）模型
- Zheng-Lin (ZL) Model | 郑-林（ZL）模型

---

## 📚 Paper Source | 论文来源

- Primary reference paper: 中国可转债定价模型比较研究 (Zheng Zhenlong, Lan Tiansheng, Chen Rong). | 主要参考论文：中国可转债定价模型比较研究（郑振龙、兰添晟、陈蓉）。
- DOI: 10.13821/j.cnki.ceq.2025.01.11. | DOI：10.13821/j.cnki.ceq.2025.01.11。
- Core idea: compare multiple convertible-bond pricing models by both pricing error and long-short alpha performance. | 核心思路：同时从定价误差与多空组合 Alpha 两个维度比较多种可转债定价模型。
- This repository includes full reports in report/ for detailed assumptions, calibration, and empirical outputs. | 本仓库在 report/ 目录提供完整报告，便于查看模型假设、参数设定与实证细节。

---

## 🗂️ Repository Structure | 仓库框架

This repository is organized by research workflow from model pricing to factor construction and portfolio backtesting. | 本仓库按研究流程组织，从模型定价到因子构建，再到组合回测。

```text
Convertible-Bond-Pricing-Research/
├─ backtest/                # BS and ZL pricing/backtest engines | BS 与 ZL 定价回测主程序
├─ mispricing factor/       # Mispricing factor and correlation analysis | 错误定价因子与相关性分析
├─ long-short strategy/     # Cross-sectional long-short strategy outputs | 横截面多空策略与绩效输出
├─ summary/                 # Concise summary for interview reading | 面试优先阅读精简总结
├─ report/                  # Full research reports (PDF) | 完整研究报告（PDF）
└─ README.md                # Project overview and methodology | 项目总览与方法框架
```

Suggested reading order | 建议阅读顺序：

1. summary/key_findings.md
2. README.md
3. report/CB_pricing_full.pdf

---

## 🏷️ Core Tags | 核心标签

- Convertible bond pricing | 可转债定价
- Absolute valuation | 绝对估值
- Mispricing alpha | 错误定价 Alpha
- Multi-factor integration | 多因子融合
- Cross-sectional long-short strategy | 横截面多空策略
- Monte Carlo path-dependent pricing | 蒙特卡洛路径依赖定价

---

## 🎯 Motivation | 研究动机

In the Chinese convertible bond market: | 在当前中国可转债市场中：

- High valuation and crowded trading distort pricing. | 高估值与拥挤交易导致价格扭曲。
- Relative valuation metrics (e.g., conversion premium) become unreliable. | 相对估值指标（如转股溢价率）失效。
- Embedded clauses create strong path dependency. | 条款复杂且路径依赖显著。

-> A unified **absolute pricing anchor** is required. | -> 需要构建统一的**绝对定价锚**。

---

## 🧠 Pricing Framework | 定价框架

### 1. Convertible Bond Decomposition | 可转债价值拆解

$$
V_{CB} = V_{bond} + V_{option}
$$

- Bond component: discounted cash flows. | 债券部分：未来现金流贴现。
- Option component: embedded equity call option. | 期权部分：嵌入的转股期权。

---

## ⚙️ Model Design | 模型设计

### 🔹 Black-Scholes Model (BS)

Pricing Logic | 定价逻辑：

$$
V_{option} = S e^{-qT} N(d_1) - X e^{-rT} N(d_2)
$$

- Closed-form solution under lognormal stock dynamics. | 在股价对数正态假设下的解析解模型。
- Ignores path-dependent clauses. | 未显式考虑路径依赖条款。

Key characteristics | 核心特征：

- High sensitivity to equity price and volatility. | 对正股价格与波动率高度敏感。
- No upper bound under call-free assumption. | 无赎回约束时，上行空间不受限。

-> Acts as an **offensive pricing anchor**. | -> 属于**进攻型定价锚**。

---

### 🔹 Zheng-Lin Model (ZL)

Pricing Logic | 定价逻辑：

Monte Carlo simulation with optimal stopping. | 基于最优停止思想的蒙特卡洛模拟。

1. Simulate stock price paths. | 模拟正股价格路径。
2. Evaluate clause triggers (Call/Put/Reset). | 评估赎回/回售/下修条款触发。
3. Discount expected payoff. | 对预期现金流折现。

Model origin and mechanism | 模型来源与机制：

- The ZL framework is built on issuer-investor clause game logic under callable/putable/adjustable convertibles. | ZL 框架建立在可赎回、可回售、可下修条款下发行人与投资者的博弈逻辑之上。
- It explicitly models optimal issuer decisions (call/reset) and investor response (convert/put/hold) along each path. | 模型在每条路径上显式刻画发行人的最优决策（赎回/下修）与投资者响应（转股/回售/持有）。
- Compared with static closed-form models, it better captures path dependency and clause-triggered nonlinear payoff. | 相较静态闭式模型，ZL 更能刻画路径依赖与条款触发导致的非线性收益结构。
- In this project, ZL is treated as a defensive anchor suitable for downside-risk-aware valuation. | 在本项目中，ZL 被定位为偏防守的定价锚，更适合下行风险约束场景。

Key characteristics | 核心特征：

- Fully path-dependent and clause-aware. | 全路径依赖，条款刻画更完整。
- Captures call cap and reset convexity. | 能刻画强赎上限与下修凸性。
- Produces more conservative valuation. | 估值相对更保守。

-> Acts as a **defensive anchor**. | -> 属于**防守型定价锚**。

---

### Model Error Comparison | 模型误差对比

| Error Metric           | BS    | ZL    |
| ---------------------- | ----- | ----- |
| Mean Error (Bias, CNY) | 2.13  | -4.73 |
| MAE (CNY)              | 13.74 | 11.66 |
| RMSE (CNY)             | 29.73 | 30.19 |
| MAPE                   | 9.79% | 7.88% |
| SMAPE                  | 9.66% | 8.14% |

Lower MAE/MAPE/SMAPE indicates better pricing fit. | MAE/MAPE/SMAPE 越低，模型定价拟合效果越好。

---

## 📊 Mispricing Signal | 错误定价因子

$$
\operatorname{Mispricing} = V_{model} - V_{market}
$$

- Positive -> undervalued -> long. | 正值 -> 低估 -> 做多。
- Negative -> overvalued -> short. | 负值 -> 高估 -> 做空。

---

## 🚀 Strategy Design | 策略构建

### 🔹 Cross-sectional Long-Short Strategy | 横截面多空策略

The strategy is constructed based on **mispricing (RD)** defined above. | 策略基于上文定义的错误定价指标（RD）构建。

- Monthly rebalancing and cross-sectional ranking by RD. | 按月调仓并在全市场按 RD 横截面排序。
- **Long portfolio**: top 20% (undervalued bonds). | **多头组合**：RD 前 20%（低估标的）。
- **Short portfolio**: bottom 20% (overvalued bonds). | **空头组合**：RD 后 20%（高估标的）。

### 🔹 Trading Logic | 交易逻辑

- Mean reversion of temporary mispricing drives excess return. | 临时性错误定价的价值回归带来超额收益。
- Convergence from market price to theoretical value is the core alpha source. | 市场价格向理论价值收敛是核心 Alpha 来源。

---

## 📈 Results | 回测结果

| Strategy | Annual Return | Sharpe | Max Drawdown |
| -------- | ------------- | ------ | ------------ |
| BS Long  | ~19.3%        | 1.35   | -22.26%      |
| ZL Long  | ~15.3%        | 1.44   | -13.74%      |

Summary: ZL performs better on relative pricing-error control, while BS is stronger in directional capture and offensive upside. | 结论摘要：ZL 在相对误差指标上更优，BS 在方向性捕捉与进攻弹性上更强。

---

## 🧩 Key Insight | 核心洞察

- BS captures **valuation expansion + momentum**. | BS 捕捉**估值扩张 + 动量**。
- ZL captures **mean reversion + downside protection**. | ZL 捕捉**价值回归 + 下行防御**。
- Mispricing factor is highly **orthogonal** to traditional style factors. | 错误定价因子与传统风格因子呈显著**正交性**。

-> Combination improves portfolio robustness. | -> 两者结合可显著提升组合稳健性。

---

## 📉 Limitations | 局限性

- BS ignores detailed clause constraints. | BS 对条款约束刻画不足。
- ZL is computationally expensive. | ZL 计算成本较高。
- Short leg can underperform during momentum-dominated markets. | 空头端在强动量市场中可能承压。

---

## 🔮 Future Work | 后续优化

- Event-driven clause modeling. | 事件驱动条款建模。
- ML-based probability estimation. | 基于机器学习的概率估计。
- Dynamic parameter calibration. | 动态参数校准。
- Integration into multi-factor system. | 融入多因子体系。

---

## 📎 Full Report | 完整报告

Full research report available here: /report/full_report.pdf | 完整研报见：/report/full_report.pdf

---

## 🧩 Contribution | 项目贡献

- Unified absolute pricing framework. | 统一的绝对定价框架。
- Tradable mispricing signal design. | 可交易的错误定价信号设计。
- Full backtesting pipeline. | 完整的回测研究流程。
- Clear separation of offensive vs defensive alpha. | 清晰区分进攻型与防守型 Alpha。

---

## 📚 Citation | 引用说明

If you use this framework or part of this project, please cite the repository and state model assumptions and data boundaries clearly. | 如使用本项目框架或部分研究成果，请引用本仓库并明确说明模型假设与数据边界。
