<div align="center">

# Convertible Bond Pricing Research (China Market)

---

## 中国市场可转债定价模型研究 | Convertible Bond Pricing Research (China Market)

[![LANGUAGE 中文](https://img.shields.io/badge/LANGUAGE-%E4%B8%AD%E6%96%87-e74c3c?style=for-the-badge)](#简体中文)
[![LANGUAGE ENGLISH](https://img.shields.io/badge/LANGUAGE-ENGLISH-2f7de1?style=for-the-badge)](#english-version)

</div>

---

<a id="简体中文"></a>

## 简体中文

**当前语言：中文 | [Switch to English](#english-version)**

👉 面试官建议先看摘要：[summary/key_findings.md](summary/key_findings.md)

---

## 项目概述

- 本项目构建了一个基于两类核心模型的**可转债绝对定价框架**。
- 核心目标：剥离情绪驱动估值偏差，并将错误定价转化为**可交易 Alpha 信号**。

核心模型：

- Black-Scholes（BS）模型
- 郑-林（ZL）模型

---

## 论文来源

- 主要参考论文：中国可转债定价模型比较研究（郑振龙、兰添晟、陈蓉）。
- DOI：10.13821/j.cnki.ceq.2025.01.11。
- 核心思路：同时从定价误差与多空组合 Alpha 两个维度比较多种可转债定价模型。
- 本仓库在 report/ 目录提供完整报告，便于查看模型假设、参数设定与实证细节。

---

## 仓库框架

本仓库按研究流程组织，从模型定价到因子构建，再到组合回测。

```text
Convertible-Bond-Pricing-Research/
├─ backtest/                #  BS 与 ZL 定价回测主程序
├─ mispricing factor/       #  错误定价因子与相关性分析
├─ long-short strategy/     #  横截面多空策略与绩效输出
├─ summary/                 #  面试优先阅读精简总结
├─ report/                  #  完整研究报告（PDF）
└─ README.md                #  项目总览与方法框架
```

建议阅读顺序：

1. summary/key_findings.md
2. README.md
3. report/CB_pricing_full.pdf

---

## 核心标签

- 可转债定价
- 绝对估值
- 错误定价 Alpha
- 多因子融合
- 横截面多空策略
- 蒙特卡洛路径依赖定价

---

## 研究动机

在当前中国可转债市场中：

- 高估值与拥挤交易导致价格扭曲。
- 相对估值指标（如转股溢价率）失效。
- 条款复杂且路径依赖显著。

-> 需要构建统一的**绝对定价锚**。

---

## 定价框架

### 可转债价值拆解

$$
V_{CB} = V_{bond} + V_{option}
$$

- 债券部分：未来现金流贴现。
- 期权部分：嵌入的转股期权。

---

## 模型设计

### 🔹 Black-Scholes Model (BS)

定价逻辑：

$$
V_{option} = S e^{-qT} N(d_1) - X e^{-rT} N(d_2)
$$

- 在股价对数正态假设下的解析解模型。
- 未显式考虑路径依赖条款。

核心特征：

- 对正股价格与波动率高度敏感。
- 无赎回约束时，上行空间不受限。

-> 属于**进攻型定价锚**。

---

### 🔹 Zheng-Lin Model (ZL)

定价逻辑：

基于最优停止思想的蒙特卡洛模拟。

1. 模拟正股价格路径。
2. 评估赎回/回售/下修条款触发。
3. 对预期现金流折现。

模型来源与机制：

- ZL 框架建立在可赎回、可回售、可下修条款下发行人与投资者的博弈逻辑之上。
- 模型在每条路径上显式刻画发行人的最优决策（赎回/下修）与投资者响应（转股/回售/持有）。
- 相较静态闭式模型，ZL 更能刻画路径依赖与条款触发导致的非线性收益结构。
- 在本项目中，ZL 被定位为偏防守的定价锚，更适合下行风险约束场景。

核心特征：

- 全路径依赖，条款刻画更完整。
- 能刻画强赎上限与下修凸性。
- 估值相对更保守。

-> 属于**防守型定价锚**。

---

### 模型误差对比

| Error Metric           | BS    | ZL    |
| ---------------------- | ----- | ----- |
| Mean Error (Bias, CNY) | 2.13  | -4.73 |
| MAE (CNY)              | 13.74 | 11.66 |
| RMSE (CNY)             | 29.73 | 30.19 |
| MAPE                   | 9.79% | 7.88% |
| SMAPE                  | 9.66% | 8.14% |

MAE/MAPE/SMAPE 越低，模型定价拟合效果越好。

---

## 错误定价因子

$$
Mispricing = V_{model} - V_{market}
$$

- 正值 -> 低估 -> 做多。
- 负值 -> 高估 -> 做空。

---

## 策略构建

### 横截面多空策略

策略基于上文定义的错误定价指标（RD）构建。

- 按月调仓并在全市场按 RD 横截面排序。
- **多头组合**：RD 前 20%（低估标的）。
- **空头组合**：RD 后 20%（高估标的）。

### 交易逻辑

- 临时性错误定价的价值回归带来超额收益。
- 市场价格向理论价值收敛是核心 Alpha 来源。

---

## 回测结果

| Strategy | Annual Return | Sharpe | Max Drawdown |
| -------- | ------------- | ------ | ------------ |
| BS Long  | ~19.3%        | 1.35   | -22.26%      |
| ZL Long  | ~15.3%        | 1.44   | -13.74%      |

结论摘要：ZL 在相对误差指标上更优，BS 在方向性捕捉与进攻弹性上更强。

---

## 核心洞察

- BS 捕捉**估值扩张 + 动量**。
- ZL 捕捉**价值回归 + 下行防御**。
- 错误定价因子与传统风格因子呈显著**正交性**。

-> 两者结合可显著提升组合稳健性。

---

## 局限性

- BS 对条款约束刻画不足。
- ZL 计算成本较高。
- 空头端在强动量市场中可能承压。

---

## 后续优化

- 事件驱动条款建模。
- 基于机器学习的概率估计。
- 动态参数校准。
- 融入多因子体系。

---

## 完整报告

完整研报见：/report/full_report.pdf

---

## 项目贡献

- 统一的绝对定价框架。
- 可交易的错误定价信号设计。
- 完整的回测研究流程。
- 清晰区分进攻型与防守型 Alpha。

---

## 引用说明

如使用本项目框架或部分研究成果，请引用本仓库并明确说明模型假设与数据边界。

---

<a id="english-version"></a>

## English Version

**Current Language: English | [切换到中文](#简体中文)**

👉 See concise summary: [summary/key_findings.md](summary/key_findings.md)

---

## 📌 Overview

- This project develops an **absolute pricing framework** for Chinese convertible bonds using two core models.
- Core objective: remove sentiment-driven valuation bias and convert mispricing into **tradable alpha signals**.

Core models

- Black-Scholes (BS) Model
- Zheng-Lin (ZL) Model

---

## 📚 Paper Source

- Primary reference paper: Comparative Study on Pricing Models of Chinese Convertible Bonds (Zheng Zhenlong, Lan Tiansheng, Chen Rong).
- DOI: 10.13821/j.cnki.ceq.2025.01.11.
- Core idea: compare multiple convertible-bond pricing models by both pricing error and long-short alpha performance.
- This repository includes full reports in report/ for detailed assumptions, calibration, and empirical outputs.

---

## 🗂️ Repository Structure

This repository is organized by research workflow from model pricing to factor construction and portfolio backtesting.

```text
Convertible-Bond-Pricing-Research/
├─ backtest/                # BS and ZL pricing/backtest engines
├─ mispricing factor/       # Mispricing factor and correlation analysis
├─ long-short strategy/     # Cross-sectional long-short strategy outputs
├─ summary/                 # Concise summary for interview reading
├─ report/                  # Full research reports (PDF)
└─ README.md                # Project overview and methodology
```

Suggested reading order

1. summary/key_findings.md
2. README.md
3. report/CB_pricing_full.pdf

---

## 🏷️ Core Tags

- Convertible bond pricing
- Absolute valuation
- Mispricing alpha
- Multi-factor integration
- Cross-sectional long-short strategy
- Monte Carlo path-dependent pricing

---

## 🎯 Motivation

In the Chinese convertible bond market:

- High valuation and crowded trading distort pricing.
- Relative valuation metrics (e.g., conversion premium) become unreliable.
- Embedded clauses create strong path dependency.

-> A unified **absolute pricing anchor** is required.

---

## 🧠 Pricing Framework

### 1. Convertible Bond Decomposition

$$
V_{CB} = V_{bond} + V_{option}
$$

- Bond component: discounted cash flows.
- Option component: embedded equity call option.

---

## ⚙️ Model Design

### 🔹 Black-Scholes Model (BS)

Pricing Logic

$$
V_{option} = S e^{-qT} N(d_1) - X e^{-rT} N(d_2)
$$

- Closed-form solution under lognormal stock dynamics.
- Ignores path-dependent clauses.

Key characteristics

- High sensitivity to equity price and volatility.
- No upper bound under call-free assumption.

-> Acts as an **offensive pricing anchor**.

---

### 🔹 Zheng-Lin Model (ZL)

Pricing Logic

Monte Carlo simulation with optimal stopping.

1. Simulate stock price paths.
2. Evaluate clause triggers (Call/Put/Reset).
3. Discount expected payoff.

Model origin and mechanism

- The ZL framework is built on issuer-investor clause game logic under callable/putable/adjustable convertibles.
- It explicitly models optimal issuer decisions (call/reset) and investor response (convert/put/hold) along each path.
- Compared with static closed-form models, it better captures path dependency and clause-triggered nonlinear payoff.
- In this project, ZL is treated as a defensive anchor suitable for downside-risk-aware valuation.

Key characteristics

- Fully path-dependent and clause-aware.
- Captures call cap and reset convexity.
- Produces more conservative valuation.

-> Acts as a **defensive anchor**.

---

### Model Error Comparison

| Error Metric           | BS    | ZL    |
| ---------------------- | ----- | ----- |
| Mean Error (Bias, CNY) | 2.13  | -4.73 |
| MAE (CNY)              | 13.74 | 11.66 |
| RMSE (CNY)             | 29.73 | 30.19 |
| MAPE                   | 9.79% | 7.88% |
| SMAPE                  | 9.66% | 8.14% |

Lower MAE/MAPE/SMAPE indicates better pricing fit.

---

## 📊 Mispricing Signal

$$
Mispricing = V_{model} - V_{market}
$$

- Positive -> undervalued -> long.
- Negative -> overvalued -> short.

---

## 🚀 Strategy Design

### 🔹 Cross-sectional Long-Short Strategy

The strategy is constructed based on **mispricing (RD)** defined above.

- Monthly rebalancing and cross-sectional ranking by RD.
- **Long portfolio**: top 20% (undervalued bonds).
- **Short portfolio**: bottom 20% (overvalued bonds).

### 🔹 Trading Logic

- Mean reversion of temporary mispricing drives excess return.
- Convergence from market price to theoretical value is the core alpha source.

---

## 📈 Results

| Strategy | Annual Return | Sharpe | Max Drawdown |
| -------- | ------------- | ------ | ------------ |
| BS Long  | ~19.3%        | 1.35   | -22.26%      |
| ZL Long  | ~15.3%        | 1.44   | -13.74%      |

Summary: ZL performs better on relative pricing-error control, while BS is stronger in directional capture and offensive upside.

---

## 🧩 Key Insight

- BS captures **valuation expansion + momentum**.
- ZL captures **mean reversion + downside protection**.
- Mispricing factor is highly **orthogonal** to traditional style factors.

-> Combination improves portfolio robustness.

---

## 📉 Limitations

- BS ignores detailed clause constraints.
- ZL is computationally expensive.
- Short leg can underperform during momentum-dominated markets.

---

## 🔮 Future Work

- Event-driven clause modeling.
- ML-based probability estimation.
- Dynamic parameter calibration.
- Integration into multi-factor system.

---

## 📎 Full Report

Full research report available here: /report/full_report.pdf

---

## 🧩 Contribution

- Unified absolute pricing framework.
- Tradable mispricing signal design.
- Full backtesting pipeline.
- Clear separation of offensive vs defensive alpha.

---

## 📚 Citation

If you use this framework or part of this project, please cite the repository and state model assumptions and data boundaries clearly.
