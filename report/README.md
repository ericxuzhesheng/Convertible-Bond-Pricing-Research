# 报告版本索引

- 2026年修正版定价周报：[43周历史回顾索引](../long-short%20strategy/weekly_value_results_pricing_v2_2026/README.md)，截至2026-10-23。采用修正后的BS/ZL/LSM价格，保留历史事件与现金流限制，不冒充事前冻结记录。

- 当前独立策略：[周度均价安全边际策略回测（2026-09-12）](../summary/weekly_value_strategy_20260912.md)。单边5bp、扣费后2%门槛；2019年起每日核算，附原始结果链接。
- 方法说明：[WEEKLY_VALUE_STRATEGY.md](../long-short%20strategy/WEEKLY_VALUE_STRATEGY.md)。
- 历史定价与六因子研究：[CB_pricing_full.pdf](CB_pricing_full.pdf)，更新页及统计截止2026-08-28，保留原PDF，不代表当前独立策略结果。
- 更早正文：[HISTORICAL_BASE_20260310.pdf](HISTORICAL_BASE_20260310.pdf)，仅作理论和历史研究参考。
- 旧周度相对排名验收：[2026-09-11记录](../summary/weekly_strategy_20260911.md)。其事件分类曾存在错误，仅保留审计，不能作为新策略绩效证据。

The current weekly value strategy is a separate study. Older pricing/factor PDFs retain their dated figures; they are not restated as results of the new 5bp/2% rule.

## Academic references

- 郑振龙、林海（2004）：[《中国可转换债券定价研究》](中国可转换债券定价研究.pdf)，《厦门大学学报（哲学社会科学版）》第2期（总第162期），93–99页。作为中国转债条款与发行人行为建模的理论参考；当前项目的 ZL 标签不表示完整复现原文模型。

## Broker research references

English titles below are descriptive translations; the original Chinese PDF contents are unchanged.

- Guosheng Securities (2023-03-02), [*Convertible Bond Pricing Models and Applications*](Guosheng_Securities_Convertible_Bond_Pricing_Models_and_Applications_20230302.pdf). CCB assumptions and analytical cash-flow decomposition, pp. 5-9.
- Zheshang Securities (2023-12-01), [*Convertible Bond Pricing Panorama: Models, Algorithms and Applications*](Zheshang_Securities_Convertible_Bond_Pricing_Panorama_Models_Algorithms_and_Applications_20231201.pdf). LSM pricing with contractual clauses, issuer behaviour and credit risk.
- Guosheng Securities (2024-02-08), [*Redemption Probability Adjusted Pricing Model for Convertible Bonds*](Guosheng_Securities_Convertible_Bond_Redemption_Probability_Adjusted_Pricing_Model_20240208.pdf). CCBA construction and announcement-based calibration, pp. 7-19.
- Zheshang Securities (2025-09-18), [*Convertible Bond Quantitative Investing: Option Pricing, Factor Enhancement and Clause Games*](Zheshang_Securities_Convertible_Bond_Quantitative_Investing_Option_Pricing_Factor_Enhancement_and_Clause_Games_20250918.pdf). LSM applications, factor strategies and clause-exercise prediction.

## Next model experiment: CCB before CCBA

Start with an independent CCB analytical benchmark. In the 2023 Guosheng report,
CCB uses a specified call-protection period followed by immediate redemption at
the price barrier; it omits puts and conversion-price resets. This differs from
the rolling trigger counts and notice period in the current ZL/LSM engines.
Validate the analytical result against a simulation with the same assumptions,
then test protection-period sensitivity. Historical comparisons require verified
as-of waiver dates; a declared protection-period scenario is not an observed
issuer decision. Do not calibrate protection periods to the same convertible
market prices later used to claim pricing accuracy.

A fixed-parameter CCB numerical experiment can run alongside the pricing
migration: keep its code and outputs separate, read only stable input snapshots,
and do not write production caches, manifests or strategy results. Complete
historical announcement coverage is not needed for this numerical experiment.

CCBA is the subsequent issuer-behaviour experiment. The 2024 Guosheng report
defines it as a probability-weighted sum of CCB prices over protection periods.
Its rolling Logistic calibration requires historical call/non-call announcements,
their availability dates and waiver end dates. The current event-cache metadata
explicitly leaves trigger/waiver history incomplete, and some announcement
records lack `available_at`. Verify these inputs and fit only on information
available at each valuation date before making historical CCBA claims. Neither
model has been implemented or backtested by this documentation update; the
existing BS/ZL/LSM baseline and research-migration guards remain in force.
