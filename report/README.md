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

## Independent CCB 2023 results

The [CCB 2023 experiment](../experiments/ccb_2023/README.md) has completed pricing
for 400 observed weeks from 2019-01-04 through 2026-10-23. It retains the 2023
protection-period and continuous-barrier framework, omitting puts and future
conversion-price resets. Observed yield curves and contractual inputs extend
the report's fixed-parameter specification at the user's request.

- [Current same-sample comparison](../experiments/ccb_2023/results_waivers_2023/REPORT.md):
  148,704 common bond-date observations across CCB, BS, ZL and LSM. CCB MAE is
  11.3459 yuan and RMSE is 30.8193 yuan.
- [2026-10-29 announcement review](../experiments/ccb_2023/results_waivers_2023/REVIEW_20261029.md):
  seven additional explicit periods, three informational notices and two linked
  replacement versions; 182 notices still lack a usable period. Coverage recovers
  284 observations, with prices on the previous common sample unchanged.
- [Basic CCB comparison](../experiments/ccb_2023/results/REPORT.md): retained as
  the control scenario, together with the original audit snapshots.

The current dataset contains 700 notices with explicit periods and is not certified
exhaustive. Unknown periods remain missing; replacement notices are used only
after their availability date. No default quarter or inference from issuer
silence is applied. CCB outputs remain separate from production BS/ZL/LSM
artifacts and frozen strategy ledgers.

CCBA probability weighting and issuer-behaviour calibration remain outside the
current scope. The 2024 research paper above is retained as a reference.
