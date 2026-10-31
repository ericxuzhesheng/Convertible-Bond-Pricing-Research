"""Render a single current README from verified pricing and research outputs."""
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd

from pricing_diagnostics import common_sample,update_readme_metrics

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent
FACTORS=ROOT/'mispricing factor'
STRATEGY=ROOT/'long-short strategy/weekly_value_results/research'
MODELS=('BS','ZL','LSM')
LABELS={'liquidity':('流动性','Liquidity'),'volatility':('波动率','Volatility'),
    'price_volume':('量价相关性','Price-volume'),'valuation':('估值','Valuation'),
    'momentum':('动量','Momentum'),'bs_deviation':('定价偏差','Mispricing'),
    'zl_deviation':('定价偏差','Mispricing'),'lsm_deviation':('定价偏差','Mispricing')}
REFERENCES=[
    ('郑振龙、林海（2004）— 中国可转换债券定价研究，厦门大学学报（哲学社会科学版）第2期，93–99页','中国可转换债券定价研究.pdf'),
    ('中国可转债定价模型比较研究','A%20Comparison%20of%20Chinese%20Convertible%20Bond%20Pricing%20Models.pdf'),
    ('Northeast Securities — Convertible Bond Research Framework','Northeast_Securities_Convertible_Bond_Research_Framework_Theory_to_Strategy_20240823.pdf'),
    ('Guosheng Securities — Pricing Models and Applications','Guosheng_Securities_Convertible_Bond_Pricing_Models_and_Applications_20230302.pdf'),
    ('Zheshang Securities — Pricing Panorama','Zheshang_Securities_Convertible_Bond_Pricing_Panorama_Models_Algorithms_and_Applications_20231201.pdf'),
    ('Guosheng Securities — Redemption Probability Adjusted Pricing','Guosheng_Securities_Convertible_Bond_Redemption_Probability_Adjusted_Pricing_Model_20240208.pdf'),
    ('Zheshang Securities — Option Pricing, Factor Enhancement and Clause Games','Zheshang_Securities_Convertible_Bond_Quantitative_Investing_Option_Pricing_Factor_Enhancement_and_Clause_Games_20250918.pdf')]


def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join('---' for _ in headers)+'|',
        *['| '+' | '.join(map(str,row))+' |' for row in rows]])


def header(cutoff):
    date = pd.Timestamp(cutoff).strftime('%Y-%m-%d')
    year = pd.Timestamp(cutoff).year
    badge_date = quote(date.replace('-', '--'))
    return f'''# 中国市场可转债定价模型研究 | Convertible Bond Pricing Research

<p align="center">
  <a href="#简体中文"><img src="https://img.shields.io/badge/语言-中文-E84D3D?style=for-the-badge&labelColor=3B3F47" alt="中文"></a>
  &nbsp;
  <a href="#english-version"><img src="https://img.shields.io/badge/Language-English-2F73C9?style=for-the-badge&labelColor=3B3F47" alt="English"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/定价模型-BS%20%C2%B7%20ZL%20%C2%B7%20LSM-F2C94C?style=for-the-badge" alt="BS + ZL + LSM">
  <img src="https://img.shields.io/badge/定价截止-{badge_date}-4CAF50?style=for-the-badge" alt="Pricing through {date}">
  <img src="https://img.shields.io/badge/研究频率-周度定价与策略-9B51E0?style=for-the-badge" alt="Weekly pricing and strategy">
</p>

<p align="center">
  <a href="summary/key_findings.md"><img src="https://img.shields.io/badge/研究摘要-阅读-2F73C9?style=for-the-badge" alt="研究摘要"></a>
  <a href="long-short%20strategy/weekly_value_results_pricing_v2_{year}/README.md"><img src="https://img.shields.io/badge/{year}周报-查看-4CAF50?style=for-the-badge" alt="{year}周报"></a>
  <a href="report/README.md"><img src="https://img.shields.io/badge/研报索引-PDF-E84D3D?style=for-the-badge" alt="研报索引"></a>
</p>

---

'''


def number(value,percent=False):
    if pd.isna(value):return 'NA'
    return f'{value:.2%}' if percent else f'{value:.2f}'


def research_tables(english):
    ic=[];monthly=[]
    for model in MODELS:
        summary=pd.read_csv(FACTORS/f'{model}_factor_ic_summary.csv')
        for row in summary.itertuples():
            ic.append([model,LABELS[row.factor][int(english)],int(row.periods),f'{row.mean_ic:.3f}',
                f'{row.mean_rank_ic:.3f}',number(row.icir),number(row.rank_icir),f'{row.ic_positive_ratio:.1%}'])
        dash={'BS':'B-S','ZL':'Z-L','LSM':'LSM'}[model]
        metrics=pd.read_csv(FACTORS/f'{dash}_alpha_strategy_metrics.csv')
        chosen=metrics.loc[metrics['因子名称'].eq('六因子等权合成因子')].iloc[0]
        monthly.append([model,*[chosen[k] for k in ('年化超额收益率','夏普比率','最大回撤')]])
    headers=['Model','Factor','Periods','IC','Rank IC','ICIR','Rank ICIR','Positive IC'] if english else ['模型','因子','持有期数','IC','Rank IC','ICIR','Rank ICIR','IC 为正比例']
    month_headers=['Model','Annualized excess return','Sharpe','Max drawdown'] if english else ['模型','年化超额收益','夏普','最大回撤']
    weekly=pd.read_csv(STRATEGY/'metrics.csv')
    rows=[[r.portfolio,number(r.annual_return,True),number(r.sharpe),number(r.max_drawdown,True)] for r in weekly.itertuples()]
    weekly_headers=['Portfolio','Annualized research return','Sharpe','Max drawdown'] if english else ['组合','年化研究收益','夏普','最大回撤']
    return table(headers,ic),table(month_headers,monthly),table(weekly_headers,rows)


def figures(english):
    parts=['### Strategy NAV and convertible-bond ETF' if english else '### 策略与转债 ETF 净值对比',
        '![Strategy and convertible-bond ETF NAV](long-short%20strategy/weekly_value_results/research/daily_nav.png)',
        ('Long-only, theoretical long-short, 000832.CSI and 511380.SH each start at 1 on their own first observation. Full strategy history is retained; ETF dates before listing stay empty. The ETF uses observed closes times same-day adjustment factors. With different inception points, the chart provides a broad trend comparison.' if english else
         '多头、理论多空、中证转债指数与 511380.SH 按各自首个观测值归一化为 1。保留策略完整区间，ETF 使用真实收盘价与同日复权因子，上市前留空，作大致走势参考。'),
        '### Pricing deviations and uncertainty' if english else '### 定价偏差与抽样不确定性']
    for model in MODELS:
        parts.append(f'![{model} model and market prices](backtest/Fig1_{model}_Price_Time_Series.png)')
    for name in ('Deviation_Distribution','Annual_Deviation','Coverage_Uncertainty'):
        parts.append(f'![{name}](backtest/Fig_Pricing_{name}.png)')
    parts.extend(['### Monthly factor portfolios' if english else '### 月度因子多头组合'])
    for model in MODELS:
        parts.append(f'![{model} factor portfolios](long-short%20strategy/{model}_model_performance.png)')
    parts.append('### Factor correlation and IC' if english else '### 因子相关性与 IC')
    for model in MODELS:
        parts.append(f'![{model} Pearson and Spearman correlation](mispricing%20factor/{model}_factor_correlation.png)')
    parts.append('![Factor IC and Rank IC](mispricing%20factor/factor_ic_comparison.png)')
    return '\n\n'.join(parts)


def render(english):
    ic,monthly,weekly=research_tables(english)
    lang='en' if english else 'zh'
    pricing=f'<!-- pricing-metrics-{lang}:start -->\n<!-- pricing-metrics-{lang}:end -->'
    if english:
        text=f'''<a id="english-version"></a>

## English Version

[简体中文](#简体中文)

## Overview

This project compares Black-Scholes (BS), clause-stopping Monte Carlo (ZL), and Least-Squares Monte Carlo (LSM) for Chinese convertible bonds. Pricing deviations, factor predictiveness and investable portfolio behaviour are evaluated separately.

## Pricing methods

In the referenced research, CCB provides analytical pricing for a specified call-protection period. The independent [CCB 2023 experiment](experiments/ccb_2023/README.md) now uses observed yield curves and verified non-call commitments. CCBA's issuer-behaviour probability weighting has not been implemented.

All models read observed inputs independently. Contractual coupons, maturity payments, conversion dates and termination rules use the same conventions. Cash payments receive credit discounting; share conversion uses the government-rate proxy. Missing or inconsistent inputs remain excluded with recorded reasons.

- **BS** values European conversion with actual conversion expiry and contractual cash value. Issuer call and holder put options are outside this baseline.
- **ZL** simulates observed clause counters, conversion boundaries and cash flows. Calling after a qualifying trigger with a 30-observation notice period is a disclosed policy assumption. Future discretionary resets and unverified waivers are not invented.
- **LSM** adds voluntary early conversion and optional puts using continuation regression. It uses 8,192 training paths and another 8,192 independent valuation paths, antithetic sampling and 192 base exercise intervals with contractual boundaries. It does not read ZL prices or impose a ZL floor.

### Independent CCB 2023 experiment (2026-10-29)

The experiment covers 2019-01-04 through 2026-10-23 and now includes 700 notices with explicit protection periods. Reviewing the previous 194-item queue resolved seven periods, classified three informational notices and linked two superseded drafts to their replacements; 182 periods remain unfilled. This restores 284 bond-date observations and expands the common sample with saved BS/ZL/LSM prices to **148,704 observations, 974 bonds and 400 weeks**.

On that common sample, CCB with explicit commitments has **MAE 11.3459 yuan and RMSE 30.8193 yuan**. All 18 announcement tests pass; 112 refined-grid checks have a maximum difference of 0.0068 yuan, below the 0.05-yuan numerical tolerance. Prices on the previous common sample are unchanged: this review increases coverage. See the [comparison report](experiments/ccb_2023/results_waivers_2023/REPORT.md) and [notice audit](experiments/ccb_2023/results_waivers_2023/REVIEW_20261029.md). CCB remains independent of production strategies; the three-model production metrics below retain their original sample.

{pricing}

[Pricing metrics](backtest/Pricing_Metrics.csv) · [Current research provenance](backtest/Research_Migration.json) · [Archived rebuild validation](backtest/Pricing_Validation.json)

## Factors and predictive tests

Five observed factors accompany each model's valuation gap: liquidity, volatility, price-volume correlation, valuation and momentum. Valuation is reversed so higher direction-adjusted values consistently represent the proposed favourable direction. Pearson and Spearman correlations measure redundancy. IC and Rank IC use the following holding period's individual-bond returns on the same eligible cross-section within each model. ICIR is mean divided by sample standard deviation and is not annualized.

{ic}

## Portfolio rules and results

The weekly value strategy averages the three model prices, selects at most the highest eligible quintile and requires a 2% valuation margin after round-trip friction. One-way friction is 5bp; each long is capped at 10%, with unused capital in cash. Frozen valuation limits are checked again at the next session's open. Daily accounting includes observed executions, transaction costs, a 3% annual short-borrow assumption and explicitly recorded stale or unexplained marks.

{weekly}

These are research estimates from observed prices and verified cash flows. Complete historical coupons, original financial vintages and short availability remain unverified, so the curves are not certified total returns or prospective live records. Original frozen tracking evidence is never replaced by a retrospectively recalculated signal.

The monthly six-factor portfolios rank equal-weight standardized factors and hold the top quintile. They use each month's last observed pricing snapshot, including the available partial-month endpoint; the whole derived history is recalculated as that endpoint advances. The implementation charges 6bp times the fraction of newly selected names at each rebalance. Excess return is strategy annualized return minus the benchmark annualized return.

{monthly}

## Key figures

{figures(True)}

## Updates and reproducibility

Every weekly update runs market inputs, independent BS/ZL/LSM pricing, observed factors, IC, strategy accounting, annual weekly reports, figures and this README before publication. Annual reports are generated and checked in staging, then committed and pushed together, including newly created weeks. Any failed stage stops publication. Pricing appends only verified new weeks; factor and portfolio outputs are recalculated against the same pricing baseline. Input fingerprints and artifact manifests detect changed data and model prices even when the cutoff is unchanged. Git preserves replaced results.

```powershell
python backtest/rebuild_research_outputs.py --full-history --refresh-inputs --promote
```

This research entrypoint uses saved pricing outputs, refreshes strategy inputs and recalculates research. Routine weekly updates run locally with `backtest/weekly_update.bat`, including the preceding data and pricing stages, weekly reports and GitHub publication. GitHub Actions has no scheduled trigger; its manual entrypoint is retained as an optional fallback.

Register the local Windows task with `backtest/setup_weekly_task.ps1`: Fridays at 16:30 Beijing time, using a validated Python environment. The computer must be on, online and the user logged in; missed starts are retried when available. Validation failures stop publication. Only generated weekly outputs are committed, preserving unrelated local changes.

## Limitations

Market deviations are not errors against known fair value. Issuer behaviour, dividends, credit treatment, simulation grids and regression policy remain model assumptions. Monte Carlo standard errors measure sampling uncertainty conditional on the fitted policy; they exclude policy-training and model uncertainty. Factor correlation and pricing fit alone do not establish tradable alpha.
'''
    else:
        text=f'''<a id="简体中文"></a>

## 简体中文

[English](#english-version)

## 项目概述

本项目比较中国可转债的 Black-Scholes（BS）、条款停止蒙特卡洛（ZL）和最小二乘蒙特卡洛（LSM）定价，并分别检验定价偏差、因子预测能力和组合表现。定价更贴近市场报价，并不直接意味着策略收益更高。

## 定价方法

文献中的 CCB 在给定不赎回保护期下进行解析定价。本项目已完成独立的[CCB 2023实验](experiments/ccb_2023/README.md)，使用实际利率曲线并纳入可核实的不赎回承诺；CCBA的发行人行为概率加权尚未实施。

三个模型独立读取真实观测输入，统一使用合同票息、到期支付、转股日期和条款终止规则。现金支付考虑信用折现，转股支付使用国债利率代理。缺失或不一致输入保留排除原因。

- **BS** 使用真实转股到期日和剩余合同现金价值计算欧式转股基准，不显式处理发行人赎回与持有人回售。
- **ZL** 模拟真实条款计数、转股边界与存续现金流。触发后赎回及 30 个模拟交易观察日的通知期是明确的政策假设；不编造未来下修或未核实的不赎回承诺。
- **LSM** 通过继续价值回归加入自愿提前转股与回售选择，使用 8,192 条训练路径和另 8,192 条独立估值路径、对偶抽样及 192 个基础行权区间，并加入合同边界。不读取 ZL 价格，也不使用 ZL 估值下限。

### 独立CCB 2023实验（2026-10-29）

覆盖2019-01-04至2026-10-23。最新公告复核后，共有700份明确期限公告；原194份待核文件中，7份补出期限、3份确认为信息性文件、2份关联更新版，182份仍不猜填。恢复284个券日，CCB与已有BS/ZL/LSM的共同样本增至**148,704个券日、974只债券、400周**。

该共同样本上，加入明确承诺的CCB **MAE为11.3459元、RMSE为30.8193元**。18项公告测试通过，112个网格加密样本最大差异0.0068元，低于0.05元数值容差。原共同样本的价格保持不变，本次改善的是覆盖范围。见[同样本比较报告](experiments/ccb_2023/results_waivers_2023/REPORT.md)和[逐项公告复核](experiments/ccb_2023/results_waivers_2023/REVIEW_20261029.md)。CCB保留在独立研究目录，未接入生产策略；下方三模型生产指标仍使用各自原定样本。

{pricing}

[指标数据](backtest/Pricing_Metrics.csv) · [当前研究来源](backtest/Research_Migration.json) · [历史重建验证](backtest/Pricing_Validation.json)

## 因子与预测检验

每个模型的定价偏差与五个观测因子共同检验：流动性、波动率、量价相关性、估值和动量。估值反向后，各因子统一为数值越高代表预设有利方向。Pearson 与 Spearman 相关用于检查冗余；IC 与 Rank IC 使用同一模型内共同合格截面和下一持有期个券收益。ICIR 为均值除以样本标准差，不年化。

{ic}

## 组合规则与结果

周度均价策略使用三模型理论价的算术平均，在风险合格池中最多选取前 20%，要求扣除往返成本后的估值余量至少为 2%。单边综合摩擦 5bp，单券最多 10%，未使用资金保留现金；下一交易日开盘重新检查冻结估值给出的买入上限。每日核算实际模拟成交、费用、按年化 3% 假设计提的借券成本，以及有记录的陈旧或无法解释估值。

{weekly}

净值是观测价格与已核验现金流的研究估计。历史付息、原始财务版本和券源仍未全部核验，因此不能称为完整总收益或事前实盘记录。事后重算信号不会覆盖原有交易前冻结证据。

月度六因子组合对标准化因子等权合成并持有排名前 20%，使用每个月最后一个已观察定价截面，包含当前未完结月份的可用端点；端点随周更新时整体重算派生历史。实现按每次新入选持仓比例扣除 6bp 换仓成本；年化超额为组合与基准年化收益之差。

{monthly}

## 关键图表

{figures(False)}

## 更新与复现

每次周更新依次执行行情输入、三模型独立定价、观测因子、IC、策略核算、年度逐周报告、图表和 README，任一步失败即停止发布。周报在暂存目录生成并校验后，与其他结果一起提交和推送，新增周报也纳入提交。定价只追加已验证边界之后的交易周，因子与组合结果按同一套定价输入重新计算。输入指纹和产物 Manifest 会识别相同截止日下的数据或定价变更，替换前的结果由 Git 保留。

```powershell
python backtest/rebuild_research_outputs.py --full-history --refresh-inputs --promote
```

该研究入口读取已有定价、刷新策略输入并重算研究结果。常规周更新在本地运行 `backtest/weekly_update.bat`，包括前置数据和定价更新、周报生成及推送 GitHub。GitHub Actions 已取消定时触发，仅保留手动备用入口。

通过 `backtest/setup_weekly_task.ps1` 注册 Windows 本地任务：每周五北京时间 16:30，使用已验证的 Python 环境。电脑需开机、联网且用户已登录；错过时间后在条件满足时补跑。校验失败停止发布，自动提交仅包含周更新产物，保留无关的本地修改。

## 局限性

市场报价偏差不等于相对已知公允价值的误差。发行人行为、股息、信用处理、模拟网格与回归策略仍有模型假设。蒙特卡洛标准误只反映给定策略下的抽样误差，不覆盖训练策略和模型假设的不确定性。相关性和定价拟合不能单独证明可交易的 Alpha。
'''
    refs='\n'.join(f'- [{label}](report/{name})' for label,name in REFERENCES)
    return text+'\n## '+('References' if english else '参考资料')+'\n\n'+refs+'\n'


def main():
    metrics=pd.read_csv(BASE/'Pricing_Metrics.csv')
    market=pd.read_csv(BASE/'Market_Prices.csv',index_col=0,parse_dates=True)
    models={model:pd.read_csv(BASE/f'{model}_Model_Prices.csv',index_col=0,parse_dates=True) for model in MODELS}
    valid=common_sample(models,market)
    if set(metrics.model)!=set(MODELS) or not metrics.n.eq(int(valid.sum().sum())).all():
        raise ValueError('Pricing metrics do not match the current common sample')
    quoted=(np.isfinite(market)&market.gt(0)).sum(axis=1)
    coverage=valid.sum(axis=1).div(quoted).mul(100)
    path=ROOT/'README.md'
    text=header(market.index.max())+render(False)+'\n---\n\n'+render(True)
    path.write_text(text,encoding='utf-8')
    update_readme_metrics(path,metrics,len(market),float(coverage.min()),int(valid.iloc[-1].sum()),int(quoted.iloc[-1]))
    print('README refreshed from current pricing, factor and strategy outputs')


if __name__=='__main__':main()
