"""Render a single current README from verified pricing and research outputs."""
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd

from pricing_diagnostics import common_sample,update_readme_metrics
from pricing_backtest import RESEARCH_MODELS as MODELS, pricing_panel

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent
FACTORS=ROOT/'mispricing factor'
STRATEGY=ROOT/'long-short strategy/weekly_value_results/research'
LABELS={'liquidity':('流动性','Liquidity'),'volatility':('波动率','Volatility'),
    'price_volume':('量价相关性','Price-volume'),'valuation':('估值','Valuation'),
    'momentum':('动量','Momentum'),'bs_deviation':('定价偏差','Mispricing'),
    'zl_deviation':('定价偏差','Mispricing'),'lsm_deviation':('定价偏差','Mispricing'),
    'ccb_deviation':('定价偏差','Mispricing'),'mean_deviation':('均价偏差','Mean-price deviation')}
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
    year = pd.Timestamp(cutoff).to_period('W-FRI').end_time.year
    badge_date = quote(date.replace('-', '--'))
    return f'''# 中国市场可转债定价模型研究 | Convertible Bond Pricing Research

<p align="center">
  <a href="#简体中文"><img src="https://img.shields.io/badge/语言-中文-E84D3D?style=for-the-badge&labelColor=3B3F47" alt="中文"></a>
  &nbsp;
  <a href="#english-version"><img src="https://img.shields.io/badge/Language-English-2F73C9?style=for-the-badge&labelColor=3B3F47" alt="English"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/定价模型-BS%20%C2%B7%20ZL%20%C2%B7%20LSM%20%C2%B7%20CCB-F2C94C?style=for-the-badge" alt="BS + ZL + LSM + CCB">
  <img src="https://img.shields.io/badge/定价截止-{badge_date}-4CAF50?style=for-the-badge" alt="Pricing through {date}">
  <img src="https://img.shields.io/badge/研究频率-周度定价与策略-9B51E0?style=for-the-badge" alt="Weekly pricing and strategy">
</p>

<p align="center">
  <a href="summary/key_findings.md"><img src="https://img.shields.io/badge/研究摘要-阅读-2F73C9?style=for-the-badge" alt="研究摘要"></a>
  <a href="long-short%20strategy/weekly_value_results_pricing_four_{year}/README.md"><img src="https://img.shields.io/badge/{year}周报-查看-4CAF50?style=for-the-badge" alt="{year}周报"></a>
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
        dash={'BS':'B-S','ZL':'Z-L'}.get(model,model)
        metrics=pd.read_csv(FACTORS/f'{dash}_alpha_strategy_metrics.csv')
        chosen=metrics.loc[metrics['因子名称'].eq('六因子等权合成因子')].iloc[0]
        monthly.append([model,*[chosen[k] for k in ('年化超额收益率','夏普比率','最大回撤')]])
    headers=['Model','Factor','Periods','IC','Rank IC','ICIR','Rank ICIR','Positive IC'] if english else ['模型','因子','持有期数','IC','Rank IC','ICIR','Rank ICIR','IC 为正比例']
    month_headers=['Model','Annualized excess return','Sharpe','Max drawdown'] if english else ['模型','年化超额收益','夏普','最大回撤']
    weekly=pd.read_csv(STRATEGY/'metrics.csv')
    rows=[[r.portfolio,number(r.annual_return,True),number(r.sharpe),number(r.max_drawdown,True)] for r in weekly.itertuples()]
    weekly_headers=['Portfolio','Annualized research return','Sharpe','Max drawdown'] if english else ['组合','年化研究收益','夏普','最大回撤']
    cutoff=weekly.period_end.iloc[0]
    weekly_cutoff=f'Daily portfolio accounting through **{cutoff}**.' if english else f'组合日度核算截至 **{cutoff}**。'
    return table(headers,ic),table(month_headers,monthly),weekly_cutoff+'\n\n'+table(weekly_headers,rows)


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


def annual_reports():
    reports = sorted((ROOT/'long-short strategy').glob('weekly_value_results_pricing_four_*/README.md'))
    return ' · '.join(f'[{path.parent.name.rsplit("_",1)[-1]}]({quote(path.relative_to(ROOT).as_posix())})'
                      for path in reports)


def render(english):
    ic,monthly,weekly=research_tables(english)
    lang='en' if english else 'zh'
    pricing=f'<!-- pricing-metrics-{lang}:start -->\n<!-- pricing-metrics-{lang}:end -->'
    if english:
        text=f'''<a id="english-version"></a>

## English Version

[简体中文](#简体中文)

## Overview

This project prices Chinese convertible bonds with Black-Scholes (BS), clause-stopping Monte Carlo (ZL), Least-Squares Monte Carlo (LSM) and CCB with issuer call protection. The four-model arithmetic mean is the main valuation signal for the factor and portfolio research.

## Pricing methods

All four models independently read observed conversion value, volatility and government yields, along with the bond's coupon schedule, redemption amount and conversion dates. Each calibrates a nonnegative credit spread to the observed straight-bond value using the contractual cash flows. Cash payments are discounted with that spread and the government-rate proxy. Shares received on conversion are discounted at the government-rate proxy alone. Volatility uses up to 250 adjusted stock closes, with at least 60 required. Missing or inconsistent inputs are recorded by bond and date.

### Black–Scholes (BS)

BS is the closed-form European conversion benchmark. It carries the observed conversion value to the last permitted conversion date under a risk-neutral lognormal process. On that date the holder chooses between converting into shares and retaining the remaining contractual cash flows. The model values those cash flows at the decision date, including any later redemption payment. Coupons paid before the last conversion date are discounted separately.

Conversion value and stock volatility drive the option component. The model's single exercise date and omission of issuer calls and holder puts make it a benchmark for the equity exposure of the bond. The valuation uses the actual contractual cash alternative as the exercise threshold, and the final coupon already included in the redemption amount is counted once.

### Clause-stopping Monte Carlo (ZL)

ZL simulates 10,000 risk-neutral paths for conversion value. Each path starts from the call and put counters observed on the valuation date. A call can qualify once conversion opens and the contractual rolling count reaches its threshold. A price-triggered put requires the applicable contract window and consecutive observations below the put threshold.

The issuer is assumed to call after the qualifying trigger, with a notice period of 30 simulated trading observations. The holder is assumed to take a qualifying put. The model evaluates conversion through the last permitted conversion date and stops future coupons when the contract terminates. The result is a valuation under a specified exercise policy. Future discretionary conversion-price resets and unverified non-call commitments fall outside that policy.

### Least-Squares Monte Carlo (LSM)

LSM uses the contractual path rules and estimates whether a holder should convert early, exercise an available put or continue holding. Working backward through exercise dates, it regresses future discounted cash flows on conversion value, its square, the call and put counters, and the time left in any call notice. It compares that continuation estimate with the value of each permitted immediate action.

The production model uses 8,192 antithetic training paths to fit the policy and another 8,192 independent valuation paths to price it. Its 192 base exercise intervals are supplemented with contractual boundaries and coupon observations. Each valuation comes from this fitted policy and the bond's cash flows. Reported Monte Carlo standard errors measure sampling uncertainty conditional on that policy.

### CCB with issuer call protection

CCB starts with a call-protection period based on the remaining time before conversion opens and a one-month minimum. When a dated issuer commitment to refrain from calling was public by the valuation close, the remaining commitment period can extend that protection. Date-only and after-close disclosures enter on the next trading session. A latest decision whose period remains unresolved leaves the corresponding CCB price missing.

Once protection ends, the observed contractual call threshold becomes a continuously monitored barrier. Crossing it ends the cash-flow stream and delivers shares. The model uses the observed government yield curve and contractual cash flows in a finite-difference solution, with grid refinement checks before publication. Its holder-put and future reset assumptions follow the 2023 model; the result omits both features.

### Four-model mean

The mean weights BS, ZL, LSM and CCB at 25% each. A bond-date requires four positive finite prices and an identical observed market quote. The four-model comparison and strategy history begin at the first shared weekly valuation in 2019.

[Detailed pricing methods and numerical checks](backtest/PRICING_METHODS.md)

{pricing}

[Pricing metrics](backtest/Pricing_Metrics.csv) · [Current research provenance](backtest/Research_Migration.json) · [Archived rebuild validation](backtest/Pricing_Validation.json)

## Factors and predictive tests

Five observed factors accompany each model's valuation gap: liquidity, volatility, price-volume correlation, valuation and momentum. Valuation is reversed so higher direction-adjusted values consistently represent the proposed favourable direction. Pearson and Spearman correlations measure redundancy. IC and Rank IC use the following holding period's individual-bond returns on the same eligible cross-section within each model. ICIR is mean divided by sample standard deviation and is not annualized.

{ic}

## Portfolio rules and results

The weekly value strategy ranks the four-model mean relative to the same-day close. It selects at most the highest eligible quintile and requires a 2% valuation margin after round-trip friction. One-way friction is 5bp. Each long is capped at 10%, with unused capital in cash. Frozen valuation limits are checked again at the next session's open. Daily accounting includes observed executions, transaction costs, a 3% annual short-borrow assumption and explicitly recorded stale or unexplained marks.

{weekly}

These are research estimates from observed prices and verified cash flows. Complete historical coupons, original financial vintages and short availability remain unverified, so the curves are not certified total returns or prospective live records. Original frozen tracking evidence is never replaced by a retrospectively recalculated signal.

The monthly six-factor portfolios rank equal-weight standardized factors and hold the top quintile. They use each month's last observed pricing snapshot, including the available partial-month endpoint; the whole derived history is recalculated as that endpoint advances. The implementation charges 6bp times the fraction of newly selected names at each rebalance. Excess return is strategy annualized return minus the benchmark annualized return.

{monthly}

## Key figures

{figures(True)}

## Updates and reproducibility

Weekly research archives {annual_reports()}

Every weekly update runs market inputs, independent BS/ZL/LSM/CCB pricing, observed factors, IC, strategy accounting, annual weekly reports, figures and this README before publication. Pricing appends verified new weeks. CCB searches for new issuer notices before valuing new weeks, and factor and portfolio research are recalculated against the resulting four-model baseline. Any failed stage stops publication.

```powershell
python backtest/rebuild_research_outputs.py --full-history --refresh-inputs --promote
```

This research entrypoint uses saved pricing outputs, refreshes strategy inputs and recalculates research. Routine weekly updates run locally with `backtest/weekly_update.bat`, including the preceding data and pricing stages, weekly reports and GitHub publication. GitHub Actions has no scheduled trigger; its manual entrypoint is retained as an optional fallback.

Register the local Windows task with `backtest/setup_weekly_task.ps1`: Fridays at 16:30 Beijing time, using a validated Python environment. The computer must be on, online and the user logged in; missed starts are retried when available. Failed runs retry hourly up to 12 times. Validation failures stop publication. Only generated weekly outputs are committed, preserving unrelated local changes.

## Limitations

Market deviations are not errors against known fair value. Issuer behaviour, dividends, credit treatment, simulation grids and regression policy remain model assumptions. Monte Carlo standard errors measure sampling uncertainty conditional on the fitted policy; they exclude policy-training and model uncertainty. Factor correlation and pricing fit alone do not establish tradable alpha.
'''
    else:
        text=f'''<a id="简体中文"></a>

## 简体中文

[English](#english-version)

## 项目概述

本项目使用 Black-Scholes（BS）、条款停止蒙特卡洛（ZL）、最小二乘蒙特卡洛（LSM）和含不赎回保护期的 CCB 为中国可转债定价。四模型算术平均是因子检验和组合研究的主要估值信号。

## 定价方法

四个模型各自读取估值日观测到的转股价值、正股波动率和国债利率，以及合同票息、到期兑付金额和转股起止日。信用利差用合同现金流与观测债底校准。现金支付按国债利率代理加信用利差折现，转股取得的股票按国债利率代理折现。波动率采用最多 250 个、至少 60 个复权收盘价估算。缺失或不一致的输入按债券和估值日记录。

### Black–Scholes（BS）

BS 给出欧式转股的解析估值。模型在风险中性对数正态假设下计算转股价值到实际最后转股日的分布。持有人在这一天比较转股所得和继续持有剩余合同现金流的价值，后续到期兑付款先折现到该决策日。最后转股日之前的票息单独折现。

转股价值和正股波动率是期权部分的主要驱动。模型只在最后转股日评估转股选择，省略发行人提前赎回和持有人回售，用于观察权益敞口对转债估值的影响。行权门槛取自合同现金流，已计入到期兑付款的末期票息只算一次。

### 条款停止蒙特卡洛（ZL）

ZL 在风险中性假设下模拟 10,000 条转股价值路径，并用估值日前已观测到的条款计数初始化。进入转股期后，模型在合同滚动窗口内累计达到赎回阈值的观察日。价格触发型回售则按合同规定的适用期间和连续低于回售阈值的天数判断。

估值假定发行人在达到触发条件后赎回，设置 30 个模拟交易观察日的通知期，并假定持有人在满足条件时回售。模型保留实际最后转股日的选择权，合约终止后停止计入后续票息。这个价格反映给定条款和行权规则下的现金流价值，未来酌情下修和未核实的不赎回承诺未纳入该规则。

### 最小二乘蒙特卡洛（LSM）

LSM 在合同路径规则下评估持有人何时主动转股、行使已满足条件的回售权，或继续持有。模型从到期向前回溯，用转股价值及其平方、赎回和回售计数、赎回通知剩余时间回归未来折现现金流，再将估计的继续持有价值与各项可行的即时行权价值比较。

生产设定以 8,192 条对偶训练路径拟合行权规则，再用另 8,192 条独立估值路径计算价格。192 个基础行权区间之外还加入合同边界和票息观测点。估值由拟合的行权规则和本券现金流得出。报告中的蒙特卡洛标准误衡量给定行权规则后的抽样不确定性。

### 含不赎回保护期的可转债模型（CCB）

CCB 先确定保护期，取距转股开始日的剩余期限和一个月下限中的较大值。估值日收盘时已经公开、能够确定有效期限的不赎回承诺，可以延长保护期。只有日期或在盘后发布的公告从下一交易日纳入，最新公告期限仍待核实时，该券日的 CCB 估值留空。

保护期结束后，以合同赎回价格阈值作为连续监控的障碍。平价触碰阈值时停止后续现金流并转股。模型使用真实国债期限曲线、合同现金流和观察债底，通过有限差分求解价值，周更对新增样本做网格加密检查。估值沿用 2023 版 CCB 的经济设定，省略持有人回售和未来酌情下修。

### 四模型均价

BS、ZL、LSM、CCB 各占 25%。同一券日需有四个正且有限的估值，并与同一市场报价对齐，才能计算均价。共同回测从 2019 年首个四模型周度截面起算。

[定价方法与数值验证](backtest/PRICING_METHODS.md)

{pricing}

[指标数据](backtest/Pricing_Metrics.csv) · [当前研究来源](backtest/Research_Migration.json) · [历史重建验证](backtest/Pricing_Validation.json)

## 因子与预测检验

每个模型的定价偏差与五个观测因子共同检验：流动性、波动率、量价相关性、估值和动量。估值反向后，各因子统一为数值越高代表预设有利方向。Pearson 与 Spearman 相关用于检查冗余；IC 与 Rank IC 使用同一模型内共同合格截面和下一持有期个券收益。ICIR 为均值除以样本标准差，不年化。

{ic}

## 组合规则与结果

周度策略按四模型均价相对当日收盘价的估值余量排名，在风险合格池中最多选取前 20%，要求扣除往返成本后的估值余量至少为 2%。单边综合摩擦 5bp，单券最多 10%，未使用资金保留现金。下一交易日开盘重新检查冻结估值给出的买入上限。账户按日核算模拟成交、费用、年化 3% 的假设借券成本和有记录的陈旧估值。

{weekly}

净值是观测价格与已核验现金流的研究估计。历史付息、原始财务版本和券源仍未全部核验，因此不能称为完整总收益或事前实盘记录。事后重算信号不会覆盖原有交易前冻结证据。

月度六因子组合对标准化因子等权合成并持有排名前 20%，使用每个月最后一个已观察定价截面，包含当前未完结月份的可用端点；端点随周更新时整体重算派生历史。实现按每次新入选持仓比例扣除 6bp 换仓成本；年化超额为组合与基准年化收益之差。

{monthly}

## 关键图表

{figures(False)}

## 更新与复现

历年研究周报 {annual_reports()}

每次周更新依次执行行情输入、四模型独立定价、观测因子、IC、策略核算、年度逐周报告、图表和 README。定价只追加已验证边界之后的交易周。CCB 先增量检索发行人公告，再为新增交易周估值，因子与组合研究按同一套定价结果重新计算。任一步失败即停止发布。

```powershell
python backtest/rebuild_research_outputs.py --full-history --refresh-inputs --promote
```

该研究入口读取已有定价、刷新策略输入并重算研究结果。常规周更新在本地运行 `backtest/weekly_update.bat`，包括前置数据和定价更新、周报生成及推送 GitHub。GitHub Actions 已取消定时触发，仅保留手动备用入口。

通过 `backtest/setup_weekly_task.ps1` 注册 Windows 本地任务：每周五北京时间 16:30，使用已验证的 Python 环境。电脑需开机、联网且用户已登录；错过时间后在条件满足时补跑，失败后每小时重试一次、最多12次。校验失败停止发布，自动提交仅包含周更新产物，保留无关的本地修改。

## 局限性

市场报价偏差不等于相对已知公允价值的误差。发行人行为、股息、信用处理、模拟网格与回归策略仍有模型假设。蒙特卡洛标准误只反映给定策略下的抽样误差，不覆盖训练策略和模型假设的不确定性。相关性和定价拟合不能单独证明可交易的 Alpha。
'''
    refs='\n'.join(f'- [{label}](report/{name})' for label,name in REFERENCES)
    return text+'\n## '+('References' if english else '参考资料')+'\n\n'+refs+'\n'


def refresh_summary(metrics, weeks):
    path = ROOT/'summary/key_findings.md'
    title, _, body = path.read_text(encoding='utf-8').partition('\n')
    start = '<!-- four-model-summary:start -->'
    end = '<!-- four-model-summary:end -->'
    if start in body:
        _, _, body = body.partition(end)
    else:
        body = '\n\n## 历史研究档案 | Historical Research Archive\n'+body
    mean = metrics.set_index('model').loc['Mean']
    weekly = pd.read_csv(STRATEGY/'metrics.csv')
    rows = [[r.portfolio, number(r.annual_return, True), number(r.sharpe), number(r.max_drawdown, True)]
            for r in weekly.itertuples()]
    results = table(['组合 / Portfolio','年化研究收益 / Annualized estimate','夏普 / Sharpe','最大回撤 / Max drawdown'], rows)
    block = f"""{start}
## 四模型均价研究 | Four-Model Mean

定价共同样本覆盖 {mean.start} 至 {mean.end} 的 {weeks} 周，共 {mean.n:,} 个券日。BS、ZL、LSM 和 CCB 各占 25%，均价相对市场的 MAE 为 {mean.mae:.2f} 元，RMSE 为 {mean.rmse:.2f} 元。

The four-model arithmetic mean uses the same {mean.n:,} observations over {weeks} weeks. The table reports daily-accounted research results.

{results}

周度研究使用下一交易日开盘复核和交易成本，历史付息与理论空头券源仍有缺口。 [定价、因子及策略结果](../README.md)
{end}"""
    path.write_text(f'{title}\n\n{block}{body}', encoding='utf-8')


def main():
    metrics=pd.read_csv(BASE/'Pricing_Metrics.csv')
    models,market=pricing_panel(BASE)
    valid=common_sample(models,market)
    if set(metrics.model)!=set(MODELS) or not metrics.n.eq(int(valid.sum().sum())).all():
        raise ValueError('Pricing metrics do not match the current common sample')
    quoted=(np.isfinite(market)&market.gt(0)).sum(axis=1)
    coverage=valid.sum(axis=1).div(quoted).mul(100)
    path=ROOT/'README.md'
    text=header(market.index.max())+render(False)+'\n---\n\n'+render(True)
    path.write_text(text,encoding='utf-8')
    update_readme_metrics(path,metrics,len(market),float(coverage.min()),int(valid.iloc[-1].sum()),int(quoted.iloc[-1]))
    refresh_summary(metrics, len(market))
    print('README refreshed from current pricing, factor and strategy outputs')


if __name__=='__main__':main()
