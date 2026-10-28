"""Versioned 2026 weekly research reports using the corrected pricing baseline."""
import hashlib
import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from weekly_strategy_core import Inputs, ValueConfig
from weekly_ensemble_strategy import run_one, LIMITATION

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'backtest'
OUT = Path(__file__).resolve().parent / 'weekly_value_results_pricing_v2_2026'


def main(*, year=2026, output_dir=OUT):
    # Weekly publication supplies a fresh stage; existing provenance remains guarded.
    OUT = Path(output_dir)
    provenance = {}
    for model in ('BS', 'ZL', 'LSM'):
        path = BASE / f'{model}_Model_Manifest.json'
        raw = path.read_bytes()
        manifest = json.loads(raw)
        provenance[model] = dict(manifest_sha256=hashlib.sha256(raw).hexdigest(), cutoff=manifest['input_cutoff'])
    if len({v['cutoff'] for v in provenance.values()}) != 1:
        raise ValueError('Model cutoffs differ')
    OUT.mkdir(parents=True, exist_ok=True)
    proof = OUT / 'pricing_sources.json'
    if proof.exists() and json.loads(proof.read_text(encoding='utf-8')) != provenance:
        raise ValueError('Pricing baseline changed; choose a new versioned output directory')
    proof.write_text(json.dumps(provenance, indent=2), encoding='utf-8')
    data = Inputs(BASE, BASE / 'strategy_inputs')
    start = pd.Timestamp(year=year, month=1, day=1)
    end = min(pd.Timestamp(data.metadata['cutoff']), pd.Timestamp(provenance['BS']['cutoff']),
              pd.Timestamp(year=year, month=12, day=31))
    if end < start:
        raise ValueError('No verified inputs for the requested year')
    config = ValueConfig(risk_mode='research')
    run_one(data, config, start, end, OUT / 'research')
    ranks = pd.read_csv(OUT / 'research/rankings.csv')
    ranks['signal_date'] = pd.to_datetime(ranks.signal_date)
    navs = {p: pd.read_csv(OUT / f'research/{p}_daily_nav.csv', parse_dates=['date']) for p in ('long_only','long_short')}
    dates = data.signal_dates(start, end, 'weekly')
    weeks = pd.period_range(start, end, freq='W-FRI')
    reports = OUT / 'weeks'
    reports.mkdir(exist_ok=True)
    index = []
    for week in weeks:
        week_end = week.end_time.normalize()
        if week_end > end:
            continue
        observed = dates[(dates >= week.start_time) & (dates <= week_end)]
        lines = [f'# 周报 {week_end.date()}（修正版定价历史回顾）', '',
                 '本报告现在回补，非当时发布或冻结的信号。采用 cashflow-pricing-v2 的 BS/ZL/LSM，CCB 尚未纳入。', '', LIMITATION, '']
        if len(observed) == 0:
            lines += ['本自然周没有已验证的新增定价截面，不生成或沿用虚构的新信号。']
            index.append(dict(week=str(week_end.date()), signal_date=None, status='no_new_observed_pricing', selected=0))
        else:
            date = observed[-1]
            part = ranks[ranks.signal_date.eq(date)]
            if part.empty:
                raise ValueError(f'Missing ranking {date}')
            selected = part[part.long_selected.eq(True)].sort_values('signal', ascending=False)
            lines += [f'实际估值日：{date.date()}；下次开盘复核日：{part.execution_date.iloc[0]}。',
                      f'有报价 {len(part)} 只，风险筛选合格 {int(part.eligible.sum())} 只，多头候选 {len(selected)} 只。',
                      '规则：单边5bp、扣往返成本后估值余量至少2%、前20%名额、单券最多10%。', '',
                      '|模型|同样本数量|MSE（元²）|RMSE（元）|MAE（元）|MAPE|', '|---|---:|---:|---:|---:|---:|']
            matched = part[np.isfinite(part[['BS','ZL','LSM','market_price']]).all(axis=1) & part[['BS','ZL','LSM','market_price']].gt(0).all(axis=1)]
            for model in ('BS','ZL','LSM'):
                error = matched[model] - matched.market_price
                lines.append(f'|{model}|{len(matched)}|{(error**2).mean():.2f}|{np.sqrt((error**2).mean()):.2f}|{error.abs().mean():.2f}|{(error.abs()/matched.market_price).mean():.2%}|')
            lines += ['', '误差衡量相对市场报价的差异，不代表已知公允价值误差。', '',
                      '|代码|转债|目标权重|扣费后估值余量|开盘买入上限（元）|', '|---|---|---:|---:|---:|']
            for row in selected.itertuples():
                lines.append(f'|{row.code}|{row.name}|{min(.1,1/len(selected)):.1%}|{row.roundtrip_net_upside:.2%}|{np.floor(row.long_entry_limit*1000)/1000:.3f}|')
            if selected.empty:
                lines.append('|—|无符合条件的多头候选|0%|—|—|')
            lines += ['', '当周研究账户变化（对应本周已有持仓，非本周末信号的未来收益）：']
            for portfolio, nav in navs.items():
                sample = nav[(nav.date >= week.start_time) & (nav.date <= week_end)]
                if sample.empty:
                    continue
                if not sample.marked_prices_verified.eq(True).all():
                    lines.append(f'- {portfolio}：有未解释估值，暂不报告当周收益。')
                else:
                    ret = (1+sample['return']).prod()-1
                    lines.append(f'- {portfolio}：当周研究收益 {ret:.2%}；现金流不完整，非完整总收益。')
            index.append(dict(week=str(week_end.date()), signal_date=str(date.date()), status='historical_research', selected=len(selected)))
        lines += ['', '来源：[版本记录](../pricing_sources.json)、[逐券排名](../research/rankings.csv)、[成交与持仓目录](../research/)。']
        (reports / f'{week_end.date()}.md').write_text('\n'.join(lines)+'\n', encoding='utf-8')
    pd.DataFrame(index).to_csv(OUT / 'report_index.csv', index=False)
    assert {x['signal_date'] for x in index if x['signal_date']} == {str(x.date()) for x in dates}
    assert len(index) == len(list(reports.glob('*.md')))
    summary = dict(status='passed', report_weeks=len(index), observed_signal_weeks=len(dates),
                   start=str(start.date()), end=str(end.date()), pricing_version='cashflow-pricing-v2',
                   scope='Retrospective research reports; no historical frozen signals or complete-total-return claim')
    (OUT / 'validation.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    links = '\n'.join(f'- [{r["week"]}](weeks/{r["week"]}.md)：{r["status"]}' for r in index)
    (OUT / 'README.md').write_text(f'# {year}年周报（修正版定价历史回顾）\n\n'+LIMITATION+'\n\n旧版冻结账本保留；本目录仅为事后研究。CCB尚未纳入。\n\n'+links+'\n', encoding='utf-8')
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year', type=int, default=2026)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    main(year=args.year, output_dir=args.output_dir or OUT.with_name(f'weekly_value_results_pricing_v2_{args.year}'))
