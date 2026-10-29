"""Independent CCB history and exact bond-date comparison with saved models."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time

import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]
BASE=ROOT/'backtest'
sys.path.insert(0,str(BASE))
from pricing_inputs import PricingInputs
from pricing_backtest import validate_outputs
from market_data_contracts import DataContractError
from curve_pde import price_curve_ccb

MODELS=['CCB','BS','ZL','LSM']


def load_data():
    d=PricingInputs(BASE)
    repairs=pd.read_csv(HERE/'inputs/repairs.csv')
    for row in repairs.itertuples():
        day=pd.Timestamp(row.date)
        old=d.frames[row.field].at[day,row.code]
        if pd.notna(old):raise ValueError('repair must not overwrite an observed input')
        d.frames[row.field].at[day,row.code]=row.value
    return d,repairs


def barrier_for(data,code):
    row=data.clauses.loc[code] if code in data.clauses.index else None
    if row is None:raise DataContractError('call barrier unavailable')
    text=re.sub(r'\s+','',str(row.redeem_clause))
    if '余额不足' in text and not re.search(r'收盘|股价|转股价格的',text):return math.inf
    ratio=pd.to_numeric(row.redeem_trigger_ratio,errors='coerce')
    if not np.isfinite(ratio) or ratio<=0:raise DataContractError('call barrier unavailable')
    return float(ratio)*float(data.basic.at[code,'par_value'])


def metrics(frame):
    rows=[]
    for model in MODELS:
        market=frame.market.to_numpy(float);price=frame[model].to_numpy(float)
        error=price-market
        rows.append(dict(model=model,n=len(frame),bonds=frame.code.nunique(),weeks=frame.date.nunique(),
            MAE=np.mean(np.abs(error)),RMSE=np.sqrt(np.mean(error**2)),MSE=np.mean(error**2),
            MAPE_pct=np.mean(np.abs(error)/market)*100,bias=np.mean(error),
            R2=1-np.sum(error**2)/np.sum((market-market.mean())**2),
            median_absolute_error=np.median(np.abs(error))))
    return pd.DataFrame(rows)


def compare(frame,out):
    valid=np.isfinite(frame[MODELS+['market']]).all(axis=1)&frame[MODELS+['market']].gt(0).all(axis=1)
    common=frame.loc[valid].copy()
    if common.empty:raise ValueError('empty common sample')
    # The supplementary provider must not change the input source of a paired
    # comparison against immutable BS/ZL/LSM outputs.
    if common.floor_source.ne('production Tushare').any():
        raise ValueError('source supplement unexpectedly overlaps immutable baseline')
    common.to_csv(out/'common_sample.csv',index=False)
    total=metrics(common);total.to_csv(out/'comparison_overall.csv',index=False)
    annual=[]
    for year,g in common.groupby(common.date.str[:4]):
        annual.append(metrics(g).assign(year=year))
    pd.concat(annual,ignore_index=True).to_csv(out/'comparison_by_year.csv',index=False)
    bands=pd.cut(common.parity,bins=[-np.inf,90,130,np.inf],labels=['parity_lt90','parity_90_130','parity_ge130'],right=False)
    pd.concat([metrics(g).assign(band=str(band)) for band,g in common.groupby(bands,observed=True)],ignore_index=True).to_csv(out/'comparison_by_parity.csv',index=False)
    weekly=common.groupby('date')[MODELS+['market']].mean()
    weekly['n']=common.groupby('date').size();weekly.to_csv(out/'common_weekly_prices.csv')
    return common,total


def main(workers=4):
    start=time.perf_counter();out=HERE/'results';out.mkdir(exist_ok=True)
    numerical=json.loads((out/'numerical_validation.json').read_text())
    if not numerical['passed']:raise ValueError('numerical verification required')
    # Invoke existing artifact guards, not replacement or relaxed checks.
    baseline={m:validate_outputs(m,BASE) for m in MODELS[1:]}
    d,repairs=load_data()
    cutoff=min(pd.Timestamp(x['input_cutoff']) for x in baseline.values())
    dates=d.dates(end=cutoff)
    prices={m:pd.read_csv(BASE/f'{m}_Model_Prices.csv',index_col=0,parse_dates=True,float_precision='round_trip') for m in MODELS[1:]}
    market_tables={m:pd.read_csv(BASE/('Market_Prices.csv' if m=='BS' else f'{m}_Market_Prices.csv'),index_col=0,parse_dates=True,float_precision='round_trip') for m in MODELS[1:]}
    repair_map={(pd.Timestamp(r.date),r.code,r.field):r.source for r in repairs.itertuples()}
    records=[];issues=[];coverage=[]

    def value_one(item):
        row,c,barrier,tp=item
        row['CCB']=price_curve_ccb(c,protection=tp,barrier=barrier)
        if not np.isfinite(row['CCB']) or row['CCB']<=0:
            raise ArithmeticError('nonpositive/nonfinite CCB output')
        return row

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i,day in enumerate(dates):
            jobs=[];quoted=0;eligible=0
            for code in d.market.columns[d.market.loc[day].gt(0)]:
                quoted+=1
                row=d.basic.loc[code]
                if pd.isna(row.list_date) or day<row.list_date or (pd.notna(row.delist_date) and day>=row.delist_date):
                    issues.append(dict(date=str(day.date()),code=code,reason='outside listed lifecycle'));continue
                eligible+=1
                try:
                    c=d.contract(code,day)
                    if str(row.cb_type)!='CB':raise DataContractError('nonstandard convertible type outside model')
                    barrier=barrier_for(d,code)
                    tp=max(c.conversion_start,1/12)
                    result=dict(date=str(day.date()),code=code,market=float(d.market.at[day,code]),
                        parity=c.parity,sigma=c.sigma,bond_floor=float(d.frames['floor'].at[day,code]),
                        spread=c.spread,protection=tp,barrier=barrier,maturity=c.maturity,
                        conversion_end=c.conversion_end,
                        floor_source=repair_map.get((day,code,'floor'),'production Tushare'),
                        volatility_source=repair_map.get((day,code,'volatility'),'production observed volatility'))
                    for m,f in prices.items():
                        value=f.at[day,code] if day in f.index and code in f else np.nan
                        result[m]=value
                        if np.isfinite(value):
                            quote=market_tables[m].at[day,code]
                            if not np.isclose(quote,result['market'],rtol=0,atol=1e-8):
                                raise ValueError(f'{m} market quote mismatch: {day} {code}')
                    jobs.append((result,c,barrier,tp))
                except DataContractError as exc:
                    issues.append(dict(date=str(day.date()),code=code,reason=str(exc)))
            valued=list(pool.map(value_one,jobs))
            records.extend(valued)
            coverage.append(dict(date=str(day.date()),quoted=quoted,listed_eligible=eligible,priced=len(valued),
                                 coverage=len(valued)/eligible if eligible else None))
            if (i+1)%25==0 or i+1==len(dates):
                print(f'CCB {i+1}/{len(dates)} weeks; {len(records)} prices; {time.perf_counter()-start:.1f}s',flush=True)
    frame=pd.DataFrame(records)
    frame.to_csv(out/'CCB_observations.csv',index=False)
    pd.DataFrame(issues).to_csv(out/'pricing_issues.csv',index=False)
    pd.DataFrame(coverage).to_csv(out/'coverage.csv',index=False)
    common,total=compare(frame,out)
    # Independent grid refinement on extrema and interior observations in each
    # year; retain the exact cases and differences, not just a pass flag.
    selected=[]
    for year,g in frame.groupby(frame.date.str[:4]):
        indices=[]
        for field in ['parity','sigma','maturity','protection']:
            order=g.sort_values(field).index.to_numpy()
            indices.extend(order[np.linspace(0,len(order)-1,5).astype(int)])
        selected.extend(set(indices))
    checks=[]
    for idx in sorted(set(selected)):
        row=frame.loc[idx];day=pd.Timestamp(row.date);c=d.contract(row.code,day)
        fine=price_curve_ccb(c,protection=row.protection,barrier=row.barrier,dx=.01,max_dt=1/192)
        checks.append(dict(date=row.date,code=row.code,production=row.CCB,fine=fine,difference=fine-row.CCB))
    check=pd.DataFrame(checks);check.to_csv(out/'observed_grid_convergence.csv',index=False)
    if check.difference.abs().max()>.05:
        raise ArithmeticError('observed grid refinement exceeds 0.05 yuan; refine before publication')
    # Reproducible selected weekly input snapshots; no full daily cache copies.
    snap=HERE/'inputs/snapshot';snap.mkdir(exist_ok=True)
    for key,f in d.frames.items():f.reindex(index=dates).to_csv(snap/f'{key}.csv')
    d.basic.to_csv(snap/'basic.csv');d.clauses.to_csv(snap/'clauses.csv');d.curve.loc[:cutoff].to_csv(snap/'yield_curve.csv')
    for model,m in baseline.items():
        (snap/f'{model}_source_manifest.json').write_text(json.dumps(m,indent=2),encoding='utf-8')
    manifest=dict(method='CCB-2023-observed-curve-pde-v1',cutoff=str(cutoff.date()),weeks=len(dates),
        prices=len(frame),common_sample=len(common),common_bonds=common.code.nunique(),
        implementation='Crank-Nicolson; Rannacher event smoothing; log-price mesh',
        dx=.02,max_time_step=1/96,workers=workers,elapsed_seconds=time.perf_counter()-start,
        precision='moderate',observed_grid_tolerance_yuan=.05,
        protection='max(actual conversion start remaining ACT/365, 1/12 year)',
        rate='observed government tenor curve, log1p effective yields, linear continuous-zero interpolation',
        credit='flat nonnegative spread calibrated only to observed pure-bond value',
        barrier='observed contractual call ratio times par; continuous monitoring after protection',
        expiry='actual last conversion date; later contractual cashflows discounted to that date',
        dividends='zero conversion-value-process dividend yield, same as BS/ZL/LSM baseline',
        limitations=['no historical waiver overlay: basic protection scenario, not issuer behaviour forecast',
                    'puts and future conversion-price resets omitted as in original CCB',
                    'credit term structure unavailable; scalar spread is identified by one bond-floor observation',
                    'rates and actual contractual barrier extend the 2023 constant-rate analytical specification',
                    'existing BS/ZL/LSM prices are unchanged; same sample does not mean identical exercise rules'],
        numerical_validation=numerical,observed_grid_cases=len(check),max_observed_grid_difference=float(check.difference.abs().max()),
        repair_count=len(repairs),baseline_cutoffs={m:v['input_cutoff'] for m,v in baseline.items()},
        baseline_integrity='existing validate_outputs verified workbook and all CSV artifact hashes',
        source_commit=subprocess.check_output(['git','-c',f'safe.directory={ROOT.as_posix()}','rev-parse','HEAD'],cwd=ROOT,text=True).strip())
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(total.to_string(index=False),flush=True)
    print(json.dumps(manifest,ensure_ascii=False),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--workers',type=int,default=4);main(p.parse_args().workers)
