"""2023 explicit non-call commitments, independent of the saved basic run."""
from concurrent.futures import ThreadPoolExecutor
import json
import time

import numpy as np
import pandas as pd

import run_history as history
from curve_pde import price_curve_ccb
from waiver_terms import protection_at, ROOT

HERE=history.HERE
OUT=HERE/'results_waivers_2023'
MODELS=['CCB_basic','CCB','BS','ZL','LSM']


def main():
    started=time.perf_counter();OUT.mkdir(exist_ok=True)
    terms=pd.read_csv(ROOT/'terms.csv',dtype={'announcement_id':str})
    for col in ['available_date','start','end']:terms[col]=pd.to_datetime(terms[col])
    frame=pd.read_csv(HERE/'results/CCB_observations.csv')
    frame=frame.rename(columns={'CCB':'CCB_basic','protection':'basic_protection'})
    frame['CCB']=frame.CCB_basic
    frame['protection']=frame.basic_protection
    frame['waiver_state']='searched_no_observed_commitment'
    frame['announcement_id']=''
    for code,events in terms.dropna(subset=['code']).groupby('code'):
        for row in frame.loc[frame.code.eq(code)].itertuples():
            tp,state,ident=protection_at(events,row.date,row.basic_protection)
            frame.loc[row.Index,['protection','waiver_state','announcement_id']]=[tp,state,ident]
    unresolved=frame.waiver_state.str.startswith('unresolved')
    changed=frame.protection.gt(frame.basic_protection+1e-12)&~unresolved
    d,_=history.load_data()
    def value(row):
        c=d.contract(row.code,pd.Timestamp(row.date))
        return row.Index,price_curve_ccb(c,protection=row.protection,barrier=row.barrier)
    with ThreadPoolExecutor(max_workers=4) as pool:
        for idx,price in pool.map(value,frame.loc[changed].itertuples()):frame.at[idx,'CCB']=price
    frame['price_change']=frame.CCB-frame.CCB_basic
    if frame.loc[changed,'price_change'].min() < -.05:
        raise ArithmeticError('longer protection unexpectedly lowers CCB by over tolerance')
    # Unknown current decisions remain missing, never silently valued as a
    # one-month or one-quarter commitment in the enhanced model.
    frame.loc[unresolved,'CCB']=np.nan
    valid=np.isfinite(frame[MODELS+['market']]).all(axis=1)&frame[MODELS+['market']].gt(0).all(axis=1)
    common=frame.loc[valid].copy()
    if common.floor_source.ne('production Tushare').any():raise ValueError('mixed-source comparison')
    history.MODELS=MODELS
    total=history.metrics(common)
    annual=pd.concat([history.metrics(g).assign(year=year) for year,g in common.groupby(common.date.str[:4])],ignore_index=True)
    checks=[];selected=set()
    for _,g in frame.loc[changed].groupby(frame.loc[changed,'date'].str[:4]):
        for field in ['protection','price_change','parity','sigma']:
            order=g.sort_values(field).index.to_numpy()
            selected.update(order[np.linspace(0,len(order)-1,4).astype(int)])
    for idx in sorted(selected):
        row=frame.loc[idx];c=d.contract(row.code,pd.Timestamp(row.date))
        fine=price_curve_ccb(c,protection=row.protection,barrier=row.barrier,dx=.01,max_dt=1/192)
        checks.append(dict(date=row.date,code=row.code,price=row.CCB,fine=fine,difference=fine-row.CCB))
    check=pd.DataFrame(checks)
    if check.empty or check.difference.abs().max()>.05:raise ArithmeticError('waiver grid tolerance not met')
    frame.to_csv(OUT/'CCB_observations.csv',index=False)
    common.to_csv(OUT/'common_sample.csv',index=False)
    total.to_csv(OUT/'comparison_overall.csv',index=False)
    annual.to_csv(OUT/'comparison_by_year.csv',index=False)
    check.to_csv(OUT/'observed_grid_convergence.csv',index=False)
    frame.groupby('waiver_state').agg(observations=('code','size'),bonds=('code','nunique')).to_csv(OUT/'coverage.csv')
    manifest=dict(method='CCB-2023-explicit-waivers-observed-curve-v1',
        protection='max(actual conversion start, 1/12 year, remaining explicitly dated non-call commitment)',
        availability='date-only and after-close notices available next trading session; no backward projection',
        scope='2023 CCB only; no CCBA, no default quarter, no inferred silent non-call',
        source='CNInfo issuer notices; cached original PDFs and extracted text',
        search='2013 through 2026-10-23; CNInfo queries 不赎回 and 不提前赎回; not certified exhaustive',
        prices=int(frame.CCB.notna().sum()),common_sample=len(common),common_bonds=common.code.nunique(),
        changed_protection=int(changed.sum()),changed_bonds=frame.loc[changed,'code'].nunique(),
        unresolved_observations=int(unresolved.sum()),term_status_counts=terms.status.value_counts().to_dict(),
        numerical_tolerance_yuan=.05,grid_cases=len(check),max_grid_difference=float(check.difference.abs().max()),
        basic_manifest='results/manifest.json',elapsed_seconds=time.perf_counter()-started,
        limitations=['keyword search and PDF extraction do not establish complete historical absence',
                     'unresolved latest notices excluded from enhanced same-sample comparison',
                     'actual curves extend the original fixed-rate report, as requested',
                     'BS/ZL/LSM are existing outputs; differing exercise rules remain'])
    (OUT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(total.to_string(index=False),flush=True)
    print(json.dumps(manifest,ensure_ascii=False),flush=True)


if __name__=='__main__':main()
