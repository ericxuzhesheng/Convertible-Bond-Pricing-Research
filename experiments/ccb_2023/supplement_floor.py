"""Supplement missing pure-bond values from a separately labelled source.

Never substitute for observed Tushare values. These added CCB-only observations
cannot enter the existing four-model comparison unless all model prices exist.
"""
import json
from pathlib import Path
import sys
import time
import numpy as np
import pandas as pd
import requests

HERE=Path(__file__).resolve().parent
BASE=HERE.parents[1]/'backtest'
sys.path.insert(0,str(BASE))
from pricing_inputs import PricingInputs
from strategy_data import now_local


def main():
    root=HERE/'inputs';raw=root/'raw';d=PricingInputs(BASE)
    missing=pd.read_csv(root/'missing_after.csv')
    repairs=pd.read_csv(root/'repairs.csv')
    candidates=[];added=[];checks=[]
    session=requests.Session()
    for code,group in missing.loc[missing.field.eq('floor')].groupby('code'):
        path=raw/f'eastmoney_floor_{code}.json'
        if path.exists():
            payload=json.loads(path.read_text(encoding='utf-8'))
        else:
            params={'type':'RPTA_WEB_KZZ_LS','sty':'ALL','filter':f'(zcode="{code[:6]}")',
                    'p':1,'ps':8000,'st':'date','sr':1}
            try:
                response=session.get('https://datacenter-web.eastmoney.com/api/data/get',params=params,timeout=30)
                response.raise_for_status();result=response.json()
                body=result.get('result') or {}
                if not result.get('success') or body.get('pages',0)>1:
                    raise ValueError('missing or truncated historical response')
                payload=dict(source='Eastmoney RPTA_WEB_KZZ_LS',params=params,fetched_at=now_local(),records=body.get('data',[]))
                path.write_text(json.dumps(payload,ensure_ascii=False),encoding='utf-8')
            except (requests.RequestException,ValueError) as exc:
                checks.append(dict(code=code,status=type(exc).__name__+': '+str(exc)));continue
            finally:time.sleep(.5)
        frame=pd.DataFrame(payload['records'])
        if frame.empty:continue
        frame.index=pd.to_datetime(frame.DATE).dt.normalize()
        if frame.index.has_duplicates:raise ValueError('duplicate historical date')
        comparisons=[]
        for day,row in frame.iterrows():
            if day in d.frames['floor'].index and code in d.frames['floor']:
                old=d.frames['floor'].at[day,code]
                if pd.notna(old) and pd.notna(row.PUREBONDVALUE):comparisons.append(float(row.PUREBONDVALUE)-old)
        checks.append(dict(code=code,status='observed independent valuation source',overlap=len(comparisons),
            median_difference=np.median(comparisons) if comparisons else None,
            max_absolute_difference=max(map(abs,comparisons)) if comparisons else None))
        for target in group.itertuples():
            day=pd.Timestamp(target.date)
            if day not in frame.index:continue
            row=frame.loc[day];value=pd.to_numeric(row.PUREBONDVALUE,errors='coerce')
            parity=d.frames['parity'].at[day,code]
            parity_ok=pd.notna(row.SWAPVALUE) and np.isclose(float(row.SWAPVALUE),parity,rtol=0,atol=.02)
            close_ok=pd.isna(row.FCLOSE) or np.isclose(float(row.FCLOSE),d.market.at[day,code],rtol=0,atol=.001)
            accepted=bool(np.isfinite(value) and value>0 and parity_ok and close_ok)
            candidates.append(dict(date=target.date,code=code,value=value,parity_matches=parity_ok,
                close_observed=pd.notna(row.FCLOSE),close_matches=close_ok,accepted=accepted,
                source='Eastmoney independent pure-bond valuation; not Tushare methodology'))
            if accepted:added.append(dict(date=target.date,code=code,field='floor',value=value,
                source=str(path.relative_to(HERE)),fetched_at=payload['fetched_at'],source_asof=target.date,age_days=0))
        print('Alternative floor checked: '+code,flush=True)
    pd.DataFrame(candidates).to_csv(root/'alternative_floor_candidates.csv',index=False)
    pd.DataFrame(checks).to_csv(root/'alternative_floor_overlap.csv',index=False)
    if added:
        repairs=pd.concat([repairs,pd.DataFrame(added)],ignore_index=True)
        if repairs.duplicated(['date','code','field']).any():raise ValueError('duplicate repair')
        repairs.to_csv(root/'repairs.csv',index=False)
        keys=set(zip(repairs.date,repairs.code,repairs.field))
        missing=missing.loc[[tuple(x) not in keys for x in missing[['date','code','field']].itertuples(index=False,name=None)]]
        missing.to_csv(root/'missing_after.csv',index=False)
    print(json.dumps(dict(supplemented=len(added),remaining=len(missing))),flush=True)


if __name__=='__main__':main()
