"""Verify whether unresolved stock-volatility inputs coincide with suspensions.

Endpoint specification: https://tushare.pro/document/2?doc_id=214
No suspension record is used as a synthetic quote or volatility observation.
"""
import json
from pathlib import Path
import sys
import pandas as pd

HERE=Path(__file__).resolve().parent
BASE=HERE.parents[1]/'backtest'
sys.path.insert(0,str(BASE))
from strategy_data import TushareReader,SourceError,now_local


def main():
    root=HERE/'inputs';raw=root/'raw'
    missing=pd.read_csv(root/'missing_after.csv')
    missing=missing.loc[missing.field.eq('volatility')].copy()
    basic=pd.read_csv(BASE/'cb_basic_info.csv',dtype=str).set_index('ts_code')
    missing['stock']=missing.code.map(basic.stk_cd)
    reader=TushareReader();records=[]
    for (stock,year),group in missing.groupby(['stock',missing.date.str[:4]]):
        path=raw/f'suspension_{stock}_{year}.json'
        error=''
        if path.exists():payload=json.loads(path.read_text(encoding='utf-8'))
        else:
            params=dict(ts_code=stock,start_date=group.date.min().replace('-',''),
                end_date=group.date.max().replace('-',''),suspend_type='S')
            try:
                f=reader.query('suspend_d','ts_code,trade_date,suspend_timing,suspend_type',**params)
                if len(f)>=5000:raise SourceError('truncated suspension response')
                payload=dict(api='suspend_d',params=params,fetched_at=now_local(),records=json.loads(f.to_json(orient='records')))
                path.write_text(json.dumps(payload,indent=2),encoding='utf-8')
            except SourceError as exc:
                payload=dict(records=[]);error=str(exc)
        f=pd.DataFrame(payload['records'])
        for r in group.itertuples():
            selected=f.loc[f.trade_date.astype(str).eq(r.date.replace('-',''))] if not f.empty else f
            full=not selected.empty and selected.suspend_timing.fillna('').eq('').any()
            records.append(dict(date=r.date,code=r.code,stock=stock,full_day_suspension=full,
                source=str(path.relative_to(HERE)) if path.exists() else '',error=error,
                resolution='retain missing: no timely observed stock quote' if full else 'unresolved source coverage'))
        print('Suspension checked: '+stock+' '+year,flush=True)
    pd.DataFrame(records).to_csv(root/'volatility_gap_diagnosis.csv',index=False)
    print('Confirmed full-day suspensions:',sum(r['full_day_suspension'] for r in records),'/',len(records),flush=True)


if __name__=='__main__':main()
