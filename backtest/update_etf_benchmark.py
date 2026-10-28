"""Append observed 511380.SH closes and adjustment factors for pricing figures."""
from pathlib import Path
import argparse
import datetime as dt
import hashlib
import json
import os
import time
import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent
CODE = '511380.SH'
LIST_DATE = pd.Timestamp('2020-04-07')
CACHE = 'ETF_511380_Daily.csv'
MANIFEST = 'ETF_511380_Manifest.json'


def load_cache(base=BASE):
    path = Path(base)/CACHE
    manifest = json.loads((Path(base)/MANIFEST).read_text(encoding='utf-8'))
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest['sha256']:
        raise ValueError('ETF cache integrity mismatch')
    frame = pd.read_csv(path,parse_dates=['trade_date'],float_precision='round_trip')
    if frame.trade_date.duplicated().any() or not frame.trade_date.is_monotonic_increasing:
        raise ValueError('ETF dates must be unique and sorted')
    if not frame.ts_code.eq(CODE).all() or frame.trade_date.min() < LIST_DATE:
        raise ValueError('Unexpected ETF code or pre-listing data')
    values=frame[['close','adj_factor','adjusted_close']].to_numpy()
    if not np.isfinite(values).all() or (values<=0).any():
        raise ValueError('Invalid observed ETF prices/factors')
    np.testing.assert_allclose(frame.adjusted_close,frame.close*frame.adj_factor,rtol=1e-12)
    return frame


def combine_observations(daily, factors, start, end):
    if daily.empty or factors.empty:
        raise ValueError(f'Missing ETF observations: {start.date()}..{end.date()}')
    frame=daily.merge(factors,on=['ts_code','trade_date'],how='left',validate='one_to_one')
    frame['trade_date']=pd.to_datetime(frame.trade_date,format='%Y%m%d')
    if not frame.ts_code.eq(CODE).all() or not frame.trade_date.between(start,end).all():
        raise ValueError('ETF response outside requested code/date range')
    for key in ['close','adj_factor']:
        frame[key]=pd.to_numeric(frame[key],errors='coerce')
    if not np.isfinite(frame[['close','adj_factor']]).all().all() or (frame[['close','adj_factor']]<=0).any().any():
        raise ValueError('ETF close or exact-date adjustment factor missing')
    frame['adjusted_close']=frame.close*frame.adj_factor
    return frame[['trade_date','ts_code','close','adj_factor','adjusted_close']].sort_values('trade_date')


def update(end=None,base=BASE,pro=None):
    base=Path(base)
    cutoffs=[json.loads((base/f'{m}_Model_Manifest.json').read_text(encoding='utf-8'))['input_cutoff'] for m in ['BS','ZL','LSM']]
    end=pd.Timestamp(end or min(cutoffs))
    if end>pd.Timestamp(min(cutoffs)):
        raise ValueError('ETF plotting cutoff must not exceed all three verified models')
    path=base/CACHE
    if path.exists() != (base/MANIFEST).exists():
        raise ValueError('Incomplete existing ETF cache/manifest pair')
    old=load_cache(base) if path.exists() else pd.DataFrame()
    start=old.trade_date.max()+pd.Timedelta(days=1) if len(old) else LIST_DATE
    required=pd.to_datetime(pd.read_csv(base/'Market_Prices.csv',usecols=[0]).iloc[:,0])
    required=required[(required>=LIST_DATE)&(required<=end)]
    if start>end:
        if not set(required).issubset(set(old.trade_date)):
            raise ValueError('Existing ETF cache misses observed pricing dates')
        print(f'ETF: no new dates; observed cache through {old.trade_date.max().date()}')
        return old
    if pro is None:
        import tushare as ts
        from token_loader import load_tushare_token
        pro=ts.pro_api(load_tushare_token())
    chunks=[]
    for year in range(start.year,end.year+1):
        left=max(start,pd.Timestamp(year=year,month=1,day=1))
        right=min(end,pd.Timestamp(year=year,month=12,day=31))
        params=dict(ts_code=CODE,start_date=left.strftime('%Y%m%d'),end_date=right.strftime('%Y%m%d'))
        daily=pro.fund_daily(**params,fields='ts_code,trade_date,close')
        time.sleep(.5)
        factors=pro.fund_adj(**params,fields='ts_code,trade_date,adj_factor')
        time.sleep(.5)
        if daily.empty and factors.empty:
            continue
        chunks.append(combine_observations(daily,factors,left,right))
    merged=pd.concat([old,*chunks],ignore_index=True).sort_values('trade_date')
    if merged.trade_date.duplicated().any() or not set(required).issubset(set(merged.trade_date)):
        raise ValueError('ETF response has duplicates or misses observed pricing dates; old cache preserved')
    temp=path.with_suffix('.tmp')
    merged.to_csv(temp,index=False)
    os.replace(temp,path)
    metadata={'ts_code':CODE,'name':'Bosera Convertible Bond ETF','source':'Tushare fund_daily + fund_adj',
        'source_urls':['https://tushare.pro/document/2?doc_id=127','https://tushare.pro/document/2?doc_id=199'],
        'listing_source':'https://static.cninfo.com.cn/finalpage/2020-04-07/1207460850.PDF',
        'listed_on':'2020-04-07','adjustment':'close * exact-date adj_factor; no backfill before listing',
        'requested_end':str(end.date()),'first_date':str(merged.trade_date.min().date()),
        'last_date':str(merged.trade_date.max().date()),'rows':len(merged),
        'fetched_at':dt.datetime.now(dt.timezone.utc).isoformat(),
        'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    (base/MANIFEST).write_text(json.dumps(metadata,indent=2),encoding='utf-8')
    load_cache(base)
    print(f'ETF: {len(merged)} observed dates, {metadata["first_date"]}..{metadata["last_date"]}')
    return merged


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--end')
    update(parser.parse_args().end)
