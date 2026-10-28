"""Refresh only new observed weekly volatility inputs; preserve old cache rows."""
from __future__ import annotations
import argparse
import io
import json
from pathlib import Path
import sys
import time
import pandas as pd
from market_data_contracts import build_observed_volatility, select_completed_weekly_dates
from token_loader import load_tushare_token

if hasattr(sys.stdout,"buffer") and sys.stdout.encoding.lower() not in ("utf-8","utf8"):
    sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding="utf-8",errors="replace")

BASE=Path(__file__).resolve().parent


def latest_verified_cutoff(base=BASE):
    paths=[Path(base)/f'{model}_Model_Manifest.json' for model in ('BS','ZL','LSM')]
    dates=[pd.Timestamp(json.loads(path.read_text(encoding='utf-8'))['input_cutoff']) for path in paths if path.exists()]
    return max(dates) if dates else None


def update(*,base=BASE,after=None,pro=None,bar=None):
    base=Path(base)
    protected=latest_verified_cutoff(base)
    if after is None and protected is None:
        raise RuntimeError('A model manifest or explicit --after cutoff is required')
    after=max(pd.Timestamp(after),protected) if after is not None and protected is not None else (protected if after is None else pd.Timestamp(after))
    price=pd.read_csv(base/'cb_price_cache.csv',index_col=0,parse_dates=True,float_precision='round_trip')
    cache=base/'bs_volatility_cache.csv'
    old=pd.read_csv(cache,index_col=0,parse_dates=True,float_precision='round_trip')
    dates=select_completed_weekly_dates(price.index)
    dates=dates[dates>pd.Timestamp(after)]
    if not len(dates):
        print('Volatility: no new completed weekly dates.',flush=True)
        return
    vol=old.reindex(index=dates,columns=price.columns)
    pending=price.reindex(index=dates).notna() & vol.isna()
    codes=pending.columns[pending.any()]
    if not len(codes):
        print('Volatility inputs already current.',flush=True)
        return
    basic=pd.read_csv(base/'cb_basic_info.csv',dtype=str).set_index('ts_code')
    stocks=basic.reindex(codes).stk_cd.dropna()
    if len(stocks)!=len(codes):
        raise RuntimeError('New pricing dates have unmapped underlying stocks')
    if pro is None or bar is None:
        import tushare as ts
        pro=ts.pro_api(load_tushare_token(),timeout=60)
        bar=ts.pro_bar
    for count,(stock,bonds) in enumerate(stocks.groupby(stocks),1):
        frame=bar(ts_code=stock,adj='qfq',api=pro,
            start_date=(dates.min()-pd.Timedelta(days=500)).strftime('%Y%m%d'),
            end_date=dates.max().strftime('%Y%m%d'))
        if frame is None or frame.empty:
            raise RuntimeError(f'Adjusted underlying history unavailable: {stock}')
        frame=frame.copy()
        frame.index=pd.to_datetime(frame.trade_date.astype(str),format='%Y%m%d')
        series=build_observed_volatility(adjusted_close=frame.close.sort_index(),target_dates=dates,
            window=250,min_observations=60,max_staleness_days=7)
        for code in bonds.index:
            vol.loc[pending[code],code]=series.loc[pending[code]]
        time.sleep(.5)
        if count%20==0 or count==stocks.nunique():
            print(f'Volatility underlying histories: {count}/{stocks.nunique()}',flush=True)
    combined=old.reindex(index=old.index.union(dates),columns=old.columns.union(price.columns,sort=False))
    combined.loc[dates,vol.columns]=vol
    temp=cache.with_suffix('.csv.tmp')
    combined.sort_index().to_csv(temp)
    temp.replace(cache)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--after')
    p.add_argument('--print-bounds',action='store_true',help='Print next data start and latest protected model cutoff; no data access')
    a=p.parse_args()
    if a.print_bounds:
        cutoff=latest_verified_cutoff()
        if cutoff is None:p.error('A model manifest is required for incremental bounds')
        print((cutoff+pd.Timedelta(days=1)).strftime('%Y%m%d'),cutoff.strftime('%Y-%m-%d'))
        return
    update(after=a.after)


if __name__=='__main__': main()
