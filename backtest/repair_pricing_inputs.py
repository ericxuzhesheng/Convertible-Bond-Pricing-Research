"""Targeted observed-source repairs, with an old/new/source audit trail.

Only missing cells and specifically flagged inconsistent floor observations are
eligible for changes. No default volatility, yield or synthetic quotes are used.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import pandas as pd
import numpy as np
from strategy_data import TushareReader, SourceError, now_local
from market_data_contracts import build_observed_volatility, extract_clause_terms

BASE=Path(__file__).resolve().parent


def repair(base=BASE, targets=None, reuse_snapshots=False):
    base=Path(base); reader=TushareReader(); fetched=now_local()
    raw=base/'logs'/'pricing_input_repairs';raw.mkdir(parents=True,exist_ok=True)
    audit=[];failures=[]
    if reuse_snapshots and (raw/'repair_status.json').exists():
        fetched=json.loads((raw/'repair_status.json').read_text(encoding='utf-8'))['fetched_at']
    def query_snapshot(name,api,fields,**kwargs):
        path=raw/name
        if reuse_snapshots and path.exists():
            return pd.read_json(path,dtype=False)
        result=reader.query(api,fields,**kwargs)
        result.to_json(path,orient='records',force_ascii=False)
        return result
    basic=pd.read_csv(base/'cb_basic_info.csv',dtype=str).set_index('ts_code')
    fields='ts_code,stk_code,value_date,maturity_date,par,pay_per_year,maturity_call_price,conv_start_date,conv_end_date,rate_clause,put_clause,call_clause'
    current=query_snapshot('cb_basic.json','cb_basic',fields)
    if current.empty or current.ts_code.duplicated().any():
        raise SourceError('cb_basic repair snapshot invalid')
    current=current.rename(columns={'stk_code':'stk_cd','par':'par_value','pay_per_year':'interest_freq'}).set_index('ts_code')
    for code in basic.index.intersection(current.index):
        for field in current.columns.intersection(basic.columns):
            old=basic.at[code,field];new=current.at[code,field]
            if (pd.isna(old) or not str(old).strip()) and pd.notna(new) and str(new).strip():
                basic.at[code,field]=str(new)
                audit.append(dict(code=code,date='',field=field,old=old,new=new,source='Tushare.cb_basic'))
    basic_repairs=len(audit)
    clauses=pd.read_csv(base/'cb_clause_terms.csv').set_index('ts_code')
    numeric=['put_trigger_ratio','put_window_days','put_eligible_years','redeem_trigger_ratio','redeem_window_days','redeem_required_days','redeem_balance_threshold_wan','maturity_redemption_price']
    for code in clauses.index.intersection(basic.index):
        # These official contractual texts are already known at issuance.
        texts={key:basic.at[code,key] for key in ['put_clause','call_clause']}
        for field,key in [('resale_clause','put_clause'),('redeem_clause','call_clause')]:
            if (pd.isna(clauses.at[code,field]) or not str(clauses.at[code,field]).strip()) and pd.notna(texts[key]):
                clauses.at[code,field]=texts[key]
                audit.append(dict(code=code,date='',field=field,old='',new=texts[key],source='Tushare.cb_basic'))
        if pd.isna(clauses.at[code,'resale_clause']) or pd.isna(clauses.at[code,'redeem_clause']):continue
        cached_call=extract_clause_terms('',clauses.at[code,'redeem_clause'],par_value=float(basic.at[code,'par_value']))
        observed_call=extract_clause_terms('',texts['call_clause'],par_value=float(basic.at[code,'par_value']))
        if cached_call.redeem_window_days is None and all(getattr(observed_call,k) is not None for k in ['redeem_trigger_ratio','redeem_window_days','redeem_required_days']):
            old=clauses.at[code,'redeem_clause'];clauses.at[code,'redeem_clause']=texts['call_clause']
            audit.append(dict(code=code,date='',field='redeem_clause',old=old,new=texts['call_clause'],source='Tushare.cb_basic clean text replaces unparseable cached text'))
        result=extract_clause_terms(clauses.at[code,'resale_clause'],clauses.at[code,'redeem_clause'],par_value=float(basic.at[code,'par_value']))
        for field in numeric:
            value=getattr(result,field)
            if pd.isna(clauses.at[code,field]) and value is not None:
                clauses.at[code,field]=value
                audit.append(dict(code=code,date='',field=field,old='',new=value,source='parsed observed contract text'))
        if str(clauses.at[code,'source_ok']).lower()!='true' and code in current.index and all(pd.notna(current.at[code,k]) and str(current.at[code,k]).strip() for k in ['put_clause','call_clause']):
            clauses.at[code,'source_ok']=True
            audit.append(dict(code=code,date='',field='source_ok',old=False,new=True,source='Tushare.cb_basic full clause texts recovered'))
    if basic_repairs:basic.reset_index().to_csv(base/'cb_basic_info.csv',index=False)
    if len(audit)>basic_repairs:clauses.reset_index().to_csv(base/'cb_clause_terms.csv',index=False)
    price=pd.read_csv(base/'cb_price_cache.csv',index_col=0,parse_dates=True,float_precision='round_trip')
    floor=pd.read_csv(base/'cb_bond_floor_cache.csv',index_col=0,parse_dates=True,float_precision='round_trip')
    vol=pd.read_csv(base/'bs_volatility_cache.csv',index_col=0,parse_dates=True,float_precision='round_trip')
    targets=targets or base/'logs'/'pricing_missing_targets.json'
    missing=json.loads(Path(targets).read_text(encoding='utf-8'))
    floor_targets={(date,code) for key,date,code in missing if key=='floor'}
    checkpoint=base/'logs/pricing_v2_full/BS_rebuild_checkpoint.json'
    if checkpoint.exists():
        state=json.loads(checkpoint.read_text(encoding='utf-8'))
        floor_targets|={(x['date'],x['code']) for x in state['issues'] if x['reason'].startswith('observed bond value exceeds')}
    grouped=defaultdict(list)
    for date,code in floor_targets:grouped[(code,date[:4])].append(date)
    for count,((code,year),dates) in enumerate(grouped.items(),1):
        try:
            frame=query_snapshot(f'floor_{code}_{year}.json','cb_daily','ts_code,trade_date,close,bond_value',ts_code=code,start_date=min(dates).replace('-',''),end_date=max(dates).replace('-',''))
            if len(frame)>=2000:raise SourceError('cb_daily repair query truncated')
            for field in ('close','bond_value'):
                frame[field]=pd.to_numeric(frame[field],errors='coerce')
            for row in frame.itertuples():
                date=str(pd.Timestamp(str(row.trade_date)).date())
                if (date,code) not in floor_targets:continue
                day=pd.Timestamp(date)
                if not np.isfinite(row.bond_value) or row.bond_value<=0:continue
                if not np.isclose(price.at[day,code],row.close,rtol=0,atol=.001):
                    failures.append(f'{code} {date}: revised close disagrees with cached quote');continue
                old=floor.at[day,code]
                if pd.isna(old) or not np.isclose(old,row.bond_value,rtol=0,atol=1e-8):
                    floor.at[day,code]=row.bond_value
                    audit.append(dict(code=code,date=date,field='bond_floor',old=old,new=row.bond_value,source='Tushare.cb_daily; close cross-check passed'))
        except (SourceError,ValueError,KeyError,TypeError) as exc:failures.append(f'{code} {year}: {exc}')
        if count%20==0 or count==len(grouped):print(f'Floor repair groups {count}/{len(grouped)}',flush=True)
    floor.to_csv(base/'cb_bond_floor_cache.csv')
    grouped=defaultdict(set)
    for key,date,code in missing:
        if key=='volatility' and code in basic.index and pd.notna(basic.at[code,'stk_cd']):
            grouped[(basic.at[code,'stk_cd'],date[:4])].add((date,code))
    for count,((stock,year),cells) in enumerate(grouped.items(),1):
        first=pd.Timestamp(min(date for date,_ in cells))-pd.Timedelta(days=600)
        last=max(date for date,_ in cells)
        try:
            daily=query_snapshot(f'daily_{stock}_{year}.json','daily','ts_code,trade_date,close',ts_code=stock,start_date=first.strftime('%Y%m%d'),end_date=last.replace('-',''))
            adj=query_snapshot(f'adj_{stock}_{year}.json','adj_factor','ts_code,trade_date,adj_factor',ts_code=stock,start_date=first.strftime('%Y%m%d'),end_date=last.replace('-',''))
            if len(daily)>=2000 or len(adj)>=2000:raise SourceError('underlying repair query truncated')
            data=daily.merge(adj,on=['ts_code','trade_date'],validate='one_to_one')
            if data.empty:raise SourceError('no observed adjusted stock history')
            close=pd.Series(data.close.to_numpy()*data.adj_factor.to_numpy(),index=pd.to_datetime(data.trade_date.astype(str),format='%Y%m%d')).sort_index()
            dates=pd.to_datetime(sorted({d for d,_ in cells}))
            series=build_observed_volatility(adjusted_close=close,target_dates=dates,window=250,min_observations=60,max_staleness_days=7)
            for date,code in cells:
                day=pd.Timestamp(date);new=series.at[day]
                if pd.notna(new) and pd.isna(vol.at[day,code]):
                    vol.at[day,code]=new
                    asof=close.index[close.index<=day][-1]
                    audit.append(dict(code=code,date=date,field='volatility',old='',new=new,source='Tushare.daily+adj_factor; 250 observed closes, min 60; log-return SD * sqrt(250)',source_asof=str(asof.date()),age_days=(day-asof).days))
        except (SourceError,ValueError,KeyError,TypeError) as exc:failures.append(f'{stock} {year}: {exc}')
        if count%20==0 or count==len(grouped):print(f'Volatility repair groups {count}/{len(grouped)}',flush=True)
    vol.to_csv(base/'bs_volatility_cache.csv')
    for row in audit:row['fetched_at']=fetched
    audit_path=base/'Pricing_Data_Repairs.csv'
    records=pd.DataFrame(audit,columns=['code','date','field','old','new','source','fetched_at','source_asof','age_days'])
    if audit_path.exists():records=pd.concat([pd.read_csv(audit_path),records],ignore_index=True)
    old_vol_source=records.source.eq('Tushare.daily+adj_factor; 250-return rolling SD, min 60')
    records.loc[old_vol_source,'source']='Tushare.daily+adj_factor; 250 observed closes, min 60; log-return SD * sqrt(250)'
    records.loc[old_vol_source,'source_asof']=records.loc[old_vol_source,'date']
    records.loc[old_vol_source,'age_days']=0
    records.to_csv(audit_path,index=False)
    (raw/'repair_status.json').write_text(json.dumps({'fetched_at':fetched,'repairs':len(audit),'failures':failures},ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Observed repairs: {len(audit)}; unresolved source queries: {len(failures)}',flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--targets',type=Path)
    p.add_argument('--reuse-snapshots',action='store_true')
    a=p.parse_args();repair(targets=a.targets,reuse_snapshots=a.reuse_snapshots)
