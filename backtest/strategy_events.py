"""Historical redemption notices from Eastmoney, using AkShare's public endpoint.
The initial history is a research vintage, never evidence that absent events were safe.
Actual payments require separately verified issuer announcements.
"""
from pathlib import Path
import json
import time
import pandas as pd
import requests

URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
FIELDS = 'SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,NOTICE_DATE_SH,RECORD_DATE_SH,EXECUTE_PRICE_SH,EXECUTE_START_DATESH,DELIST_DATE,REDEEM_TYPE'

def normalize_notices(frame, fetched_at):
    columns = ['ts_code','ann_date','is_call','available_at','source','vintage']
    if frame.empty:
        return pd.DataFrame(columns=columns)
    # NOTICE_DATE_SH is also populated for announcements NOT to redeem.
    # Require the provider redemption type AND execution evidence. Unknown rows
    # remain review candidates; they do not establish either a call or safety.
    kind = frame.get('REDEEM_TYPE', pd.Series('', index=frame.index)).astype(str).str.removesuffix('.0')
    record = pd.to_datetime(frame.get('RECORD_DATE_SH', pd.Series('', index=frame.index)), errors='coerce')
    price = pd.to_numeric(frame.get('EXECUTE_PRICE_SH', pd.Series('', index=frame.index)), errors='coerce')
    rows = frame.loc[frame.NOTICE_DATE_SH.notna() & kind.eq('2') & record.notna() & price.gt(0)].copy()
    return pd.DataFrame({'ts_code':rows.SECUCODE, 'ann_date':rows.NOTICE_DATE_SH.str[:10],
        'is_call':'announced_redemption', 'available_at':'',
        'source':rows.SECURITY_CODE.map(lambda c:f'https://data.eastmoney.com/kzz/detail/{c}.html'),
        'vintage':'provider_history_unverified', 'fetched_at':fetched_at}).reset_index(drop=True)

def merge_notices(old, new, fetched_at):
    if old.empty:
        return new
    # Keep original observations. A later-discovered historical notice cannot alter frozen history.
    missing = new.loc[~new.ts_code.isin(old.ts_code)].copy()
    missing['available_at'] = fetched_at
    missing['vintage'] = 'observed_increment'
    return pd.concat([old,missing],ignore_index=True)

def update_events(root, cutoff):
    # Local import avoids a circular import when strategy_data calls this routine.
    from strategy_data import write_json,write_csv,now_local,SourceError
    root=Path(root)
    manifest=root/'event_sources.json'
    previous=json.loads(manifest.read_text(encoding='utf-8')) if manifest.exists() else {}
    if previous.get('cutoff')==cutoff and previous.get('status')=='complete' and previous.get('normalization_revision')==2:
        return previous
    path=root/'raw'/'eastmoney'/f'{cutoff}.json'
    fetched=now_local()
    if path.exists():
        raw=json.loads(path.read_text(encoding='utf-8'));records=raw['records'];fetched=raw['fetched_at']
    else:
        records=[]
        for page in range(1,101):
            params={'reportName':'RPT_BOND_CB_LIST','columns':FIELDS,'pageSize':500,
                    'pageNumber':page,'source':'WEB','client':'WEB'}
            try:
                r=requests.get(URL,params=params,timeout=40);r.raise_for_status();payload=r.json()
                result=payload.get('result') or {}
                if not payload.get('success') or not result.get('data'):
                    raise ValueError('Empty or failed response')
            except (requests.RequestException,ValueError) as exc:
                raise SourceError(f'Eastmoney redemption notices unavailable: {type(exc).__name__}') from None
            records.extend(result['data'])
            if page>=int(result['pages']):break
            time.sleep(.5)
        else:
            raise SourceError('Eastmoney page limit exceeded')
        write_json(path,{'source':URL,'fetched_at':fetched,'records':records})
    frame=pd.DataFrame(records)
    if frame.SECUCODE.duplicated().any():
        raise SourceError('Duplicate Eastmoney redemption records')
    calls=normalize_notices(frame,fetched)
    output=root/'redemption_notices_v2.csv'
    old=pd.read_csv(output,dtype=str).fillna('') if output.exists() else pd.DataFrame()
    calls=merge_notices(old,calls,fetched)
    write_csv(output,calls.sort_values(['ts_code','ann_date']))
    rejected = frame.loc[frame.NOTICE_DATE_SH.notna() & ~frame.SECUCODE.isin(calls.ts_code)].copy()
    rejected['review_status'] = 'announcement_date_alone_not_redemption_evidence'
    write_csv(root/'redemption_notice_review.csv', rejected)
    # Candidate amounts/dates retained for verification, not implicitly authorized as cash flows.
    write_csv(root/'redemption_candidates.csv',frame.dropna(subset=['NOTICE_DATE_SH']).sort_values('SECUCODE'))
    state={'cutoff':cutoff,'status':'complete','fetched_at':fetched,'source':URL,
           'records':len(frame),'notices':len(calls),'review_candidates':len(rejected),
           'normalization_revision':2,'normalized_at':now_local(),'cashflow_verified':False,
           'limitation':'Provider redemption type and execution evidence required; dates alone include waivers. Missing trigger/waiver history remains unknown.'}
    write_json(manifest,state)
    return state

if __name__=='__main__':
    root=Path(__file__).resolve().parent/'strategy_inputs'
    cutoff=json.loads((root/'acquisition.json').read_text(encoding='utf-8'))['cutoff']
    print(update_events(root,cutoff))
