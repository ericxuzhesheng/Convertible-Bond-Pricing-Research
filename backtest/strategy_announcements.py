"""Cache issuer coupon announcement indexes from CNInfo (AkShare query protocol).
An index is evidence of a notice, NOT verification of its payment amount/dates.
"""
import argparse
import json
import time
from pathlib import Path
import pandas as pd
import requests
from strategy_data import write_json, write_csv, SourceError, now_local

URL='https://www.cninfo.com.cn/new/hisAnnouncement/query'

def fetch_year(root, year, end):
    key=root/'raw'/'cninfo_coupon'/f'{year}-{end}.json'
    if key.exists():
        return json.loads(key.read_text(encoding='utf-8'))['records']
    rows=[]
    for page in range(1,401):
        payload={'pageNum':page,'pageSize':30,'column':'szse','tabName':'fulltext',
            'plate':'','stock':'','searchkey':'付息','secid':'','category':'category_kzzq_szsh',
            'trade':'','seDate':f'{year}-01-01~{end}','sortName':'','sortType':'','isHLtitle':'false'}
        r=requests.post(URL,data=payload,timeout=40);r.raise_for_status();data=r.json()
        if 'totalAnnouncement' not in data:
            raise SourceError('CNInfo invalid announcement response')
        part=data.get('announcements') or []
        if not part and len(rows)<int(data['totalAnnouncement']):
            raise SourceError('CNInfo incomplete pagination')
        rows.extend(part)
        if page*30>=int(data['totalAnnouncement']):break
        time.sleep(.5)
    else:raise SourceError('CNInfo announcement page limit')
    write_json(key,{'source':URL,'fetched_at':now_local(),'records':rows})
    return rows

def update_index(root,start,end):
    rows=[]
    for year in range(pd.Timestamp(start).year,pd.Timestamp(end).year+1):
        part=fetch_year(root,year,min(end,f'{year}-12-31'))
        rows.extend(part)
        print(f'CNInfo coupon announcements {year}: {len(part)}',flush=True)
    frame=pd.DataFrame(rows).drop_duplicates('announcementId')
    frame['source']=frame.adjunctUrl.map(lambda p:'https://static.cninfo.com.cn/'+p)
    frame['announcement_at']=pd.to_datetime(frame.announcementTime,unit='ms',utc=True).dt.tz_convert('Asia/Shanghai')
    frame['cashflow_verified']=False
    write_csv(root/'coupon_announcement_index.csv',frame[['secCode','secName','announcementId',
        'announcementTitle','announcement_at','source','cashflow_verified']].sort_values('announcement_at'))
    return len(frame)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',default='2019-01-01')
    parser.add_argument('--end',default='2026-09-11')
    parser.add_argument('--input-dir',type=Path,default=Path(__file__).resolve().parent/'strategy_inputs')
    args=parser.parse_args()
    print('Announcements cached:',update_index(args.input_dir,args.start,args.end),flush=True)
