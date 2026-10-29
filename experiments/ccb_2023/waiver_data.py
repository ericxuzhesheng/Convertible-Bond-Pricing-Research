"""Issuer-disclosed non-call commitments for the 2023 CCB extension only.

No quarter-long default and no inference from two weeks without an announcement.
CNInfo search uses the repository's existing public announcement protocol.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
from pathlib import Path
import re
import sys
import time

import pandas as pd
import requests
from pypdf import PdfReader

HERE=Path(__file__).resolve().parent
ROOT=HERE/'inputs/waivers'
URL='https://www.cninfo.com.cn/new/hisAnnouncement/query'
BASE=HERE.parents[1]/'backtest'


def fetch_index(start=2013,end='2026-10-23'):
    folder=ROOT/'index';folder.mkdir(parents=True,exist_ok=True)
    rows=[];audit=[]
    for year in range(start,int(end[:4])+1):
        for word in ['不赎回','不提前赎回']:
            page=1
            while True:
                path=folder/f'{year}_{word}_{page}.json'
                if path.exists():data=json.loads(path.read_text(encoding='utf-8'))
                else:
                    payload=dict(pageNum=page,pageSize=30,column='szse',tabName='fulltext',plate='',stock='',
                        searchkey=word,secid='',category='',trade='',seDate=f'{year}-01-01~{min(end,str(year)+"-12-31")}',
                        sortName='',sortType='',isHLtitle='false')
                    for attempt in range(3):
                        try:
                            response=requests.post(URL,data=payload,timeout=40);response.raise_for_status()
                            data=response.json()
                            if 'totalAnnouncement' not in data:raise ValueError('invalid CNInfo response')
                            break
                        except (requests.RequestException,ValueError):
                            if attempt==2:raise
                            time.sleep(2+attempt)
                    path.write_text(json.dumps(data,ensure_ascii=False),encoding='utf-8')
                    time.sleep(.5)
                part=data.get('announcements') or []
                total=int(data['totalAnnouncement'])
                if not part and (page-1)*30<total:raise ValueError('incomplete CNInfo pagination')
                rows.extend(part)
                if page*30>=total:
                    audit.append(dict(year=year,query=word,total=total,pages=page));break
                page+=1
                if page>400:raise ValueError('pagination limit')
        print(f'Waiver index year {year}: {len(rows)} raw rows',flush=True)
    frame=pd.DataFrame(rows).drop_duplicates('announcementId')
    frame['announcementTitle']=frame.announcementTitle.str.replace(r'<[^>]*>','',regex=True)
    frame=frame.loc[frame.announcementTitle.str.contains('转债|可转换|可转债',regex=True)].copy()
    # Broker opinions are secondary duplicates, not issuer decisions.
    frame['issuer_notice']=~frame.announcementTitle.str.contains('核查|保荐|律师|法律意见',regex=True)
    frame['announcement_at']=pd.to_datetime(frame.announcementTime,unit='ms',utc=True).dt.tz_convert('Asia/Shanghai').astype(str)
    frame['source']='https://static.cninfo.com.cn/'+frame.adjunctUrl
    frame.sort_values(['announcementTime','announcementId']).to_csv(ROOT/'announcement_index.csv',index=False)
    pd.DataFrame(audit).to_csv(ROOT/'search_coverage.csv',index=False)
    return frame


def fetch_text(row):
    folder=ROOT/'pdf';folder.mkdir(exist_ok=True)
    textdir=ROOT/'text';textdir.mkdir(exist_ok=True)
    ident=str(row['announcementId'])
    pdf=folder/f'{ident}.pdf';txt=textdir/f'{ident}.txt'
    try:
        if not pdf.exists():
            for attempt in range(3):
                try:
                    r=requests.get(row['source'],timeout=40);r.raise_for_status()
                    if not r.content.startswith(b'%PDF'):raise ValueError('not PDF')
                    pdf.write_bytes(r.content);break
                except (requests.RequestException,ValueError):
                    if attempt==2:raise
                    time.sleep(2+attempt)
            time.sleep(.5)
        if not txt.exists():
            reader=PdfReader(pdf)
            text='\n'.join(p.extract_text() or '' for p in reader.pages)
            txt.write_text(text,encoding='utf-8')
        else:text=txt.read_text(encoding='utf-8')
        return dict(announcement_id=ident,status='ok' if len(text.strip())>40 else 'empty_text',
                    characters=len(text),pdf=str(pdf.relative_to(HERE)),text=str(txt.relative_to(HERE)))
    except Exception as exc:
        return dict(announcement_id=ident,status=type(exc).__name__+': '+str(exc),characters=0,pdf='',text='')


def download(frame,workers=4):
    basic=pd.read_csv(BASE/'cb_basic_info.csv',dtype=str)
    observations=pd.read_csv(HERE/'results/CCB_observations.csv',usecols=['code'])
    stocks=set(basic.loc[basic.ts_code.isin(observations.code.unique()),'stk_cd'].str[:6])
    selected=frame.loc[frame.issuer_notice & frame.secCode.astype(str).str.zfill(6).isin(stocks)]
    rows=[]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks={pool.submit(fetch_text,r):r for r in selected.to_dict('records')}
        for future in as_completed(tasks):
            rows.append(future.result())
            if len(rows)%100==0 or len(rows)==len(tasks):
                print(f'Issuer PDFs {len(rows)}/{len(tasks)}',flush=True)
    pd.DataFrame(rows).to_csv(ROOT/'downloads.csv',index=False)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--download',action='store_true')
    args=parser.parse_args();frame=fetch_index()
    if args.download:download(frame)
