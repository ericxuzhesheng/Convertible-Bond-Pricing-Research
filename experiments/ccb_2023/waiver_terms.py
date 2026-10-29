"""Extract explicit 2023-model protection periods and apply as-of availability."""
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE/'inputs/waivers'
BASE=HERE.parents[1]/'backtest'
DATE=r'(\d{4})年(\d{1,2})月(\d{1,2})日'
NONCALL=r'不(?:提前)?赎回|不.{0,20}行使.{0,30}赎回|不.{0,15}提出.{0,15}赎回'


def normalize(text):
    text=text.translate(str.maketrans('０１２３４５６７８９','0123456789'))
    return re.sub(r'\s+','',text)


def date_value(year,month,day):
    try:return pd.Timestamp(year=int(year),month=int(month),day=int(day))
    except (ValueError,TypeError):return None


def parse_term(text,announcement_date,title='',maturity=None):
    """Only dated commitments, or durations with an explicit dated anchor.

Multiple conflicting future periods fail closed. A bare 'future three months'
is retained for review rather than arbitrarily anchored at the publication date.
"""
    text=normalize(text);ann=pd.Timestamp(announcement_date).tz_localize(None).normalize()
    if re.search('交易情况|减持计划|买卖情况|独立意见|核查|保荐|律师|法律意见|受托管理',title):
        return dict(status='informational_notice',start='',end='',evidence='')
    if '更正后' in title:
        title=title.replace('更正后','')
    elif '更正' in title and '更正后' in text:
        corrected=parse_term(text.split('更正后',1)[1],ann,'',maturity)
        if corrected['status']=='explicit':
            corrected['rule']='corrected_text_'+corrected['rule']
            return corrected
    if re.search('更正|取消|撤销',title):
        return dict(status='correction_review_required',start='',end='',evidence='')
    candidates=[]
    def add(start,end,left,right,rule):
        if start is None or end is None or end<=ann or start>end or (end-start).days>730:
            return
        context=text[max(0,left-80):min(len(text),right+180)]
        # A qualifying decision must be near the dated period, not merely in
        # the title of a long report. Reject share-trading disclosures.
        if not re.search(NONCALL,context):return
        local=text[max(0,left-25):min(len(text),right+40)]
        if re.search('减持|买卖|买入|卖出',local):return
        if start>ann+pd.Timedelta(days=35):return
        candidates.append(dict(start=str(start.date()),end=str(end.date()),evidence=context,rule=rule))
    pair=re.compile(DATE+r'(?:起)?(?:至|到|—|－|-|~|～)(?:(\d{4})年)?(\d{1,2})月(\d{1,2})日')
    for m in pair.finditer(text):
        y,mo,day,y2,mo2,day2=m.groups()
        start=date_value(y,mo,day);end=date_value(y2 or y,mo2,day2)
        # Never silently infer a year rollover from an omitted year.
        add(start,end,m.start(),m.end(),'explicit_date_range')
    # Explicit "from [date] for N months"; inclusive end of the period.
    duration=re.compile(r'自'+DATE+r'(?:起)?(?:的)?(?:未来)?([一二三四五六七八九十两\d]+)个?月(?:内|期间)')
    digits={'一':1,'二':2,'两':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}
    for m in duration.finditer(text):
        y,mo,day,num=m.groups();months=int(num) if num.isdigit() else digits.get(num)
        start=date_value(y,mo,day)
        if start is not None and months is not None:
            add(start,start+pd.DateOffset(months=months)-pd.Timedelta(days=1),m.start(),m.end(),'explicit_date_and_duration')
    # A precise year-end commitment is also a dated end, not a 2024 default.
    for m in re.finditer(r'(?:本年度内|本年内|今年内|(?:(\d{4})年(?:年)?底(?:前|内)?))',text):
        year=int(m.group(1)) if m.group(1) else ann.year
        add(ann,pd.Timestamp(year=year,month=12,day=31),m.start(),m.end(),'explicit_year_end')
    # Use a future explicit ending date only when the surrounding phrase
    # states a non-call interval, not a trigger-count or conversion interval.
    endpoint=re.compile(r'(?:截至|直至|至)'+DATE+r'(?:止|期间|前|内|（含当日）|\(含当日\))?')
    for m in endpoint.finditer(text):
        if any(c['rule']=='explicit_date_range' and c['end']==str(date_value(*m.groups()).date())
               for c in candidates if date_value(*m.groups()) is not None):continue
        prefix=text[max(0,m.start()-45):m.start()]
        suffix=text[m.end():m.end()+75]
        if not re.search(NONCALL,prefix+suffix):continue
        if not re.search('不赎回|不提前赎回|不行使',suffix) and not re.search('不赎回期|不提前赎回.*期限',prefix):continue
        end=date_value(*m.groups())
        if end is not None and m.group(0).endswith('前'):end-=pd.Timedelta(days=1)
        add(ann,end,m.start(),m.end(),'explicit_end_date')
    # Some issuers spell the endpoint as "until maturity (date)" or give the
    # maturity date immediately before the promise. The PDF date governs;
    # a vendor may store the following redemption/payment day as maturity.
    for m in re.finditer(r'(?:到期日[（(]|将于)'+DATE+r'(?:[）)]|到期)',text):
        local=text[max(0,m.start()-100):m.end()+140]
        if re.search(r'至[^。；]{0,20}到期日',local) and re.search(NONCALL,local):
            add(ann,date_value(*m.groups()),m.start(),m.end(),'explicit_notice_maturity_date')
    if maturity is not None and pd.notna(maturity):
        end=pd.Timestamp(maturity)
        # Contractual maturity is usable only when the notice itself explicitly
        # promises protection until maturity and corroborates that exact date.
        dated=f'{end.year}年{end.month}月{end.day}日'
        for m in re.finditer(r'至[^。；]{0,20}到期日',text):
            context=text[max(0,m.start()-100):m.end()+100]
            if dated in text and re.search(NONCALL,context):
                add(ann,end,m.start(),m.end(),'explicit_until_verified_maturity')
    ends={c['end'] for c in candidates}
    if len(ends)>1:
        # A notice may expressly extend a still-active promise with a directly
        # adjoining period. Both ranges must be dated; never bridge a gap.
        ranges=sorted({(c['start'],c['end']) for c in candidates})
        joined_start,joined_end=ranges[0]
        contiguous=True
        for start,end in ranges[1:]:
            if pd.Timestamp(start)>pd.Timestamp(joined_end)+pd.Timedelta(days=1):
                contiguous=False;break
            joined_end=max(joined_end,end)
        if contiguous and pd.Timestamp(joined_start)<=ann:
            return dict(status='explicit',start=joined_start,end=joined_end,
                        evidence=json.dumps(candidates,ensure_ascii=False),rule='explicit_contiguous_extension')
        return dict(status='conflicting_periods_review',start='',end='',evidence=json.dumps(candidates,ensure_ascii=False))
    if candidates:
        # Prefer the complete range over an ending-date-only extraction.
        best=next((c for c in candidates if c['rule']=='explicit_date_range'),candidates[0])
        return dict(status='explicit',**best)
    return dict(status='no_explicit_future_period',start='',end='',evidence=text[-1600:])


def available_date(timestamp,calendar):
    t=pd.Timestamp(timestamp)
    if t.tzinfo is not None:t=t.tz_convert('Asia/Shanghai').tz_localize(None)
    day=t.normalize()
    known_intraday=t!=day
    same_close=known_intraday and t<=day+pd.Timedelta(hours=15)
    if day<calendar.min():
        # Pre-study notices retain their historical ordering. Their exact next
        # trading session is immaterial to valuations beginning in 2019.
        return day+pd.Timedelta(days=1),'prestudy_next_calendar_day'
    side='left' if same_close else 'right'
    idx=calendar.searchsorted(day,side=side)
    result=calendar[idx] if idx<len(calendar) else day+pd.Timedelta(days=1)
    return result,('timestamp_before_close' if same_close else 'next_session_date_only_or_after_close')


def build_terms():
    index=pd.read_csv(ROOT/'announcement_index.csv',dtype={'secCode':str,'announcementId':str})
    downloaded=pd.read_csv(ROOT/'downloads.csv',dtype={'announcement_id':str}).set_index('announcement_id')
    basic=pd.read_csv(BASE/'cb_basic_info.csv',dtype=str).set_index('ts_code')
    code_map={c[:6]:c for c in basic.index}
    calendar=pd.read_csv(BASE/'strategy_inputs/calendar.csv',dtype={'cal_date':str})
    calendar=pd.DatetimeIndex(pd.to_datetime(calendar.loc[calendar.is_open.eq(1),'cal_date'],format='%Y%m%d')).sort_values()
    records=[]
    for row in index.to_dict('records'):
        ident=row['announcementId']
        if ident not in downloaded.index:continue
        download=downloaded.loc[ident]
        available,rule=available_date(row['announcement_at'],calendar)
        result=dict(announcement_id=ident,title=row['announcementTitle'],announcement_at=row['announcement_at'],
            available_date=str(available.date()),availability_rule=rule,source=row['source'],code='')
        if download.status!='ok':
            result.update(status='source_unreadable',start='',end='',evidence=str(download.status))
            text=''
        else:
            text=(HERE/download.text).read_text(encoding='utf-8')
            result.update(parse_term(text,pd.Timestamp(row['announcement_at']).date(),row['announcementTitle']))
        cleaned=normalize(text)
        codes={code_map[x] for x in re.findall(r'(?:转债|债券)代码[:：﹕“"（(]*([0-9]{6})',cleaned[:1500]) if x in code_map}
        if len(codes)==1:result['code']=next(iter(codes));result['mapping']='PDF bond-code header'
        else:
            matches=basic.loc[basic.stk_cd.str[:6].eq(str(row['secCode']).zfill(6))]
            by_title=[c for c,r in matches.iterrows() if str(r.bond_short_name) in normalize(row['announcementTitle'])]
            by_name=by_title if len(by_title)==1 else [c for c,r in matches.iterrows() if str(r.bond_short_name) in cleaned[:1500]]
            if len(by_name)==1:result['code']=by_name[0];result['mapping']='issuer stock plus exact bond name'
            else:result['status']='bond_mapping_review';result['evidence']=str(sorted(codes))+'; '+str(by_name)
        if result['code'] and result['status']=='no_explicit_future_period':
            result.update(parse_term(text,pd.Timestamp(row['announcement_at']).date(),row['announcementTitle'],basic.at[result['code'],'maturity_date']))
        # Source-reviewed supplements change trigger wording / share-trading
        # disclosure only and expressly leave the original decision unchanged.
        if ident in {'1210659121','1215951440','1224740083','1225357656'}:
            result.update(status='informational_notice',start='',end='',rule='reviewed_nonperiod_supplement',
                          evidence='Source reviewed: wording/trading supplement or trigger forecast only; no new explicit non-call period.')
        records.append(result)
    terms=pd.DataFrame(records)
    terms=apply_reviewed_decisions(terms)
    terms.to_csv(ROOT/'terms.csv',index=False)
    terms.loc[~terms.status.isin(['explicit','informational_notice','superseded_notice'])].to_csv(ROOT/'review_queue.csv',index=False)
    print(terms.status.value_counts().to_string(),flush=True)
    return terms


def apply_reviewed_decisions(terms):
    """Apply narrowly reviewed source decisions, with as-of source guards."""
    folder=ROOT/'review_20261029'
    path=folder/'decisions.json'
    if not path.exists():return terms
    decisions=json.loads(path.read_text(encoding='utf-8'))
    supporting={r['announcement_id']:r for r in json.loads((folder/'supporting_sources.json').read_text(encoding='utf-8'))}
    result=terms.copy()
    for ident,review in decisions.items():
        matches=result.index[result.announcement_id.eq(ident)]
        if len(matches)!=1:raise ValueError('reviewed announcement missing or duplicated: '+ident)
        idx=matches[0];row=result.loc[idx]
        text=normalize((ROOT/'text'/f'{ident}.txt').read_text(encoding='utf-8'))
        if not all(quote in text for quote in review['required_text']):
            raise ValueError('review evidence no longer matches original: '+ident)
        links=[]
        for source_id in review.get('supporting_ids',[]):
            source=supporting[source_id]
            if source['publication_date']>row.announcement_at[:10]:
                raise ValueError('future supporting source cannot backfill commitment')
            if not (HERE/source['text']).is_file():raise ValueError('supporting source unavailable')
            links.append(source['source'])
        replacement=review.get('superseded_by','')
        if replacement:
            linked=result.loc[result.announcement_id.eq(replacement)]
            if len(linked)!=1 or linked.iloc[0].code!=row.code or linked.iloc[0].status!='explicit':
                raise ValueError('invalid replacement linkage')
        result.at[idx,'status']=review['status']
        result.at[idx,'start']=row.announcement_at[:10] if review['status']=='explicit' else ''
        result.at[idx,'end']=review.get('end','')
        result.at[idx,'superseded_by']=replacement
        result.at[idx,'rule']='source_review_20261029_'+review['outcome']
        result.at[idx,'evidence']='；'.join(review['required_text'])+'；'+review['reason']
        result.at[idx,'supporting_sources']=';'.join(links)
    return result


def protection_at(events,date,base_protection):
    """Return explicit extension, provenance, and unresolved latest-decision flag."""
    day=pd.Timestamp(date)
    observed=events.loc[events.available_date<=day]
    if 'superseded_by' in observed:
        # A replacement only neutralizes the old draft after it becomes public.
        replaced=observed.status.eq('superseded_notice') & observed.superseded_by.isin(observed.announcement_id)
        observed=observed.loc[~replaced]
    observed=observed.loc[~observed.status.eq('informational_notice')]
    if observed.empty:return base_protection,'searched_no_observed_commitment',''
    latest_date=observed.available_date.max()
    latest=observed.loc[observed.available_date.eq(latest_date)]
    # A correction can replace an earlier end date. Until reviewed, do not
    # reuse a superseded or potentially cancelled promise as verified evidence.
    if latest.status.isin(['correction_review_required','conflicting_periods_review']).any():
        return base_protection,'unresolved_latest_notice',';'.join(latest.announcement_id.astype(str))
    latest_explicit=latest.loc[latest.status.eq('explicit')]
    if latest_explicit.empty:
        return base_protection,'unresolved_latest_notice',';'.join(latest.announcement_id.astype(str))
    if latest_explicit.end.nunique()>1:
        return base_protection,'unresolved_same_day_conflict',';'.join(latest.announcement_id.astype(str))
    explicit=latest_explicit.loc[(latest_explicit.start<=day)&(latest_explicit.end>=day)]
    if explicit.empty:return base_protection,'explicit_commitment_expired',''
    chosen=explicit.loc[explicit.end.idxmax()]
    # An inclusive end date protects through that day's close. CCB's temporal
    # boundary is the following midnight; this does not add another month.
    years=(chosen.end+pd.Timedelta(days=1)-day).days/365.
    return max(base_protection,years),'explicit_active',str(chosen.announcement_id)


if __name__=='__main__':build_terms()
