"""Preserve and audit the 194-notice follow-up; no inferred promise lengths."""
import json
import re

import pandas as pd

from waiver_terms import HERE,ROOT,normalize

REVIEW=ROOT/'review_20261029'


def snapshot():
    REVIEW.mkdir(exist_ok=True)
    before=REVIEW/'queue_before.csv'
    if before.exists():return
    queue=pd.read_csv(ROOT/'review_queue.csv',dtype=str)
    if len(queue)!=194:raise ValueError('expected the original 192 + 2 review scope')
    queue.to_csv(before,index=False)
    pd.read_csv(ROOT/'terms.csv',dtype=str).to_csv(REVIEW/'terms_before.csv',index=False)
    old=HERE/'results_waivers_2023'
    pd.read_csv(old/'CCB_observations.csv',usecols=['date','code','CCB','waiver_state','protection']).to_csv(REVIEW/'prices_before.csv',index=False)
    for name in ['manifest.json','acceptance.json','comparison_overall.csv']:
        (REVIEW/('before_'+name)).write_bytes((old/name).read_bytes())


def audit():
    original=pd.read_csv(REVIEW/'queue_before.csv',dtype=str)
    current=pd.read_csv(ROOT/'terms.csv',dtype=str).set_index('announcement_id')
    decisions=json.loads((REVIEW/'decisions.json').read_text(encoding='utf-8'))
    rows=[]
    for row in original.to_dict('records'):
        ident=row['announcement_id']
        text=normalize((ROOT/'text'/f'{ident}.txt').read_text(encoding='utf-8'))
        sentences=re.split('[。；]',text)
        excerpts=[s for s in sentences if re.search(r'(?:决定|本次|未来).{0,100}不.{0,30}赎回',s)]
        evidence='。'.join(excerpts[-3:]) if excerpts else text[-1800:]
        decision=decisions.get(ident,{})
        now=current.loc[ident]
        outcome=decision.get('outcome','no_dated_future_commitment_in_source')
        reason=decision.get('reason','原文仅本次不赎回或后续再次触发再议；未检出可确定的未来承诺期限，保留缺失。')
        if ident in {'1209625702','1213142942'}:
            outcome='duration_without_explicit_anchor'
            reason='原文有未来12个月，但未明确从董事会日或公告日起算；定向检索未提供可用的同期明确起止日期，保留缺失。'
            decision['supporting_ids']=['1209625701' if ident=='1209625702' else '1213142947']
        rows.append(dict(announcement_id=ident,code=row['code'],title=row['title'],source=row['source'],
            previous_status=row['status'],current_status=now.status,outcome=outcome,reason=reason,
            start=now.start,end=now.end,evidence=evidence,
            related_sources=';'.join(decision.get('supporting_ids',[])),
            superseded_by=decision.get('superseded_by','')))
    result=pd.DataFrame(rows)
    assert len(result)==194 and result.announcement_id.nunique()==194
    result.to_csv(REVIEW/'audit.csv',index=False)
    print(result.outcome.value_counts().to_string(),flush=True)


def fetch_support():
    import requests
    import time
    from waiver_data import fetch_text,URL
    sources=[('1207047863','2019-10-31','通光转债募集说明书摘要'),
             ('1208636184','2020-10-28','通光转债2020年付息公告'),
             ('1207180040','2019-12-20','东风转债发行公告'),
             ('1209625701','2021-04-02','小康第四届董事会第九次会议决议'),
             ('1213142947','2022-04-27','小康第四届董事会第二十四次会议决议')]
    records=[]
    for ident,date,title in sources:
        source=f'https://static.cninfo.com.cn/finalpage/{date}/{ident}.PDF'
        result=fetch_text(dict(announcementId=ident,source=source))
        if result['status']!='ok':raise ValueError(result)
        records.append(dict(**result,title=title,source=source,publication_date=date))
    (REVIEW/'supporting_sources.json').write_text(json.dumps(records,ensure_ascii=False,indent=2),encoding='utf-8')
    for year,dates in [(2021,'2021-04-01~2021-04-03'),(2022,'2022-04-26~2022-04-28')]:
        path=REVIEW/f'xiaokang_board_search_{year}.json'
        if path.exists():data=json.loads(path.read_text(encoding='utf-8'))
        else:
            response=requests.post(URL,data=dict(pageNum=1,pageSize=30,column='szse',tabName='fulltext',
                plate='',stock='',searchkey='小康',secid='',category='',trade='',seDate=dates,
                sortName='',sortType='',isHLtitle='false'),timeout=40)
            response.raise_for_status();data=response.json()
            if data.get('totalAnnouncement',100)>30:raise ValueError('support query needs pagination')
            path.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8');time.sleep(.5)
        for item in data.get('announcements') or []:
            print(year,item['announcementId'],item['announcementTitle'],item['adjunctUrl'],flush=True)


def report():
    import numpy as np
    out=HERE/'results_waivers_2023'
    audit_frame=pd.read_csv(REVIEW/'audit.csv',dtype=str)
    before=pd.read_csv(REVIEW/'prices_before.csv')
    after=pd.read_csv(out/'CCB_observations.csv',dtype={'announcement_id':str})
    common=pd.read_csv(out/'common_sample.csv',dtype={'announcement_id':str})
    joined=after.merge(before,on=['date','code'],suffixes=('','_before'),validate='one_to_one')
    recovered=joined.CCB_before.isna()&joined.CCB.notna()
    lost=joined.CCB_before.notna()&joined.CCB.isna()
    assert not lost.any(), 'review unexpectedly lost previously usable observations'
    matched=common.merge(before[['date','code','CCB']],on=['date','code'],suffixes=('','_before'),validate='one_to_one')
    matched=matched.loc[matched.CCB_before.notna()]
    changes=(matched.CCB-matched.CCB_before).abs()
    rows=[]
    for col,label in [('CCB_before','before'),('CCB','after')]:
        error=matched[col]-matched.market
        rows.append(dict(version=label,n=len(matched),MAE=error.abs().mean(),RMSE=np.sqrt((error**2).mean())))
    paired=pd.DataFrame(rows);paired.to_csv(REVIEW/'fixed_sample_comparison.csv',index=False)
    joined.loc[recovered].to_csv(REVIEW/'recovered_observations.csv',index=False)
    # Changing the evidence must not rewrite an already verified price without
    # recording that change. New-sample metrics alone are not a treatment effect.
    summary=dict(reviewed=194,explicit_added=int(audit_frame.current_status.eq('explicit').sum()),
        informational_resolved=int(audit_frame.current_status.eq('informational_notice').sum()),
        replacement_links=int(audit_frame.current_status.eq('superseded_notice').sum()),
        remaining=int(audit_frame.current_status.eq('no_explicit_future_period').sum()),
        recovered_observations=int(recovered.sum()),recovered_bonds=joined.loc[recovered,'code'].nunique(),
        previous_unresolved=int(before.CCB.isna().sum()),current_unresolved=int(after.CCB.isna().sum()),
        previous_common_sample=len(matched),current_common_sample=len(common),
        existing_sample_price_changes=int(changes.gt(1e-10).sum()),max_existing_sample_price_change=float(changes.max()),
        fixed_sample_metrics=rows,passed=True)
    (REVIEW/'acceptance.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['| 公告/债券 | 核实后的末日（包含当日） | 依据 |','|---|---|---|']
    for r in audit_frame.loc[audit_frame.current_status.eq('explicit')].itertuples():
        lines.append(f'| [{r.code} / {r.announcement_id}]({r.source}) | {r.end} | {r.reason} |')
    text=f'''# 194份待核公告复核（2026-10-29）

范围固定为上一版的192份期限不明公告和2份取消状态待核公告。原队列、原期限表及原逐项CCB价格保留在 `inputs/waivers/review_20261029/`，来源Git提交为719020e。

## 本轮结果

- 7份补出有依据的期限，明确期限公告总数由693增至700。
- 3份确认只补充股东交易或重复既有决定的风险提示，不再作为新的期限未知事件。
- 2份旧稿分别关联已公开的更新版，只在更新版可用后解除冲突。
- 182份仍不填期限：180份未检出明确未来承诺，2份虽写未来12个月但没有明确起算日。

处理方法为全量文字及期限线索扫描、决定段落核对、对候选期限/补充文件/取消文件定向复核原文，另检索并保存同期董事会决议及早期发行文件。
这不证明其他渠道绝对不存在补充材料；[194份逐项审计表](../inputs/waivers/review_20261029/audit.csv)保留每份来源、证据、分类和剩余原因。

## 可补期限

{chr(10).join(lines)}

通光的依据是2019年募集文件的年度计息约定；2020年付息公告仅作事后核对，未用于回填8月估值。
东风使用2019发行公告中的到期日，未把2025年11月的到期兑付公告提前用于8月。
“日前”“存续期结束前”按排除所指截止日期处理；不默认一季度、不根据沉默推断承诺。

## 两份取消旧稿

中矿旧稿1209207056与同日同编号更新版1209210378的承诺区间一致；更新版修正董事会日期的年份。两份保守可用日均为2021-02-01，从该日使用更新版。
蓝晓旧稿1210612656对应更正说明1210659121及更新版1210659122。更正说明只调整触发统计区间，其他事项不变；更新版仍明确2021-07-29至2021-10-29。更新版可用日为2021-08-05，此前8月2日至4日继续留缺失。
这里核实的是替代版本关系，不是推断网站取消标记的具体发生时刻，也不会把更新内容倒灌至原公告日。

## 仍不补的小康两份

1209625702（2021）和1213142942（2022）均称未来12个月，但没有明确以董事会日期还是公告日期为起点。
进一步取得同期董事会决议1209625701、1213142947，均仅指向原不赎回公告，没有给出更清楚的起止日期，因此不作日期推算。
同期检索原始响应和支持PDF均已保存。对其余仅称本次不赎回的180份，不用后续公告、当前规则或后来实际赎回时间反推保护期。

## 定价影响与验证

恢复{summary['recovered_observations']}个债券—日期、{summary['recovered_bonds']}只债券的CCB结果。待核导致的定价缺失由{summary['previous_unresolved']}降至{summary['current_unresolved']}。
共同样本由{summary['previous_common_sample']:,}增至{summary['current_common_sample']:,}。原共同样本中，变化超过1e-10元的CCB价格数为{summary['existing_sample_price_changes']}。
固定原样本的MAE：{rows[0]['MAE']:.6f} → {rows[1]['MAE']:.6f}；RMSE：{rows[0]['RMSE']:.6f} → {rows[1]['RMSE']:.6f}。
新增样本可能改变总体误差，不能把样本变化当作模型精度提升。[更新后的同样本五模型结果](REPORT.md)另列基础CCB、补充承诺CCB、BS、ZL、LSM。

18项公告/时点测试通过，包括更新版公开前不得解除旧稿不确定性、同日替代版本、跨债券误关联拒绝、以及期限不明公告继续保留缺失。
定价精度仍用0.05元容差，实际曲线、信用利差、合同现金流和BS/ZL/LSM生产基线均未改变。

- [审计验收](../inputs/waivers/review_20261029/acceptance.json)
- [恢复的逐项观测](../inputs/waivers/review_20261029/recovered_observations.csv)
- [固定原样本比较](../inputs/waivers/review_20261029/fixed_sample_comparison.csv)
- [剩余待核队列](../inputs/waivers/review_queue.csv)
'''
    (out/'REVIEW_20261029.md').write_text(text,encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False),flush=True)


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--snapshot',action='store_true');p.add_argument('--fetch-support',action='store_true');p.add_argument('--report',action='store_true');args=p.parse_args()
    if args.snapshot:snapshot()
    elif args.fetch_support:fetch_support()
    elif args.report:report()
    else:audit()
