"""Independent checks and a source-backed report for the 2023 waiver run."""
import json

import numpy as np
import pandas as pd

from run_history import HERE,BASE,validate_outputs

OUT=HERE/'results_waivers_2023'
MODELS=['CCB_basic','CCB','BS','ZL','LSM']


def table(frame):
    cols=['model','n','MAE','RMSE','MAPE_pct','bias']
    lines=['| 模型 | 样本数 | MAE | RMSE | MAPE % | 有符号偏差 |',
           '|---|---:|---:|---:|---:|---:|']
    for r in frame[cols].itertuples(index=False,name=None):
        lines.append('| '+' | '.join([r[0],str(r[1])]+[f'{x:.4f}' for x in r[2:]])+' |')
    return '\n'.join(lines)


def main():
    f=pd.read_csv(OUT/'CCB_observations.csv',dtype={'announcement_id':str})
    c=pd.read_csv(OUT/'common_sample.csv',dtype={'announcement_id':str})
    basic=pd.read_csv(HERE/'results/CCB_observations.csv')
    terms=pd.read_csv(HERE/'inputs/waivers/terms.csv',dtype={'announcement_id':str})
    manifest=json.loads((OUT/'manifest.json').read_text(encoding='utf-8'))
    saved=pd.read_csv(OUT/'comparison_overall.csv').set_index('model')
    key=['date','code'];assert not f.duplicated(key).any()
    assert f[key].equals(basic[key])
    assert f.date.min()==manifest['start'] and f.date.max()==manifest['cutoff']
    assert f.date.nunique()==manifest['weeks']
    for model in ['BS','ZL','LSM','market']:
        assert np.allclose(f[model],basic[model],rtol=0,atol=1e-10,equal_nan=True)
    assert np.allclose(f.CCB_basic,basic.CCB,rtol=0,atol=1e-10)
    unresolved=f.waiver_state.str.startswith('unresolved')
    assert f.loc[unresolved,'CCB'].isna().all()
    mask=np.isfinite(f[MODELS+['market']]).all(axis=1)&f[MODELS+['market']].gt(0).all(axis=1)
    pd.testing.assert_frame_equal(f.loc[mask].reset_index(drop=True),c)
    assert len(c)==manifest['common_sample'] and f.CCB.notna().sum()==manifest['prices']
    for model in MODELS:
        error=c[model]-c.market
        assert abs(error.abs().mean()-saved.at[model,'MAE'])<1e-10
        assert abs(np.sqrt((error**2).mean())-saved.at[model,'RMSE'])<1e-10
    active=f.loc[f.waiver_state.eq('explicit_active')]
    evidence=active.merge(terms,on='announcement_id',validate='many_to_one',suffixes=('','_notice'))
    assert len(evidence)==len(active)
    assert evidence.status.eq('explicit').all()
    assert evidence.code.eq(evidence.code_notice).all()
    assert (evidence.available_date<=evidence.date).all()
    assert (evidence.start<=evidence.date).all() and (evidence.end>=evidence.date).all()
    remaining=(pd.to_datetime(evidence.end)+pd.Timedelta(days=1)-pd.to_datetime(evidence.date)).dt.days/365
    assert np.allclose(evidence.protection,np.maximum(evidence.basic_protection,remaining),rtol=0,atol=1e-12)
    changed=f.protection.gt(f.basic_protection+1e-12)&~unresolved
    unchanged=~changed&~unresolved
    assert np.allclose(f.loc[unchanged,'CCB'],f.loc[unchanged,'CCB_basic'],rtol=0,atol=1e-10)
    grid=pd.read_csv(OUT/'observed_grid_convergence.csv')
    assert grid.difference.abs().max()<.05
    # Retain the production artifact guards. No guard or manifest is replaced.
    guards={m:validate_outputs(m,BASE)['input_cutoff'] for m in ['BS','ZL','LSM']}
    acceptance=dict(passed=True,unique_observations=len(f),common_sample=len(c),
        asof_provenance_checks=len(evidence),unchanged_prices_verified=int(unchanged.sum()),
        unresolved_excluded=int(unresolved.sum()),production_artifact_guards=guards,
        grid_cases=len(grid),max_grid_difference=float(grid.difference.abs().max()))
    (OUT/'acceptance.json').write_text(json.dumps(acceptance,ensure_ascii=False,indent=2),encoding='utf-8')
    # Report the treatment effect on identical affected observations separately.
    affected=c.loc[c.protection.gt(c.basic_protection+1e-12)]
    affected_metrics=[]
    for model in MODELS:
        error=affected[model]-affected.market
        affected_metrics.append(dict(model=model,n=len(affected),MAE=error.abs().mean(),RMSE=np.sqrt((error**2).mean()),
                                     MAPE_pct=(error.abs()/affected.market).mean()*100,bias=error.mean()))
    affected_metrics=pd.DataFrame(affected_metrics)
    affected_metrics.to_csv(OUT/'comparison_affected.csv',index=False)
    annual=pd.read_csv(OUT/'comparison_by_year.csv')
    status=terms.status.value_counts()
    coverage=pd.read_csv(OUT/'coverage.csv')
    source_sample=terms.loc[terms.announcement_id.isin(['1218363713','1214308301','1221739601','1217560801'])]
    links='\n'.join(f'- [{r.title}]({r.source})：{r.start} 至 {r.end}；可用日 {r.available_date}。' for r in source_sample.itertuples())
    report=f'''# CCB 2023：明确不赎回承诺补充结果

按 2023 版保护期逻辑纳入可从公告确定期限的不赎回承诺。估值区间为 {manifest['start']} 至 {manifest['cutoff']}，覆盖 {manifest['weeks']} 个周度截面，共有 {manifest['prices']:,} 个有效增强版价格。

## 结果

共同样本有 {len(c):,} 个券日、{c.code.nunique()} 只债券，覆盖 {c.date.min()} 至 {c.date.max()} 的 {c.date.nunique()} 个周度截面。

{table(saved.reset_index())}

CCB_basic 为基础保护期版本在上述样本上的误差，CCB 为纳入明确承诺后的误差。相对基础版，增强版 MAE 变化 {saved.at['CCB','MAE']-saved.at['CCB_basic','MAE']:+.4f} 元，RMSE 变化 {saved.at['CCB','RMSE']-saved.at['CCB_basic','RMSE']:+.4f} 元。

实际延长保护期的 {len(affected):,} 个共同观测单独比较：

{table(affected_metrics)}

2026年同样本结果：

{table(annual.loc[annual.year.eq(2026)])}

## 保护期与时点

`保护期 = max(实际转股起始日剩余时间, 1/12年, 已披露明确不赎回期限的剩余时间)`。
承诺期末按公告的包含当日口径处理，之后不额外再加一个月。更正公告只从其可用日覆盖旧期限。
盘后公告及只有日期、没有可靠时刻的公告，从下一交易日纳入；历史引用不会回填到新公告发布之前。
连续延期仅在原文给出相接的明确区间时合并。仅披露股东交易、核查意见或触发预告的文件不作为新承诺。
公告“直到到期日”能够明确给出日期时，以公告日期为准，并保留原文；不会把数据库的次日兑付日期当成承诺截止日。

继续使用观测国债期限曲线、实际合同现金流及赎回阈值，未恢复固定2%利率。
这属于按用户要求对2023框架的实际数据扩展，并非研报全部固定参数的逐项复刻。
回售和未来下修仍按基础CCB省略；平坦信用利差、零股息等局限见[原报告](../results/REPORT.md)。

## 公告覆盖与缺口

巨潮资讯检索覆盖 {manifest['search_start']} 至 {manifest['search_end']}，查询词为“不赎回”和“不提前赎回”。原始分页响应、PDF、提取文字和债券映射均可复核。
下载并处理 {len(terms)} 份与基础样本发行人相关的公告，其中 {status.get('explicit',0)} 份明确期限、{status.get('informational_notice',0)} 份信息性文件、{status.get('no_explicit_future_period',0)} 份尚不能确定未来期限、{status.get('correction_review_required',0)} 份取消/更正状态待核，另有 {status.get('superseded_notice',0)} 份旧稿已关联更新版。
最新公告仍未解决时，增强版留空并从同样本比较排除，共 {int(unresolved.sum()):,} 个观测；详见[待核队列](../inputs/waivers/review_queue.csv)。
保护期实际延长 {int(changed.sum()):,} 个观测、{f.loc[changed,'code'].nunique()} 只债券。

**这不是完整历史承诺数据库。** 关键词覆盖、历史网页变更、未写明期限和公告撤销时点仍有限制。
“未检索到承诺”沿用基础保护期，仅表示当前检索中没有可用记录，不证明发行人没有作出承诺。
旧稿仅在其更新版已经可用时退出冲突判断，不能将当前撤销状态当作历史当日已知信息。
2026-10-29 对原 194 份待核公告的逐项复核见[审计明细](../inputs/waivers/review_20261029/audit.csv)及[复核报告](REVIEW_20261029.md)。
2026-11-25 的更正关系和同期披露核查见[复核记录](../inputs/waivers/review_20261125/README.md)。

可追溯公告示例：

{links}

## 验证与文件

对 {len(grid)} 个受影响观测加密网格，最大差异 {grid.difference.abs().max():.6f} 元，低于 0.05 元数值容差。
逐项验证了公告可用日、债券映射、承诺期限、相同样本交集、未调整价格保持一致，以及MAE/RMSE独立复算。
生产BS/ZL/LSM通过现有artifact完整性校验，本次未重算或改写生产定价。

- [比较总表](comparison_overall.csv)、[分年比较](comparison_by_year.csv)、[实际受影响样本](comparison_affected.csv)
- [共同样本与逐项来源](common_sample.csv)、[全部观测与缺失状态](CCB_observations.csv)
- [公告期限表](../inputs/waivers/terms.csv)、[检索覆盖](../inputs/waivers/search_coverage.csv)
- [运行参数](manifest.json)、[验证记录](acceptance.json)、[网格差异](observed_grid_convergence.csv)
'''
    (OUT/'REPORT.md').write_text(report,encoding='utf-8')
    print(json.dumps(acceptance,ensure_ascii=False),flush=True)


if __name__=='__main__':main()
