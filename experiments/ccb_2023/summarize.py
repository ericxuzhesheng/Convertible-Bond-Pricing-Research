"""Generate the standalone comparison report from completed, validated outputs."""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

HERE=Path(__file__).resolve().parent
OUT=HERE/'results'


def table(frame,columns):
    header='| '+' | '.join(columns)+' |\n| '+' | '.join(['---']*len(columns))+' |\n'
    lines=[]
    for row in frame[columns].itertuples(index=False,name=None):
        lines.append('| '+' | '.join(f'{x:.4f}' if isinstance(x,float) else str(x) for x in row)+' |')
    return header+'\n'.join(lines)


def main():
    m=json.loads((OUT/'manifest.json').read_text(encoding='utf-8'))
    total=pd.read_csv(OUT/'comparison_overall.csv')
    yearly=pd.read_csv(OUT/'comparison_by_year.csv')
    issues=pd.read_csv(OUT/'pricing_issues.csv')
    before=pd.read_csv(HERE/'inputs/missing_before.csv')
    after=pd.read_csv(HERE/'inputs/missing_after.csv')
    repairs=pd.read_csv(HERE/'inputs/repairs.csv')
    vols=pd.read_csv(HERE/'inputs/volatility_gap_diagnosis.csv')
    common=pd.read_csv(OUT/'common_sample.csv',usecols=['date','code','CCB','BS','ZL','LSM','market'])
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False})
    colors={'CCB':'#c44921','BS':'#2d74a8','ZL':'#27846a','LSM':'#8056a3'}
    fig,axes=plt.subplots(1,2,figsize=(12,4.5),layout='constrained')
    for model,g in yearly.groupby('model',sort=False):
        for ax,metric in zip(axes,['MAE','RMSE']):
            ax.plot(g.year,g[metric],marker='o',label=model,color=colors[model],linewidth=2)
            ax.set(xlabel='Valuation year',ylabel=f'{metric} (yuan per bond)')
            ax.grid(alpha=.2);ax.set_xticks(sorted(yearly.year.unique()))
    axes[0].legend(frameon=False,ncol=2)
    fig.suptitle(f'Identical bond-date sample | {len(common):,} observations | through {m["cutoff"]}')
    fig.savefig(OUT/'comparison_by_year.png',dpi=160);plt.close(fig)
    rows=[]
    for year,g in yearly.groupby('year'):
        row={'year':year,'n':int(g.n.iloc[0])}
        row.update({r.model:float(r.RMSE) for r in g.itertuples()});rows.append(row)
    issue_counts=issues.groupby('reason').size().rename('observations').reset_index().sort_values('observations',ascending=False)
    issue_counts.to_csv(OUT/'issue_summary.csv',index=False)
    recent=yearly.loc[yearly.year.eq(yearly.year.max())].set_index('model')
    text=f'''# 基础 CCB 与 BS / ZL / LSM 同样本比较

估值区间：2019-01-04 至 {m['cutoff']}，{m['weeks']} 个周度截面。
CCB 共得到 {m['prices']:,} 条价格；四模型共同有效样本 {m['common_sample']:,} 条、{m['common_bonds']} 只债。
本次只做定价与市场报价的比较，不是策略收益回测。未重算、覆盖或发布现有 BS/ZL/LSM 基线。

## 全样本结果

{table(total,['model','n','MAE','RMSE','MAPE_pct','bias','R2','median_absolute_error'])}

MAE、RMSE、bias、绝对误差中位数单位为元/张；MAPE 为百分数；bias=模型价格−市场报价。
同一行券日必须同时有四个正的有限模型价格及市场报价，且已有三模型的市场报价与输入一致。
所有模型采用完全相同的样本、每个券日等权，没有按单个模型另选样本。
上述指标衡量与市场价格的偏离，不能证明谁的真实公允价值更准确。
全期 CCB 的 MAE 低于 BS、ZL，高于 LSM；RMSE 略高于 BS 和 LSM。
但最新2026年样本中，CCB 的 RMSE 为 {recent.at['CCB','RMSE']:.2f} 元，BS 为 {recent.at['BS','RMSE']:.2f} 元，
CCB 并未在最新年份取得更好的市场拟合。不能用全期平均代替逐年结论。

## 年度 RMSE

{table(pd.DataFrame(rows),['year','n','CCB','BS','ZL','LSM'])}

![年度同样本 MAE 与 RMSE](comparison_by_year.png)

## 模型设定与原研报的关系

- 来源：国盛证券《可转债定价模型与应用》（2023-03-02），第5—9页及第30—31页。
- 保留基础 CCB 的经济假设：保护期内不赎回；之后连续触及赎回线即终止并转股；省略回售和未来下修。
- 保护期为 max(估值日至实际转股开始日的剩余年数, 1/12年)。没有把每只新债统一填成半年。
- 按用户后续要求，主结果**不用固定2%利率**：逐估值日读取国债期限曲线，年有效收益率转为连续利率后按期限插值。
- 使用已观测合同赎回阈值、实际票息和兑付金额。到期兑付已包含末期票息，不重复计算。
- 尊重实际最后转股日；若晚于该日还有现金支付，先将其折现到最后转股日，再作选择。
- 信用风险遵循研报的现金流折减法：到期比较的是信用折减后的现金价值与转股价值。信用利差仅由债底校准，不用转债市场价格倒推。
- 一条债底观测只能识别一个平坦利差；本项目尚无逐券历史信用利率期限结构，未伪造期限利差。政府利率曲线仍完整使用。
- 波动率沿用现有三模型的真实正股历史口径：至多250个复权收盘价、至少60个、年化系数 sqrt(250)，仅允许7个日历日内的已观测值。
- 转股价值过程股息率为0，与现有三模型一致；未声称已纳入逐券未来分红预测。
- **没有历史不赎回公告保护期覆盖层**。这是一月下限的基础情景，不是已验证的发行人真实行为，也不是 CCBA。
- 因使用实际曲线、合同阈值及转股截止日期，本结果是“2023 CCB 经济假设的实际输入扩展”，不是研报固定参数结果的逐项复刻。现有 BS/ZL/LSM 的行权与信用处理也不同。

## 数值验证

- 常利率确定性积分参考，与研报权益/债券现金流分解等价；固定2%只用于这些验证案例。
- 主求解器：对数价格网格上的 Crank–Nicolson PDE，跳跃事件后使用 Rannacher 平滑；网格 dx={m['dx']}，最大步长={m['max_time_step']:.8f} 年。
- 独立 Brownian-bridge 蒙特卡罗：{m['numerical_validation']['cases']} 组，每组32,768条路径，标准误按独立反向配对均值计算；最大标准化偏差 {m['numerical_validation']['max_mc_z']:.3f}。
- {m['observed_grid_cases']} 个真实券日覆盖各年的平价、波动率、剩余期限及保护期极值与内部点；网格加密后最大变化 {m['max_observed_grid_difference']:.6f} 元。
- 检查记录：[数值核验](numerical_validation.csv)、[真实样本精度检查](observed_grid_convergence.csv)。抽样精度检查不构成所有券日的统一误差上界。

## 补数及剩余缺口

- 本次已上市且有报价样本原有基础缺失：{len(before)} 个字段单元（债底{int(before.field.eq('floor').sum())}、波动率{int(before.field.eq('volatility').sum())}；同一券日可能同时缺两项。
- Tushare 缺失债底重新查询后仍未返回；从东方财富历史数据取得并通过同日平价核对的独立债底 {len(repairs)} 条，全部有来源记录。
- 这些东方财富值不是同口径 Tushare 债底，只用于补充 CCB 研究覆盖；程序断言四模型共同样本不包含这些来源替换。
- 剩余字段单元 {len(after)} 个：债底{int(after.field.eq('floor').sum())}、波动率{int(after.field.eq('volatility').sum())}。
- 波动率缺口中 {int(vols.full_day_suspension.sum())}/{len(vols)} 条对应正股全天停牌，保留缺失。停牌证据来自 [Tushare suspend_d](https://tushare.pro/document/2?doc_id=214)，未用假报价或常数波动率填平。
- 缺失字段数量不等于模型排除数量；部分补入债底仍可能不满足现金流一致性检查。以下按模型实际退出原因计数。

{table(issue_counts,['reason','observations'])}

合同不完整、生命周期之外、债底超过合同无风险现金流现值等记录不以默认值强行纳入。
源数据及差异：[修复明细](../inputs/repairs.csv)、[不同债底来源的重叠比较](../inputs/alternative_floor_overlap.csv)、[剩余缺失](../inputs/missing_after.csv)、[停牌核查](../inputs/volatility_gap_diagnosis.csv)。

## 可复现文件

- [逐券 CCB 与三模型价格](CCB_observations.csv)
- [四模型共同样本](common_sample.csv)
- [总体指标](comparison_overall.csv)、[年度指标](comparison_by_year.csv)、[按平价分组](comparison_by_parity.csv)
- [周度覆盖率](coverage.csv)、[逐券排除原因](pricing_issues.csv)、[运行记录](manifest.json)
- `../inputs/snapshot/` 保存本次实际使用的周度输入、合同信息、曲线和三模型来源 manifest。
- 三模型来源提交：`{m['source_commit']}`。原有 manifest 的工作簿和CSV完整性校验已通过。
'''
    (OUT/'REPORT.md').write_text(text,encoding='utf-8')
    print('Report and chart generated:',OUT/'REPORT.md')


if __name__=='__main__':main()
