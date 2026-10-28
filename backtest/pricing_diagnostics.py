"""Observed-price diagnostics only; no model simulation or strategy rebuild."""
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from update_etf_benchmark import load_cache

BASE=Path(__file__).resolve().parent
MODELS=('BS','ZL','LSM')
COLORS={'BS':'#c88b20','ZL':'#286da8','LSM':'#28846d','Market':'#343a40','ETF':'#b44d63'}


def common_sample(models,market):
    valid=np.isfinite(market)&market.gt(0)
    for frame in models.values():
        if not frame.index.equals(market.index) or not frame.columns.equals(market.columns):
            raise ValueError('Pricing/market axes differ')
        valid &= np.isfinite(frame)&frame.gt(0)
    if not valid.any().any():
        raise ValueError('No common observed pricing sample')
    return valid


def pricing_metrics(models,market):
    valid=common_sample(models,market).to_numpy()
    y=market.to_numpy()[valid]
    variance=np.square(y-y.mean()).sum()
    rows=[]
    for name,frame in models.items():
        error=frame.to_numpy()[valid]-y
        mse=np.square(error).mean()
        rows.append(dict(model=name,n=len(y),mse=float(mse),rmse=float(np.sqrt(mse)),
            mae=float(abs(error).mean()),mape_pct=float((abs(error)/y).mean()*100),
            bias=float(error.mean()),r_squared=float(1-np.square(error).sum()/variance) if variance else np.nan,
            start=str(market.index.min().date()),end=str(market.index.max().date())))
    return pd.DataFrame(rows)


def normalized_comparison(means,etf):
    joint=means.join(etf.rename('ETF'))
    common=joint.dropna().index
    if not len(common):
        raise ValueError('No common observed ETF/model date')
    start=common[0]
    if (joint.loc[start]<=0).any():
        raise ValueError('Nonpositive normalization base')
    # Keep any later missing observations as gaps; never backfill pre-listing.
    return joint.loc[start:].div(joint.loc[start]).mul(100)


def update_readme_metrics(path,metrics,weeks,minimum_coverage,latest_count,latest_quoted):
    """Refresh only the two generated pricing blocks; preserve archived research."""
    import re
    before=Path(path).read_text(encoding='utf-8')
    result=before
    first=metrics.iloc[0]
    rows=''.join(f'| {r.model} | {r.n:,} | {r.mse:.2f} | {r.rmse:.2f} | {r.mae:.2f} | {r.mape_pct:.2f}% | {r.bias:+.2f} | {r.r_squared:.3f} |\n' for r in metrics.itertuples())
    for language in ['zh','en']:
        if language=='zh':
            intro=f'## 最新定价结果与图解（{first.end}）\n\n三个模型使用相同的 **{first.n:,} 个有效券日**，覆盖 **{first.start} 至 {first.end}、{weeks} 个观察周**。MSE单位为元²；RMSE、MAE和平均偏差为元；MAPE为绝对百分比偏差。平均偏差为理论价减市场价，R²相对于市场报价均值计算。最低单周共同覆盖率 **{minimum_coverage:.2f}%**，最新周 **{latest_count}/{latest_quoted}** 只。指标衡量模型与市场报价的偏离。\n\n'
            header='| 模型 | 样本数 | MSE | RMSE | MAE | MAPE | 平均偏差 | R² |\n'
        else:
            intro=f'## Current pricing results and visual diagnostics ({first.end})\n\nAll models use the same **{first.n:,} valid bond-date cells over {weeks} observed weeks, {first.start} through {first.end}**. MSE is in yuan²; RMSE, MAE and bias are in yuan. Bias is model minus market price; R² uses the market-price mean as its constant baseline. Minimum weekly common coverage is **{minimum_coverage:.2f}%**, with **{latest_count}/{latest_quoted}** bonds in the latest week. These are market-price deviation diagnostics.\n\n'
            header='| Model | N | MSE | RMSE | MAE | MAPE | Bias | R² |\n'
        start=f'<!-- pricing-metrics-{language}:start -->'
        end=f'<!-- pricing-metrics-{language}:end -->'
        replacement=start+'\n'+intro+header+'|---|---:|---:|---:|---:|---:|---:|---:|\n'+rows+end
        result,count=re.subn(re.escape(start)+'.*?'+re.escape(end),lambda _:replacement,result,flags=re.S)
        if count!=1:raise ValueError(f'Expected exactly one generated README metrics block: {language}')
    if result!=before:Path(path).write_text(result,encoding='utf-8')


def plot_pricing_diagnostics(base=BASE):
    base=Path(base)
    def read(name):
        return pd.read_csv(base/name,index_col=0,parse_dates=True,float_precision='round_trip')
    models={m:read(f'{m}_Model_Prices.csv') for m in MODELS}
    market=read('Market_Prices.csv')
    valid=common_sample(models,market)
    metrics=pricing_metrics(models,market)
    metrics.to_csv(base/'Pricing_Metrics.csv',index=False)
    end=market.index.max()
    def save(name):
        plt.savefig(base/name,dpi=200,bbox_inches='tight')
        plt.close()
        print(f'  saved: {base/name}')

    means=pd.DataFrame({m:f.where(valid).mean(axis=1) for m,f in models.items()})
    means['Market']=market.where(valid).mean(axis=1)
    means.loc[valid.sum(axis=1)<20]=np.nan
    etf=load_cache(base).set_index('trade_date').adjusted_close
    if end not in etf.index:
        raise ValueError('ETF cache does not cover latest pricing date')
    comparison=normalized_comparison(means,etf)
    fig,ax=plt.subplots(figsize=(12,6))
    for name in [*MODELS,'Market','ETF']:
        label={'Market':'转债市场共同样本均价','ETF':'转债ETF 511380.SH（复权）'}.get(name,name+'模型均价')
        ax.plot(comparison.index,comparison[name],label=label,color=COLORS[name],
            lw=2.2 if name in ('Market','ETF') else 1.3,ls='--' if name=='Market' else '-')
    ax.set(title=f'三模型与转债ETF行情对比 | 截至 {end:%Y-%m-%d}',ylabel='归一化价格（首个共同周 = 100）',xlabel='年份')
    ax.legend(loc='upper left',ncol=2,fontsize=9)
    fig.text(.5,.005,f'共同起点 {comparison.index[0]:%Y-%m-%d}；均价随样本构成变化，不等同于组合收益。ETF上市前不补值。',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.035,1,1))
    save('Fig_Pricing_ETF_Comparison.png')

    deviations={m:((f-market)/market).where(valid)*100 for m,f in models.items()}
    fig,ax=plt.subplots(figsize=(10,5))
    values=[f.to_numpy()[valid.to_numpy()] for f in deviations.values()]
    boxes=ax.boxplot(values,vert=False,tick_labels=MODELS,whis=(5,95),showfliers=False,patch_artist=True)
    for box,m in zip(boxes['boxes'],MODELS):box.set(facecolor=COLORS[m],alpha=.65)
    ax.axvline(0,color='#555',lw=.9)
    ax.set(title='三模型定价偏差分布（相同有效券日样本）',xlabel='（理论价 - 市场价）/ 市场价（%）')
    fig.text(.5,.01,f'每模型 N = {int(valid.sum().sum()):,}；箱体为25–75分位，须线为5–95分位；所有有效样本参与统计。',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.05,1,1))
    save('Fig_Pricing_Deviation_Distribution.png')

    years=sorted(market.index.year.unique())
    annual=np.array([[float(deviations[m].loc[market.index.year==year].stack().mean()) for year in years] for m in MODELS])
    fig,ax=plt.subplots(figsize=(11,3.5))
    bound=max(1.,float(np.abs(annual).max()))
    img=ax.imshow(annual,cmap='RdBu_r',vmin=-bound,vmax=bound,aspect='auto')
    for i in range(3):
        for j in range(len(years)):
            ax.text(j,i,f'{annual[i,j]:+.1f}%',ha='center',va='center',color='white' if abs(annual[i,j])>bound*.6 else '#222')
    ax.set_xticks(range(len(years)),years)
    ax.set_yticks(range(3),MODELS)
    ax.set_title('年度平均相对定价偏差（券日等权）')
    ax.grid(False)
    fig.colorbar(img,ax=ax,label='偏差（%）')
    fig.tight_layout()
    save('Fig_Pricing_Annual_Deviation.png')

    fig,(ax,uncertainty)=plt.subplots(2,1,figsize=(12,8),sharex=True)
    quoted=market.gt(0).sum(axis=1)
    count=valid.sum(axis=1)
    coverage=count/quoted*100
    update_readme_metrics(base.parent/'README.md',metrics,len(market),float(coverage.min()),int(count.iloc[-1]),int(quoted.iloc[-1]))
    ax.plot(count.index,quoted,label='有市场报价',color='#777',ls='--')
    ax.plot(count.index,count,label='三模型共同有效样本',color='#286da8')
    ax.set(ylabel='转债数量',title='有效样本覆盖与蒙特卡罗抽样误差')
    right=ax.twinx()
    right.plot(coverage.index,coverage,color='#9867a5',alpha=.65,label='共同有效覆盖率')
    right.set(ylabel='共同覆盖率（%）',ylim=(max(0,float(coverage.min())-1),100.5))
    lines=[*ax.lines,*right.lines]
    ax.legend(lines,[x.get_label() for x in lines],loc='upper left',fontsize=9)
    for m in ['ZL','LSM']:
        error=read(f'{m}_Monte_Carlo_SE.csv').where(valid)
        median=error.median(axis=1);upper=error.quantile(.95,axis=1)
        uncertainty.plot(median.index,median,color=COLORS[m],label=m+' 中位数')
        uncertainty.plot(upper.index,upper,color=COLORS[m],ls='--',label=m+' 95分位')
    uncertainty.set(ylabel='单券蒙特卡罗标准误（元）',xlabel='年份')
    uncertainty.legend(ncol=2,fontsize=9)
    fig.text(.5,.005,'抽样标准误不包含回归策略不确定性、行权网格偏差与模型假设误差。',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.035,1,1))
    save('Fig_Pricing_Coverage_Uncertainty.png')
    return metrics
