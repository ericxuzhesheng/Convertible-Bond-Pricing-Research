from pathlib import Path
import sys
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backtest'))
from pricing_diagnostics import pricing_metrics,normalized_comparison,update_readme_metrics
from update_etf_benchmark import combine_observations,load_cache
import update_etf_benchmark as etf_cache


def test_metrics_use_identical_observed_cells_and_expected_units():
    dates=pd.to_datetime(['2024-01-05','2024-01-12'])
    market=pd.DataFrame({'A':[100.,200.],'B':[50.,0.]},index=dates)
    models={'BS':market+10.,'ZL':market-10.,'LSM':market.copy()}
    models['LSM'].loc[dates[0],'B']=np.nan
    result=pricing_metrics(models,market).set_index('model')
    assert result.n.to_dict()=={'BS':2,'ZL':2,'LSM':2}
    assert result.loc['BS','mse']==100
    assert result.loc['BS','rmse']==10
    assert result.loc['BS','mae']==10
    assert result.loc['BS','mape_pct']==pytest.approx(7.5)
    assert result.loc['BS','r_squared']==pytest.approx(.96)
    assert result.loc['ZL','bias']==-10
    assert result.loc['LSM','mse']==0


def test_etf_comparison_never_backfills_listing_or_later_missing_dates():
    dates=pd.date_range('2020-04-03',periods=4,freq='7D')
    means=pd.DataFrame({'Market':[90.,100.,110.,120.],'LSM':[180.,200.,220.,240.]},index=dates)
    etf=pd.Series([10.,12.],index=dates[[1,3]])
    normalized=normalized_comparison(means,etf)
    assert normalized.index.min()==dates[1]
    assert (normalized.iloc[0]==100).all()
    assert np.isnan(normalized.loc[dates[2],'ETF'])
    assert normalized.loc[dates[3],'ETF']==120
    assert normalized.loc[dates[3],'LSM']==120


def test_generated_readme_metrics_refresh_without_touching_archived_text(tmp_path):
    dates=pd.to_datetime(['2024-01-05','2024-01-12'])
    market=pd.DataFrame({'A':[100.,200.]},index=dates)
    metrics=pricing_metrics({'BS':market+10.,'ZL':market-10.,'LSM':market},market)
    p=tmp_path/'README.md'
    blocks='\n'.join(f'<!-- pricing-metrics-{lang}:start -->old<!-- pricing-metrics-{lang}:end -->' for lang in ['zh','en'])
    p.write_text('User introduction\n'+blocks+'\nArchived v1 statistics: 2026-08-28',encoding='utf-8')
    update_readme_metrics(p,metrics,2,97.35,312,313)
    content=p.read_text(encoding='utf-8')
    assert content.startswith('User introduction\n') and content.endswith('Archived v1 statistics: 2026-08-28')
    assert '2024-01-12' not in content and '| BS | 2 | 100.00 | 10.00 |' in content
    assert '312/313' in content and '97.35%' in content


def test_adjustment_factor_must_match_the_same_observed_etf_date():
    daily=pd.DataFrame({'ts_code':['511380.SH'],'trade_date':['20200407'],'close':[10.]})
    factors=pd.DataFrame({'ts_code':['511380.SH'],'trade_date':['20200407'],'adj_factor':[2.]})
    result=combine_observations(daily,factors,pd.Timestamp('2020-04-07'),pd.Timestamp('2020-04-10'))
    assert result.adjusted_close.iloc[0]==20
    factors['trade_date']='20200408'
    with pytest.raises(ValueError,match='adjustment factor missing'):
        combine_observations(daily,factors,pd.Timestamp('2020-04-07'),pd.Timestamp('2020-04-10'))


def test_etf_update_preserves_history_and_no_new_date_is_a_true_noop(tmp_path,monkeypatch):
    import json,hashlib
    for model in ['BS','ZL','LSM']:
        (tmp_path/f'{model}_Model_Manifest.json').write_text(json.dumps({'input_cutoff':'2024-01-12'}))
    pd.DataFrame({'A':[100.,101.]},index=pd.to_datetime(['2024-01-05','2024-01-12'])).to_csv(tmp_path/'Market_Prices.csv')
    old=pd.DataFrame({'trade_date':['2024-01-05'],'ts_code':['511380.SH'],'close':[10.],'adj_factor':[2.],'adjusted_close':[20.]})
    old.to_csv(tmp_path/etf_cache.CACHE,index=False)
    (tmp_path/etf_cache.MANIFEST).write_text(json.dumps({'sha256':hashlib.sha256((tmp_path/etf_cache.CACHE).read_bytes()).hexdigest()}))
    class Pro:
        calls=0
        def fund_daily(self,**kwargs):
            self.calls+=1
            assert kwargs['start_date']=='20240106'
            return pd.DataFrame({'ts_code':['511380.SH'],'trade_date':['20240112'],'close':[11.]})
        def fund_adj(self,**kwargs):
            self.calls+=1
            return pd.DataFrame({'ts_code':['511380.SH'],'trade_date':['20240112'],'adj_factor':[2.]})
    monkeypatch.setattr(etf_cache.time,'sleep',lambda _:None)
    pro=Pro()
    merged=etf_cache.update(base=tmp_path,pro=pro)
    assert merged.close.tolist()==[10.,11.]
    assert merged.adjusted_close.tolist()==[20.,22.]
    before=[(tmp_path/n).read_bytes() for n in [etf_cache.CACHE,etf_cache.MANIFEST]]
    etf_cache.update(base=tmp_path,pro=pro)
    assert pro.calls==2
    assert before==[(tmp_path/n).read_bytes() for n in [etf_cache.CACHE,etf_cache.MANIFEST]]
    with (tmp_path/etf_cache.CACHE).open('a') as stream:stream.write('\n')
    with pytest.raises(ValueError,match='integrity'):
        load_cache(tmp_path)
