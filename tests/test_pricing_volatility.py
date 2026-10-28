from pathlib import Path
import json
import sys
import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backtest'))
import update_pricing_volatility as module
from market_data_contracts import build_observed_volatility


def test_an_advanced_independent_model_protects_its_historical_missing_volatility(tmp_path):
    days=pd.to_datetime(['2024-01-05','2024-01-12'])
    pd.DataFrame({'A':[100.,101.]},index=days).to_csv(tmp_path/'cb_price_cache.csv')
    cache=tmp_path/'bs_volatility_cache.csv'
    pd.DataFrame({'A':[.3,np.nan]},index=days).to_csv(cache)
    (tmp_path/'ZL_Model_Manifest.json').write_text(json.dumps({'input_cutoff':'2024-01-05'}))
    (tmp_path/'LSM_Model_Manifest.json').write_text(json.dumps({'input_cutoff':'2024-01-12'}))
    before=cache.read_bytes()
    def forbidden(**kwargs):raise AssertionError('Historical holes must not trigger a source request')
    module.update(base=tmp_path,after='2024-01-05',pro=object(),bar=forbidden)
    assert cache.read_bytes()==before


def test_asof_volatility_uses_only_recent_past_quotes_and_never_future_quotes():
    close=pd.Series([100.,110.,105.,110.,1e9],
        index=pd.to_datetime(['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-20']))
    target=pd.to_datetime(['2024-01-05','2024-01-12','2023-12-31'])
    result=build_observed_volatility(adjusted_close=close,target_dates=target,
        window=3,min_observations=3,max_staleness_days=7)
    expected=np.std(np.log([105/110,110/105]),ddof=1)*np.sqrt(250)
    assert np.isclose(result.iloc[0],expected)
    assert result.iloc[1:].isna().all()


def test_new_observed_volatility_preserves_history_and_groups_underlying_fetch(tmp_path,monkeypatch):
    dates=pd.to_datetime(['2024-01-05','2024-01-12'])
    pd.DataFrame({'A':[100.,101.],'B':[110.,111.]},index=dates).to_csv(tmp_path/'cb_price_cache.csv')
    old=pd.DataFrame({'A':[np.nan,np.nan],'B':[.31,.42]},index=dates)
    cache=tmp_path/'bs_volatility_cache.csv';old.to_csv(cache)
    pd.DataFrame({'ts_code':['A','B'],'stk_cd':['S','S']}).to_csv(tmp_path/'cb_basic_info.csv',index=False)
    calls=[]
    def bar(**kwargs):
        calls.append(kwargs['ts_code'])
        days=pd.bdate_range('2022-10-01','2024-01-12')
        return pd.DataFrame({'trade_date':days.strftime('%Y%m%d'),
            'close':100*np.exp(.01*np.sin(np.arange(len(days))))})
    monkeypatch.setattr(module.time,'sleep',lambda _:None)
    module.update(base=tmp_path,after='2024-01-05',pro=object(),bar=bar)
    result=pd.read_csv(cache,index_col=0,parse_dates=True)
    assert pd.isna(result.loc[dates[0],'A'])
    assert result.loc[dates[0],'B']==.31 and result.loc[dates[1],'B']==.42
    assert result.loc[dates[1],'A']>0 and calls==['S']
    before=cache.read_bytes()
    module.update(base=tmp_path,after='2024-01-12',pro=object(),bar=bar)
    assert cache.read_bytes()==before and calls==['S']
