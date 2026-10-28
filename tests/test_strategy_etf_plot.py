from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backtest'))
from regenerate_plots import strategy_etf_comparison


def test_etf_preserves_full_nav_history_and_leaves_unavailable_dates_empty():
    dates=pd.date_range('2020-04-06',periods=4)
    navs={name:pd.DataFrame({'date':dates,'nav':values,
        'benchmark_nav':[1.1,1.2,1.3,1.4],'marked_prices_verified':True})
        for name,values in {'long_only':[1.,1.25,1.5,2.],
                            'long_short':[2.,2.5,2.7,3.]}.items()}
    etf=pd.Series([10.,11.],index=dates[[1,3]])
    result=strategy_etf_comparison(navs,etf)
    assert result.index.equals(dates)
    assert result.iloc[0].dropna().eq(1).all()
    assert np.isnan(result.loc[dates[0],'ETF'])
    assert result.loc[dates[1],'ETF']==1
    assert result.loc[dates[3],'long_only']==pytest.approx(2.)
    assert result.loc[dates[3],'long_short']==pytest.approx(1.5)
    assert result.loc[dates[3],'000832.CSI']==pytest.approx(1.4/1.1)
    assert result.loc[dates[3],'ETF']==pytest.approx(1.1)
    assert np.isnan(result.loc[dates[2],'ETF'])
    shorter=strategy_etf_comparison(navs,etf.iloc[:1])
    assert shorter.index.equals(dates)
    assert np.isnan(shorter.loc[dates[3],'ETF'])
    navs['long_only'].loc[2,'marked_prices_verified']=False
    assert np.isnan(strategy_etf_comparison(navs,etf).loc[dates[2],'long_only'])
