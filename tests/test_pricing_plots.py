from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backtest'))
import regenerate_plots


def test_pricing_only_refresh_does_not_touch_legacy_research(monkeypatch):
    calls=[]
    names=['plot_bs_timeseries','plot_zl_timeseries','plot_lsm_timeseries','plot_pricing_diagnostics',
        'plot_strategy_performance','plot_factor_correlation','plot_factor_ic_comparison']
    for name in names:
        monkeypatch.setattr(regenerate_plots,name,lambda name=name:calls.append(name))
    regenerate_plots.main(['--pricing-only'])
    assert calls==names[:4]
