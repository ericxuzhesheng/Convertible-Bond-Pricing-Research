import math
from pathlib import Path
import sys

import numpy as np
import pytest
from scipy.special import ndtr

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'experiments/ccb_2023'))
from ccb import price_ccb, survival_moments
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'backtest'))
from pricing_engine import Contract
from curve_pde import price_curve_ccb
from dataclasses import replace


def test_no_call_equals_european_with_credit_adjusted_strike():
    s, sigma, r, spread, t, redemption = 100., .3, math.log1p(.02), .03, 3., 110.
    k = redemption*np.exp(-spread*t)
    d1 = (np.log(s/k)+(r+sigma*sigma/2)*t)/(sigma*np.sqrt(t))
    expected = s*ndtr(d1)+k*np.exp(-r*t)*ndtr(-d1+sigma*np.sqrt(t))
    expected += 2*np.exp(-(r+spread))
    assert price_ccb(parity=s, sigma=sigma, maturity=t, redemption=redemption,
                     protection=t, spread=spread, coupon_times=[1], coupon_amounts=[2]) == pytest.approx(expected)


def test_immediate_call_and_coupon_stop():
    assert price_ccb(parity=140, sigma=.3, maturity=3, redemption=110, protection=0,
                     coupon_times=[1, 2], coupon_amounts=[2, 2]) == 140


def test_deterministic_protection_end_coupon_and_no_double_final_coupon():
    assert price_ccb(parity=140, sigma=0, maturity=3, redemption=110, protection=1,
                     rate=0, coupon_times=[1, 2], coupon_amounts=[2, 2]) == 142
    assert price_ccb(parity=80, sigma=0, maturity=3, redemption=110, protection=1,
                     rate=0, coupon_times=[1, 2], coupon_amounts=[2, 2]) == 114


@pytest.mark.parametrize('parity', [40., 100., 129.9, 130., 160., 300.])
@pytest.mark.parametrize('protection', [1/12, .5])
def test_quadrature_convergence_and_bounds(parity, protection):
    args = dict(parity=parity, sigma=.5, maturity=6., redemption=108., spread=.03,
                protection=protection, coupon_times=np.arange(1., 6.), coupon_amounts=np.ones(5))
    a, b = price_ccb(**args, order=96), price_ccb(**args, order=192)
    assert abs(a-b) < 1e-6
    assert parity <= b <= price_ccb(**{**args, 'protection':6.}, order=192)+1e-8


def test_survival_decreases_with_time():
    p, m = survival_moments(100., .3, .02, 1/12, np.array([.1, .5, 1., 3.]), 130.)
    assert np.all(np.diff(p) <= 0)
    assert np.all((p >= 0) & (p <= 1))
    assert np.all(m <= 130*p+1e-10)


@pytest.mark.parametrize('parity', [60.,100.,140.])
@pytest.mark.parametrize('protection', [1/12,.5])
def test_pde_matches_report_constant_rate_solution(parity,protection):
    c=Contract(parity=parity,sigma=.5,maturity=6.,redemption=108.,spread=.03,
        coupon_times=np.arange(1.,6.),coupon_amounts=np.ones(5),curve_rates=np.full(2,np.log1p(.02)))
    a=price_ccb(parity=parity,sigma=.5,maturity=6,redemption=108,spread=.03,
                coupon_times=c.coupon_times,coupon_amounts=c.coupon_amounts,protection=protection)
    assert abs(price_curve_ccb(c,protection=protection,barrier=130.)-a)<.02


def test_actual_conversion_end_and_full_curve_without_call():
    c=Contract(parity=100.,sigma=.3,maturity=3.,redemption=110.,spread=.02,conversion_end=2.9,
        coupon_times=np.array([1.,2.]),coupon_amounts=np.array([2.,3.]),
        curve_tenors=np.array([1.,3.,5.]),curve_rates=np.array([.01,.03,.04]))
    t=c.conversion_end;discount=float(c.discount(t))
    k=c.redemption*float(c.discount(c.maturity))*np.exp(-c.spread*c.maturity)/discount
    sd=c.sigma*np.sqrt(t);d1=(np.log(c.parity/k/discount)+.5*sd*sd)/sd
    expected=c.parity*ndtr(d1)+k*discount*ndtr(-d1+sd)
    expected+=np.sum(c.coupon_amounts*c.discount(c.coupon_times)*np.exp(-c.spread*c.coupon_times))
    assert price_curve_ccb(c,protection=3,barrier=130)==pytest.approx(expected)


def test_coupon_coincident_with_grid_node_does_not_create_zero_half_step():
    # 123043.SZ at 2020-11-06: these nodes differ by 4.44e-16 years.
    c=Contract(parity=100.,sigma=.4914751008893093,maturity=5.326027397260274,
        redemption=110.,spread=.03,coupon_times=np.array([3.328767123287671]),
        coupon_amounts=np.array([2.]),curve_rates=np.full(2,np.log1p(.02)))
    a=price_curve_ccb(c,protection=1/12,barrier=130.)
    b=price_ccb(parity=c.parity,sigma=c.sigma,maturity=c.maturity,redemption=c.redemption,
        spread=c.spread,coupon_times=c.coupon_times,coupon_amounts=c.coupon_amounts)
    assert np.isfinite(a) and abs(a-b)<.02


def test_full_curve_shape_changes_callable_price_even_at_same_terminal_yield():
    c=Contract(parity=120.,sigma=.3,maturity=3.,redemption=110.,spread=.02,
        curve_tenors=np.array([.1,1.,3.]),curve_rates=np.array([.01,.01,.03]))
    other=replace(c,curve_rates=np.array([.04,.04,.03]))
    assert float(c.discount(3))==float(other.discount(3))
    assert abs(price_curve_ccb(c,protection=1/12,barrier=130)-price_curve_ccb(other,protection=1/12,barrier=130))>.01


def test_comparison_excludes_missing_model_and_rejects_source_mix(tmp_path):
    import pandas as pd
    from run_history import compare
    rows=[dict(date='2020-01-03',code=str(i),market=100.+i,parity=100.,floor_source='production Tushare',
          CCB=101.+i,BS=102.+i,ZL=100.+i,LSM=101.+i) for i in range(3)]
    rows[2]['LSM']=np.nan
    common,summary=compare(pd.DataFrame(rows),tmp_path)
    assert len(common)==2 and summary.n.eq(2).all()
    rows[0]['floor_source']='independent source'
    with pytest.raises(ValueError,match='source supplement'):
        compare(pd.DataFrame(rows),tmp_path)
