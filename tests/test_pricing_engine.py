from dataclasses import replace
import sys
from pathlib import Path
import numpy as np
import pytest
from scipy.special import ndtr

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"backtest"))
from pricing_engine import Contract, price_bs, price_mc
import pricing_engine


def contract(**kwargs):
    return Contract(parity=80., maturity=1., redemption=110., sigma=0., spread=0.,
                    curve_rates=np.zeros(2), **kwargs)


def test_european_uses_actual_redemption_and_matches_bs_without_credit():
    c = replace(contract(), parity=100., sigma=.2)
    d1 = (np.log(100/110)+.5*.2**2)/.2
    expected = 110+100*ndtr(d1)-110*ndtr(d1-.2)
    assert price_bs(c) == pytest.approx(expected)


@pytest.mark.parametrize("method", ["ZL", "LSM"])
def test_cash_coupons_are_paid_only_while_alive_and_maturity_coupon_not_doubled(method):
    c = contract(coupon_times=np.array([.5]), coupon_amounts=np.array([5.]))
    value, error = price_mc(c, method=method, paths=64, seed=1)
    assert value == pytest.approx(115.)
    assert error == pytest.approx(0.)
    assert price_bs(c) == pytest.approx(115.)


@pytest.mark.parametrize("method", ["ZL", "LSM"])
def test_forced_call_terminates_option_and_future_coupons(method):
    c = replace(contract(), parity=150., sigma=.6, forced_call_time=0.,
                coupon_times=np.array([.5]), coupon_amounts=np.array([20.]))
    value, error = price_mc(c, method=method, paths=128, seed=7)
    assert value == pytest.approx(150.)
    assert error == 0.


def test_time_zero_conversion_and_equity_do_not_receive_cash_credit_discount():
    c = replace(contract(), parity=150., spread=.1)
    assert price_bs(c) == pytest.approx(150.)
    assert price_mc(c, method="LSM", paths=64)[0] == pytest.approx(150.)


def test_conversion_lock_is_observed():
    c = replace(contract(), parity=150., redemption=1., dividend_yield=1., conversion_start=.5)
    assert price_mc(c, method="LSM", paths=64, exercise_steps=48)[0] == pytest.approx(150*np.exp(-.5), rel=1e-10)


@pytest.mark.parametrize('method',['ZL','LSM'])
def test_last_conversion_day_is_observed_before_maturity_cash_payment(method):
    c=replace(contract(),parity=120.,conversion_start=.5,conversion_end=364/365)
    assert price_mc(c,method=method,paths=64)[0]==pytest.approx(120.)
    c=replace(c,parity=105.,coupon_times=np.array([.25]),coupon_amounts=np.array([5.]))
    assert price_mc(c,method=method,paths=64)[0]==pytest.approx(115.)


@pytest.mark.parametrize("method", ["ZL", "LSM"])
def test_cash_flows_receive_spread_and_equity_does_not(method):
    c = replace(contract(coupon_times=np.array([.5]), coupon_amounts=np.array([5.])), spread=.1)
    expected = 5*np.exp(-.05)+110*np.exp(-.1)
    assert price_mc(c, method=method, paths=64)[0] == pytest.approx(expected)


def test_observed_call_window_can_terminate_at_time_zero():
    c = replace(contract(), parity=140., sigma=.5, call_ratio=1.3,
                call_history=np.array([1]*15+[0]*15), call_notice_days=0)
    assert price_mc(c, method="LSM", paths=64)[0] == pytest.approx(140.)


def test_held_out_lsm_is_reproducible_and_matches_no_call_european():
    c = replace(contract(), parity=100., sigma=.2)
    value, error = price_mc(c, method="LSM", paths=8192, seed=123)
    assert abs(value-price_bs(c)) <= 4*error+.05
    assert (value, error) == price_mc(c, method="LSM", paths=8192, seed=123)


def test_invalid_coupon_at_maturity_and_nan_inputs_are_rejected():
    with pytest.raises(ValueError):
        price_bs(contract(coupon_times=np.array([1.]), coupon_amounts=np.array([5.])))
    with pytest.raises(ValueError):
        price_bs(replace(contract(), sigma=np.nan))


@pytest.mark.parametrize('method',['ZL','LSM'])
def test_fixed_full_put_price_is_distinct_from_call_and_not_given_extra_interest(method):
    c=replace(contract(),redemption=90.,put_start=0.,put_ratio=.9,put_days=1,
        initial_put_count=1,put_redemption=103.,put_includes_accrued=True,
        accrual_starts=np.array([-.5]),accrual_ends=np.array([.5]),accrual_rates=np.array([8.]))
    assert price_mc(c,method=method,paths=64)[0]==pytest.approx(103.)
    assert price_mc(replace(c,forced_call_time=0.),method=method,paths=64)[0]==pytest.approx(104.)


def test_parallel_contract_streams_are_deterministic_and_isolated():
    from concurrent.futures import ThreadPoolExecutor
    c=replace(contract(),parity=100.,sigma=.2)
    def calculate(seed):return price_mc(c,method='LSM',paths=128,seed=seed)
    seeds=[11,21,11,31]
    serial=list(map(calculate,seeds))
    with ThreadPoolExecutor(max_workers=3) as pool:
        parallel=list(pool.map(calculate,seeds))
    assert parallel==serial and parallel[0]==parallel[2]


def test_reported_sampling_error_matches_repeated_european_valuations():
    c=Contract(parity=100.,maturity=1.,redemption=110.,sigma=.3,spread=.01)
    reference=price_bs(c)
    runs=np.array([price_mc(c,method='ZL',paths=1024,seed=seed) for seed in range(1,65)])
    standardized=(runs[:,0]-reference)/runs[:,1]
    assert .6<standardized.std(ddof=1)<1.5
    assert abs(standardized.mean())<.5


def test_training_does_not_depend_on_evaluation_path_survival(monkeypatch):
    def paths(*args):
        stock=np.zeros((16,3));stock[:,0]=120.;stock[:8,1]=150.
        states=np.zeros((16,3,3));alive=np.zeros((16,3),bool)
        alive[:,0]=True;alive[:8,1]=True
        cash=np.r_[np.full(8,110.),np.zeros(8)]
        equity=np.r_[np.zeros(8),np.full(8,135.)]
        return stock,states,alive,np.zeros((16,3),bool),cash,equity
    monkeypatch.setattr(pricing_engine,'_simulate',paths)
    c=replace(contract(),parity=120.,sigma=.3,call_ratio=1.3)
    assert price_mc(c,paths=8,exercise_steps=2)[0]==pytest.approx(135.)


def test_regression_error_cannot_trigger_conversion_below_guaranteed_cash(monkeypatch):
    def bad_fit(*args,**kwargs):return np.array([-1e9,0.,0.,0.,0.,0.]),None,None,None
    monkeypatch.setattr(np.linalg,'lstsq',bad_fit)
    assert price_mc(contract(),paths=64)[0]==pytest.approx(110.)
