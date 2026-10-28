"""Run on CUDA hosts; the ordinary CPU CI skips device checks."""
from dataclasses import replace
from pathlib import Path
import sys
import numpy as np
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backtest'))
from pricing_engine import Contract, price_mc


def gpu_ready():
    try:
        from numba import cuda
        return cuda.is_available()
    except Exception:
        return False


@pytest.mark.skipif(not gpu_ready(),reason='CUDA toolkit/device unavailable')
@pytest.mark.parametrize('method',['LSM','ZL'])
def test_cuda_matches_cpu_replay_for_contractual_path_cases(method):
    base=Contract(parity=100.,maturity=1.,redemption=110.,sigma=.3,spread=.02,
        coupon_times=np.array([.4]),coupon_amounts=np.array([2.]),
        accrual_starts=np.array([0.]),accrual_ends=np.array([1.]),accrual_rates=np.array([3.]))
    cases=[base,replace(base,conversion_start=.25,conversion_end=.9),
        replace(base,call_ratio=1.1,call_notice_days=0),
        replace(base,call_ratio=1.3,call_history=np.r_[np.ones(15,dtype=np.int8),np.zeros(15,dtype=np.int8)]),
        replace(base,put_start=.1,put_ratio=.9,put_redemption=103.,put_includes_accrued=True),
        replace(base,forced_call_time=.4),replace(base,forced_call_time=0.),
        replace(base,sigma=0.,parity=60.)]
    for c in cases:
        cpu=price_mc(c,method=method,paths=512,exercise_steps=48,seed=123,backend='cuda-reference')
        gpu=price_mc(c,method=method,paths=512,exercise_steps=48,seed=123,backend='cuda')
        np.testing.assert_allclose(gpu,cpu,rtol=2e-11,atol=2e-9)


def test_xoroshiro_replay_has_reproducible_separate_antithetic_pairs():
    from pricing_cuda import _draws
    a=_draws(32,200,42)
    np.testing.assert_array_equal(a,_draws(32,200,42))
    assert not np.array_equal(a[:8],a[8:])
    assert not np.array_equal(a,_draws(32,200,43))
