from pathlib import Path
import sys
import numpy as np
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"backtest"))
from pricing_engine import Contract
from lsm_backend import price_lsm_batch


def test_independent_lsm_uses_no_base_price_and_is_reproducible():
    c=Contract(parity=100.,maturity=1.,redemption=110.,sigma=.2,spread=.01)
    a=price_lsm_batch([c],paths=64,seed=7)
    np.testing.assert_array_equal(a,price_lsm_batch([c],paths=64,seed=7))
    assert a[0]>=100.


def test_lsm_handles_empty_batch():
    assert price_lsm_batch([]).shape==(0,)


def test_lsm_rejects_invalid_path_count():
    c=Contract(parity=100.,maturity=1.,redemption=110.,sigma=.2,spread=.01)
    with pytest.raises(ValueError): price_lsm_batch([c],paths=15)
