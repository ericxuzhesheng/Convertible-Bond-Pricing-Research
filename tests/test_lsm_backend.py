from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest


BACKTEST_DIR = Path(__file__).resolve().parents[1] / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

from lsm_backend import (  # noqa: E402
    price_early_conversion_premium_batch,
    price_lsm_enhanced_zl_batch,
)


def _params(*, spread: float = 0.0, sigma: float = 0.0) -> dict:
    return {
        "S0": np.array([100.0]),
        "r": np.array([0.02]),
        "cs": np.array([spread]),
        "sigma": np.array([sigma]),
        "T": np.array([1.0]),
        "maturity_redem": np.array([110.0]),
    }


def test_lsm_is_deterministic_for_fixed_seed() -> None:
    params = _params(spread=0.03, sigma=0.25)

    first = price_early_conversion_premium_batch(
        params, paths=64, exercise_steps=12, seed=7
    )
    second = price_early_conversion_premium_batch(
        params, paths=64, exercise_steps=12, seed=7
    )

    np.testing.assert_array_equal(first, second)


def test_lsm_enhanced_value_respects_the_zl_floor() -> None:
    params = _params(spread=0.08, sigma=0.30)
    base = np.array([105.0])

    premium = price_early_conversion_premium_batch(
        params, paths=128, exercise_steps=16, seed=11
    )
    enhanced = price_lsm_enhanced_zl_batch(
        base, params, paths=128, exercise_steps=16, seed=11
    )

    assert premium[0] >= 0.0
    assert enhanced[0] >= base[0]


def test_lsm_rejects_invalid_path_count() -> None:
    with pytest.raises(ValueError, match="even integer"):
        price_early_conversion_premium_batch(_params(), paths=15)


def test_lsm_handles_empty_batch() -> None:
    params = {field: np.array([]) for field in _params()}

    actual = price_early_conversion_premium_batch(params)

    assert actual.shape == (0,)
