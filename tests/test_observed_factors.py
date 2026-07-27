from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
FACTOR_DIR = REPO_ROOT / "mispricing factor"
sys.path.insert(0, str(BACKTEST_DIR))
sys.path.insert(0, str(FACTOR_DIR))

from build_observed_factors import build_observed_factors  # noqa: E402
from market_data_contracts import DataContractError  # noqa: E402
from mispricing_factor_core import MultiFactorBacktest  # noqa: E402


def _matrix(values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {"123001.SZ": values},
        index=pd.bdate_range("2024-01-02", periods=len(values)),
    )


def test_observed_factors_use_only_aligned_market_inputs() -> None:
    price = _matrix([100.0, 102.0, 101.0, 104.0])
    amount = _matrix([1.0, 2.0, 4.0, 8.0])
    conversion_value = _matrix([80.0, 81.0, 82.0, 83.0])
    bond_floor = _matrix([90.0, 90.0, 91.0, 92.0])

    factors = build_observed_factors(
        price=price,
        amount=amount,
        conversion_value=conversion_value,
        bond_floor=bond_floor,
        lookback=2,
        min_observations=2,
    )

    assert set(factors) == {
        "liquidity",
        "volatility",
        "price_volume",
        "valuation",
        "momentum",
    }
    expected_valuation = (
        (104.0 / 83.0 - 1.0) + (104.0 / 92.0 - 1.0)
    ) / 2.0
    assert factors["valuation"].iloc[-1, 0] == pytest.approx(
        expected_valuation
    )
    assert factors["momentum"].iloc[-1, 0] == pytest.approx(
        104.0 / 102.0 - 1.0
    )


def test_observed_factors_do_not_forward_fill_missing_prices() -> None:
    price = _matrix([100.0, 101.0, np.nan, 103.0])
    amount = _matrix([1.0, 2.0, 3.0, 4.0])
    conversion_value = _matrix([80.0, 81.0, 82.0, 83.0])
    bond_floor = _matrix([90.0, 90.0, 91.0, 92.0])

    factors = build_observed_factors(
        price=price,
        amount=amount,
        conversion_value=conversion_value,
        bond_floor=bond_floor,
        lookback=2,
        min_observations=2,
    )

    for factor in factors.values():
        assert pd.isna(factor.iloc[2, 0])


def test_observed_factors_reject_nonpositive_market_values() -> None:
    price = _matrix([100.0, 101.0])
    amount = _matrix([1.0, 2.0])
    conversion_value = _matrix([80.0, 0.0])
    bond_floor = _matrix([90.0, 91.0])

    with pytest.raises(DataContractError, match="conversion_value"):
        build_observed_factors(
            price=price,
            amount=amount,
            conversion_value=conversion_value,
            bond_floor=bond_floor,
            lookback=2,
            min_observations=2,
        )


def test_factor_preprocessing_preserves_missing_observations() -> None:
    dates = pd.bdate_range("2024-01-02", periods=2)
    columns = ["A", "B", "C"]
    backtest = MultiFactorBacktest()
    backtest.prices = pd.DataFrame(
        [[100.0, 101.0, 102.0], [100.0, 101.0, 102.0]],
        index=dates,
        columns=columns,
    )
    backtest.factors = {
        "liquidity": pd.DataFrame(
            [[1.0, 2.0, 3.0], [np.nan, 3.0, 4.0]],
            index=dates,
            columns=columns,
        )
    }

    backtest.preprocess_factors()

    normalized = backtest.normalized_factors["liquidity"]
    assert pd.isna(normalized.loc[dates[1], "A"])
    assert normalized.loc[dates[1], ["B", "C"]].notna().all()
