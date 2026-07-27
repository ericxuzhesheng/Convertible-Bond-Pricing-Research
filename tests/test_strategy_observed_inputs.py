from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
STRATEGY_PATH = (
    REPO_ROOT / "long-short strategy" / "B-S_Z-L_strategy.py"
)
SPEC = importlib.util.spec_from_file_location("cb_strategy", STRATEGY_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_strategy_alignment_does_not_invent_missing_prices_or_turnover() -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    matrix = pd.DataFrame(
        {"123001.SZ": [1.0, np.nan, 3.0]},
        index=dates,
    )

    aligned = MODULE.align_observed_strategy_inputs(
        common_index=dates,
        ratings=matrix,
        remaining_term=matrix,
        balance=matrix,
        turnover=matrix,
        prices=matrix,
    )

    for frame in aligned.values():
        assert pd.isna(frame.iloc[1, 0])


def test_strategy_liquidity_and_balance_thresholds_use_wan_units() -> None:
    date = pd.Timestamp("2024-02-29")
    code = "123001.SZ"
    strategy = MODULE.CBStrategy()
    strategy.ratings = pd.DataFrame({code: [4]}, index=[date])
    strategy.remaining_term = pd.DataFrame({code: [2.0]}, index=[date])
    strategy.balance = pd.DataFrame({code: [1_000.0]}, index=[date])
    strategy.turnover = pd.DataFrame({code: [100.0]}, index=[date])
    strategy.relative_deviation = pd.DataFrame(
        {code: [0.1]}, index=[date]
    )
    strategy.prices = pd.DataFrame({code: [110.0]}, index=[date])
    strategy.listing_dates = pd.Series(
        {code: date - pd.Timedelta(days=60)}
    )

    assert strategy.get_first_layer_universe(date).empty

    strategy.balance.loc[date, code] = 4_000.0
    strategy.turnover.loc[date, code] = 600.0
    assert strategy.get_first_layer_universe(date).tolist() == [code]


def test_strategy_has_no_external_legacy_data_fallback() -> None:
    source = STRATEGY_PATH.read_text(encoding="utf-8")
    assert "LEGACY_DIR" not in source
    assert "假设无风险利率为0" not in source
    assert "observed_average_risk_free_rate" in source
