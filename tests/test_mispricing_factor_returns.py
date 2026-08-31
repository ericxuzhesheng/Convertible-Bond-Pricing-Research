from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_PATH = REPO_ROOT / "mispricing factor" / "mispricing_factor_core.py"
SPEC = importlib.util.spec_from_file_location("mispricing_factor_core", CORE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_delisted_holding_exits_at_last_observed_price() -> None:
    dates = pd.to_datetime(["2019-02-22", "2019-03-25", "2019-03-29"])
    backtest = MODULE.MultiFactorBacktest(model="BS")
    backtest.prices = pd.DataFrame(
        {"110032.SH": [141.76, 176.09, np.nan]},
        index=dates,
    )
    backtest.delist_dates = pd.Series(
        {"110032.SH": pd.Timestamp("2019-03-26")}
    )

    result = backtest.calculate_portfolio_return(
        dates[0],
        dates[2],
        ["110032.SH"],
    )

    assert result == (176.09 - 141.76) / 141.76


def test_suspended_holding_is_marked_to_last_observed_price() -> None:
    dates = pd.to_datetime(["2024-01-31", "2024-02-28"])
    backtest = MODULE.MultiFactorBacktest(model="BS")
    backtest.prices = pd.DataFrame(
        {"123001.SZ": [110.0, np.nan]},
        index=dates,
    )
    backtest.observed_daily_prices = pd.DataFrame(
        {"123001.SZ": [110.0]},
        index=dates[:1],
    )
    backtest.delist_dates = pd.Series(dtype="datetime64[ns]")

    result = backtest.calculate_portfolio_return(
        dates[0],
        dates[1],
        ["123001.SZ"],
    )

    assert result == 0.0


def test_delisted_holding_uses_daily_exit_between_weekly_dates() -> None:
    weekly_dates = pd.to_datetime(["2019-07-26", "2019-08-30"])
    daily_dates = pd.to_datetime(
        ["2019-07-26", "2019-07-30", "2019-07-31"]
    )
    backtest = MODULE.MultiFactorBacktest(model="BS")
    backtest.prices = pd.DataFrame(
        {"110040.SH": [152.68, np.nan]},
        index=weekly_dates,
    )
    backtest.observed_daily_prices = pd.DataFrame(
        {"110040.SH": [152.68, 166.78, 167.56]},
        index=daily_dates,
    )
    backtest.delist_dates = pd.Series(
        {"110040.SH": pd.Timestamp("2019-08-01")}
    )

    result = backtest.calculate_portfolio_return(
        weekly_dates[0],
        weekly_dates[1],
        ["110040.SH"],
    )

    assert result == (167.56 - 152.68) / 152.68


def test_factor_ic_uses_next_period_return_and_direction_adjustment() -> None:
    codes = [f"12{i:04d}.SZ" for i in range(12)]
    dates = pd.to_datetime(["2024-01-31", "2024-02-29"])
    expected_returns = pd.Series(
        np.linspace(-0.06, 0.08, len(codes)),
        index=codes,
    )
    prices = pd.DataFrame(
        [np.full(len(codes), 100.0), 100.0 * (1 + expected_returns)],
        index=dates,
        columns=codes,
    )
    increasing = pd.DataFrame(
        [np.arange(len(codes)), np.arange(len(codes))],
        index=dates,
        columns=codes,
    )

    backtest = MODULE.MultiFactorBacktest(model="BS")
    backtest.prices = prices
    backtest.observed_daily_prices = prices
    backtest.normalized_factors = {
        "momentum": increasing,
        "valuation": -increasing,
    }

    records = backtest.calculate_factor_ic_period(
        dates[0],
        dates[1],
        codes,
    )
    by_factor = {record["factor"]: record for record in records}

    assert by_factor["momentum"]["n_obs"] == len(codes)
    assert by_factor["momentum"]["ic"] > 0.99
    assert by_factor["momentum"]["rank_ic"] > 0.99
    assert by_factor["valuation"]["ic"] > 0.99
    assert by_factor["valuation"]["rank_ic"] > 0.99


def test_factor_correlation_exports_pearson_and_spearman(tmp_path) -> None:
    codes = [f"12{i:04d}.SZ" for i in range(12)]
    dates = pd.to_datetime(["2024-01-31", "2024-02-29"])
    base = pd.DataFrame(
        [np.arange(len(codes)), np.arange(len(codes)) + 1],
        index=dates,
        columns=codes,
        dtype=float,
    )
    backtest = MODULE.MultiFactorBacktest(data_dir=tmp_path, model="BS")
    backtest.factors = {
        "liquidity": base,
        "momentum": base**2,
        "valuation": -base,
    }

    matrices = backtest.check_factor_correlation()

    assert set(matrices) == {"pearson", "spearman"}
    assert matrices["pearson"].columns.tolist() == [
        "liquidity",
        "valuation",
        "momentum",
    ]
    assert (tmp_path / "BS_factor_correlation_pearson.csv").exists()
    assert (tmp_path / "BS_factor_correlation_spearman.csv").exists()
    assert (tmp_path / "BS_factor_correlation.png").exists()
