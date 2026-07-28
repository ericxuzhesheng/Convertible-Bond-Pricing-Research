from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CORE = _load_module(
    "mispricing_factor_core_contract",
    REPO_ROOT / "mispricing factor" / "mispricing_factor_core.py",
)
BENCHMARK = _load_module(
    "update_benchmark_contract",
    REPO_ROOT / "long-short strategy" / "update_benchmark.py",
)


def test_portfolio_return_does_not_drop_missing_held_bond() -> None:
    backtest = object.__new__(CORE.MultiFactorBacktest)
    dates = pd.to_datetime(["2024-01-05", "2024-01-12"])
    backtest.prices = pd.DataFrame(
        {
            "A": [100.0, 110.0],
            "B": [100.0, np.nan],
        },
        index=dates,
    )

    result = backtest.calculate_portfolio_return(
        dates[0], dates[1], ["A", "B"]
    )

    assert pd.isna(result)


def test_empty_portfolio_is_missing_not_zero_return() -> None:
    backtest = object.__new__(CORE.MultiFactorBacktest)
    dates = pd.to_datetime(["2024-01-05", "2024-01-12"])
    backtest.prices = pd.DataFrame({"A": [100.0, 110.0]}, index=dates)

    result = backtest.calculate_portfolio_return(
        dates[0], dates[1], []
    )

    assert pd.isna(result)


def test_benchmark_return_requires_exact_rebalance_dates() -> None:
    backtest = object.__new__(CORE.MultiFactorBacktest)
    dates = pd.to_datetime(["2024-01-05", "2024-01-12"])
    backtest.benchmark_prices = pd.Series(
        [100.0],
        index=pd.to_datetime(["2024-01-05"]),
    )

    result = backtest.calculate_benchmark_return(dates[0], dates[1])

    assert pd.isna(result)


def test_weekly_benchmark_must_cover_latest_completed_market_date() -> None:
    market_dates = pd.to_datetime(
        ["2024-01-05", "2024-01-12", "2024-01-15"]
    )
    benchmark = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-05"]),
            "close": [400.0],
        }
    )

    try:
        BENCHMARK.validate_benchmark_freshness(
            benchmark=benchmark,
            market_dates=market_dates,
            as_of=pd.Timestamp("2024-01-16"),
        )
    except RuntimeError as exc:
        assert "2024-01-12" in str(exc)
    else:
        raise AssertionError("stale benchmark was accepted")
