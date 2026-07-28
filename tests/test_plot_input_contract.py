from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

from regenerate_plots import (  # noqa: E402
    build_matched_plot_series,
    build_reliable_weekly_plot_series,
)


def test_plot_series_uses_identical_finite_bond_cells() -> None:
    dates = pd.DatetimeIndex(["2024-01-05", "2024-01-12"])
    model = pd.DataFrame(
        [[110.0, np.nan], [120.0, 130.0]],
        index=dates,
        columns=["A", "B"],
    )
    market = pd.DataFrame(
        [[100.0, 500.0], [100.0, 100.0]],
        index=dates,
        columns=["A", "B"],
    )
    relative_deviation = pd.DataFrame(
        [[0.10, np.inf], [0.20, 0.30]],
        index=dates,
        columns=["A", "B"],
    )

    series = build_matched_plot_series(
        model=model,
        market=market,
        relative_deviation=relative_deviation,
    )

    assert series.loc[dates[0], "model"] == 110.0
    assert series.loc[dates[0], "market"] == 100.0
    assert series.loc[dates[0], "relative_deviation_pct"] == 10.0
    assert series.loc[dates[0], "sample_count"] == 1
    assert series.loc[dates[1], "sample_count"] == 2


def test_weekly_plot_uses_last_valid_observation_not_empty_week_end() -> None:
    dates = pd.DatetimeIndex(
        ["2024-01-09", "2024-01-11", "2024-01-12"]
    )
    daily = pd.DataFrame(
        {
            "model": [110.0, 111.0, np.nan],
            "market": [100.0, 101.0, np.nan],
            "relative_deviation_pct": [10.0, 9.9, np.nan],
            "sample_count": [30, 31, 0],
        },
        index=dates,
    )

    weekly = build_reliable_weekly_plot_series(
        daily,
        min_sample_count=20,
    )

    assert weekly.index.tolist() == [pd.Timestamp("2024-01-11")]
    assert weekly.iloc[0]["sample_count"] == 31


def test_weekly_plot_marks_tiny_cross_section_as_missing() -> None:
    daily = pd.DataFrame(
        {
            "model": [150.0],
            "market": [100.0],
            "relative_deviation_pct": [50.0],
            "sample_count": [6],
        },
        index=pd.DatetimeIndex(["2024-01-12"]),
    )

    weekly = build_reliable_weekly_plot_series(
        daily,
        min_sample_count=20,
    )

    assert weekly[["model", "market", "relative_deviation_pct"]].isna().all().all()
    assert weekly.iloc[0]["sample_count"] == 6


def test_weekly_plot_preserves_fully_missing_week_as_a_gap() -> None:
    daily = pd.DataFrame(
        {
            "model": [110.0, np.nan],
            "market": [100.0, np.nan],
            "relative_deviation_pct": [10.0, np.nan],
            "sample_count": [30, 0],
        },
        index=pd.DatetimeIndex(["2024-01-12", "2024-01-19"]),
    )

    weekly = build_reliable_weekly_plot_series(
        daily,
        min_sample_count=20,
    )

    assert weekly.index.tolist() == [
        pd.Timestamp("2024-01-12"),
        pd.Timestamp("2024-01-19"),
    ]
    assert pd.isna(weekly.loc[pd.Timestamp("2024-01-19"), "model"])


def test_weekly_plot_inserts_absent_calendar_week_as_a_gap() -> None:
    daily = pd.DataFrame(
        {
            "model": [110.0, 112.0],
            "market": [100.0, 102.0],
            "relative_deviation_pct": [10.0, 9.8],
            "sample_count": [30, 32],
        },
        index=pd.DatetimeIndex(["2024-01-12", "2024-01-26"]),
    )

    weekly = build_reliable_weekly_plot_series(
        daily,
        min_sample_count=20,
    )

    assert weekly.index.tolist() == [
        pd.Timestamp("2024-01-12"),
        pd.Timestamp("2024-01-19"),
        pd.Timestamp("2024-01-26"),
    ]
    assert weekly.loc[pd.Timestamp("2024-01-19")].isna().all()
