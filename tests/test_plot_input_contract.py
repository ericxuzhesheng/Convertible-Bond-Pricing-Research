from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

from regenerate_plots import build_matched_plot_series  # noqa: E402


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
