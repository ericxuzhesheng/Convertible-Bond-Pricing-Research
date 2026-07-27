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
