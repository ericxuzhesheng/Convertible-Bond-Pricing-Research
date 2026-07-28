from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

import daily_signal  # noqa: E402


def test_daily_pipeline_failure_stops_before_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        daily_signal.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="data source failed",
        ),
    )

    with pytest.raises(RuntimeError, match="data_pipeline"):
        daily_signal.run_pipeline()


def test_zl_gpu_failure_does_not_silently_fall_back_to_cpu(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_run(command, **kwargs):
        calls.append(Path(command[1]).name)
        if Path(command[1]).name == "B-S_backtest.py":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="CUDA unavailable",
        )

    monkeypatch.setattr(daily_signal.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="Z-L"):
        daily_signal.run_models()

    assert calls == ["B-S_backtest.py", "Z-L_backtest_GPU_prod.py"]


def test_weekly_model_run_passes_weekly_flag_to_both_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(daily_signal.subprocess, "run", fake_run)

    daily_signal.run_models(weekly_only=True)

    assert calls[0][-1] == "--weekly"
    assert calls[1][-1] == "--weekly"


def test_signal_date_requires_same_fresh_market_and_model_date() -> None:
    dates = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
    market = pd.DataFrame({"A": [100.0, 101.0]}, index=dates)
    bs = pd.DataFrame({"A": [0.1, 0.2]}, index=dates)
    zl = pd.DataFrame({"A": [0.1, np.nan]}, index=dates)

    with pytest.raises(RuntimeError, match="dates disagree"):
        daily_signal.select_signal_date(
            bs_deviation=bs,
            zl_deviation=zl,
            market_price=market,
            now=pd.Timestamp("2024-01-04"),
        )


def test_signal_score_requires_both_observed_model_outputs() -> None:
    candidates = pd.DataFrame(
        {
            "ts_code": ["A", "B"],
            "bs_dev": [0.2, 0.3],
            "zl_dev": [0.1, np.nan],
        }
    )

    scored = daily_signal.combine_observed_model_scores(candidates)

    assert scored["ts_code"].tolist() == ["A"]
    assert scored.iloc[0]["score"] == pytest.approx(0.15)
