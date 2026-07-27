from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

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
