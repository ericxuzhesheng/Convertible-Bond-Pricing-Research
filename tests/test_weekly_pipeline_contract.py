from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

import rebuild_research_outputs  # noqa: E402


def test_research_output_rebuild_runs_all_downstream_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_run(command, **kwargs):
        calls.append(Path(command[1]).name)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        rebuild_research_outputs.subprocess, "run", fake_run
    )
    rebuild_research_outputs.run_checked_steps()

    assert calls == [
        "build_observed_factors.py",
        "B-S_mispricing_factor.py",
        "Z-L_mispricing_factor.py",
        "B-S_Z-L_strategy.py",
        "regenerate_plots.py",
    ]


def test_research_output_rebuild_stops_on_first_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_run(command, **kwargs):
        calls.append(Path(command[1]).name)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(
        rebuild_research_outputs.subprocess, "run", fake_run
    )

    with pytest.raises(RuntimeError, match="build_observed_factors"):
        rebuild_research_outputs.run_checked_steps()

    assert calls == ["build_observed_factors.py"]


def test_weekly_batch_fails_closed_before_git_publish() -> None:
    source = (BACKTEST_DIR / "weekly_update.bat").read_text(
        encoding="utf-8"
    )

    assert "rebuild_research_outputs.py" in source
    assert source.count("goto :fail") >= 3
