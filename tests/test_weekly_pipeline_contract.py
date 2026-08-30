from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
DATA_PIPELINE_SOURCE = (
    BACKTEST_DIR / "data_pipeline.py"
).read_text(encoding="utf-8")
BS_SOURCE = (BACKTEST_DIR / "B-S_backtest.py").read_text(encoding="utf-8")
sys.path.insert(0, str(BACKTEST_DIR))

import rebuild_research_outputs  # noqa: E402
import full_history_rebuild  # noqa: E402


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


def test_weekly_batch_runs_only_after_the_verified_incremental_cutoff() -> None:
    source = (BACKTEST_DIR / "weekly_update.bat").read_text(
        encoding="utf-8"
    )

    assert "ZL_Model_Manifest.json" in source
    assert '--start "%PIPELINE_START%"' in source
    assert '--incremental-after "%MODEL_CUTOFF%"' in source
    assert "--backend cuda --weekly --offline-inputs" in source


def test_bs_incremental_volatility_write_preserves_history() -> None:
    source = (BACKTEST_DIR / "B-S_backtest.py").read_text(encoding="utf-8")

    assert "existing_volatility_history" in source
    assert "merge_incremental_history(" in source


def test_zl_weekly_increment_never_reprices_verified_dates() -> None:
    source = (BACKTEST_DIR / "Z-L_backtest_GPU_prod.py").read_text(
        encoding="utf-8"
    )

    assert "refresh_dates > verified_cutoff" in source
    assert "checkpoint_cutoff=verified_cutoff" in source
    assert "stable_input_fingerprint" in source


def test_zl_weekly_history_read_failure_is_fail_closed() -> None:
    source = (BACKTEST_DIR / "Z-L_backtest_GPU_prod.py").read_text(
        encoding="utf-8"
    )

    assert "weekly incremental ZL cannot read its verified history" in source
    assert "refusing to continue or overwrite published results" in source


def test_full_history_rebuild_checks_gpu_before_any_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        full_history_rebuild.subprocess,
        "run",
        lambda command, **kwargs: calls.append(command),
    )

    with pytest.raises(RuntimeError, match="CUDA"):
        full_history_rebuild.run_full_rebuild(gpu_available=False)

    assert calls == []


def test_full_history_rebuild_uses_real_data_rebuild_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(full_history_rebuild.subprocess, "run", fake_run)
    full_history_rebuild.run_full_rebuild(gpu_available=True)

    names = [Path(command[1]).name for command in calls]
    assert names == [
        "data_pipeline.py",
        "B-S_backtest.py",
        "Z-L_backtest_GPU_prod.py",
        "update_benchmark.py",
        "rebuild_research_outputs.py",
    ]
    assert "--rebuild-all" in calls[0]
    assert "--rebuild-all" in calls[1]
    assert "--rebuild-all" in calls[2]
    assert "--weekly" in calls[1]
    assert "--weekly" in calls[2]
    assert "--refresh-input-cache" in calls[1]
    assert "--refresh-input-cache" in calls[2]
    assert "--weekly" in calls[0]


def test_full_weekly_rebuild_validates_every_completed_week() -> None:
    assert "source_validation_dates" in DATA_PIPELINE_SOURCE
    assert "contract_validation_dates" in BS_SOURCE
