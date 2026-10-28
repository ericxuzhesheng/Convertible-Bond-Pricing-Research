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
        "LSM_mispricing_factor.py",
        "strategy_data.py",
        "weekly_value_strategy.py",
        "regenerate_plots.py",
        "update_research_readme.py",
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

    assert 'rebuild_research_outputs.py" --full-history --refresh-inputs --promote' in source
    assert source.count("goto :fail") >= 3


def test_weekly_batch_runs_only_after_the_verified_incremental_cutoff() -> None:
    source = (BACKTEST_DIR / "weekly_update.bat").read_text(
        encoding="utf-8"
    )

    assert 'update_pricing_volatility.py" --print-bounds' in source
    assert '--start "%PIPELINE_START%"' in source
    assert '--incremental-after "%MODEL_CUTOFF%"' in source
    assert "--backend cpu --weekly --offline-inputs" in source


def test_bs_incremental_volatility_write_preserves_history():
    source=(BACKTEST_DIR/"pricing_inputs.py").read_text(encoding="utf-8")
    assert "bs_volatility_cache.csv" in source
    assert "to_csv(" not in source.split("def fingerprint")[0]


def test_zl_weekly_increment_never_reprices_verified_dates():
    source=(BACKTEST_DIR/"pricing_backtest.py").read_text(encoding="utf-8")
    assert "dates = dates[dates > cutoff]" in source
    assert "data.fingerprint(cutoff,params)" in source


def test_zl_weekly_history_read_failure_is_fail_closed():
    source=(BACKTEST_DIR/"pricing_backtest.py").read_text(encoding="utf-8")
    assert "validate_outputs(model,output)" in source
    assert "refusing full rebuild" in source


def test_lsm_weekly_increment_never_reprices_verified_dates():
    source=(BACKTEST_DIR/"pricing_backtest.py").read_text(encoding="utf-8")
    assert "dates = dates[dates > cutoff]" in source
    assert '"--initialize-history"' in source


def test_full_history_rebuild_is_cache_only_and_explicit(monkeypatch):
    calls=[]
    monkeypatch.setattr(full_history_rebuild.subprocess,"run",lambda command,**kwargs:calls.append((command,kwargs)))
    full_history_rebuild.run_full_rebuild()
    assert len(calls)==1
    assert Path(calls[0][0][1]).name=="reprice_history.py"
    assert calls[0][0][2:]==["--resume","--promote"]
    assert calls[0][1]["check"] is True



def test_full_history_rebuild_does_not_download_inputs_or_run_reports(monkeypatch):
    calls=[]
    monkeypatch.setattr(full_history_rebuild.subprocess,"run",lambda command,**kwargs:calls.append(command))
    full_history_rebuild.run_full_rebuild()
    assert all("data_pipeline.py" not in str(c) and "rebuild_research_outputs.py" not in str(c) for c in calls)



def test_full_weekly_rebuild_validates_every_completed_week():
    assert "source_validation_dates" in DATA_PIPELINE_SOURCE
    source=(BACKTEST_DIR/"pricing_backtest.py").read_text(encoding="utf-8")
    assert "model_price=model_frame,dates=dates" in source
