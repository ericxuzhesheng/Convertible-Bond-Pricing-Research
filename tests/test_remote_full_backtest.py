from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

import remote_gpu_rebuild  # noqa: E402


def test_remote_gpu_stage_stops_before_mutation_without_cuda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        remote_gpu_rebuild.subprocess,
        "run",
        lambda command, **kwargs: calls.append(command),
    )

    with pytest.raises(RuntimeError, match="CUDA"):
        remote_gpu_rebuild.run_remote_gpu_stage(gpu_available=False)

    assert calls == []


def test_remote_gpu_stage_runs_zl_then_downstream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def fake_run(command, **kwargs):
        calls.append(Path(command[1]).name)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(remote_gpu_rebuild.subprocess, "run", fake_run)
    monkeypatch.setattr(
        remote_gpu_rebuild,
        "validate_zl_coverage",
        lambda **kwargs: 1.0,
    )
    monkeypatch.setattr(
        remote_gpu_rebuild,
        "build_results_archive",
        lambda **kwargs: tmp_path / "results.zip",
    )

    remote_gpu_rebuild.run_remote_gpu_stage(
        gpu_available=True,
        results_repo=None,
    )

    assert calls == [
        "Z-L_backtest_GPU_prod.py",
        "rebuild_research_outputs.py",
    ]


def test_validate_zl_coverage_rejects_incomplete_rebuild(
    tmp_path: Path,
) -> None:
    import pandas as pd

    market = pd.DataFrame({"A": [100.0, 101.0], "B": [99.0, 98.0]})
    model = pd.DataFrame({"A": [110.0, None], "B": [None, None]})
    market_path = tmp_path / "market.csv"
    model_path = tmp_path / "model.csv"
    market.to_csv(market_path)
    model.to_csv(model_path)

    with pytest.raises(RuntimeError, match="coverage"):
        remote_gpu_rebuild.validate_zl_coverage(
            market_path=market_path,
            model_path=model_path,
            minimum=0.90,
        )


def test_cpu_workflow_rebuilds_bs_and_benchmark_before_publish() -> None:
    workflow = (
        REPO_ROOT / ".github" / "workflows" / "full-backtest-cpu.yml"
    ).read_text(encoding="utf-8")

    assert "B-S_backtest.py --rebuild-all" in workflow
    assert "update_benchmark.py" in workflow
    assert "python -m pytest -q" in workflow
    assert "actions/upload-artifact" in workflow
    assert "git push origin" in workflow
