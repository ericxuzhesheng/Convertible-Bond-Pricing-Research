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
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
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
    monkeypatch.setattr(
        remote_gpu_rebuild,
        "validate_cpu_stage_handoff",
        lambda **kwargs: {},
    )

    remote_gpu_rebuild.run_remote_gpu_stage(
        gpu_available=True,
        results_repo=None,
    )

    assert Path(calls[0][1]).name == "Z-L_backtest_GPU_prod.py"
    assert calls[0][2:] == [
        "--rebuild-all",
        "--weekly",
    ]
    assert Path(calls[1][1]).name == "rebuild_research_outputs.py"


def test_gpu_stage_rejects_corrupt_cpu_handoff(tmp_path: Path) -> None:
    import hashlib
    import json

    source = tmp_path / "input.csv"
    source.write_text("original", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "workflow_run_id": "123",
                "workflow_source_sha": "abc",
                "branch": "test-branch",
                "files": {
                    "backtest/input.csv": hashlib.sha256(
                        source.read_bytes()
                    ).hexdigest()
                }
            }
        ),
        encoding="utf-8",
    )
    source.write_text("changed", encoding="utf-8")

    with pytest.raises(RuntimeError, match="hash mismatch"):
        remote_gpu_rebuild.validate_cpu_stage_handoff(
            manifest_path=manifest,
            required_files={"backtest/input.csv": source},
            expected_run_id="123",
            expected_source_sha="abc",
            expected_branch="test-branch",
        )


def test_gpu_stage_rejects_wrong_cpu_workflow_run(tmp_path: Path) -> None:
    import hashlib
    import json

    source = tmp_path / "input.csv"
    source.write_text("verified", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "workflow_run_id": "old-run",
                "workflow_source_sha": "abc",
                "branch": "test-branch",
                "files": {
                    "backtest/input.csv": hashlib.sha256(
                        source.read_bytes()
                    ).hexdigest()
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="workflow_run_id"):
        remote_gpu_rebuild.validate_cpu_stage_handoff(
            manifest_path=manifest,
            required_files={"backtest/input.csv": source},
            expected_run_id="new-run",
            expected_source_sha="abc",
            expected_branch="test-branch",
        )


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

    assert "data_pipeline.py --rebuild-all --weekly" in workflow
    assert (
        "B-S_backtest.py --rebuild-all --weekly --refresh-input-cache"
        in workflow
    )
    assert "select_completed_weekly_dates" in workflow
    assert "needs: rebuild-data" in workflow
    assert "actions/download-artifact" in workflow
    assert "path: backtest" in workflow
    assert '"files": file_hashes' in workflow
    for required in (
        "cb_amount_cache.csv",
        "cb_balance_cache.csv",
        "cb_rating_cache.csv",
        "BS_Model_Summary.xlsx",
    ):
        assert required in workflow
    assert "update_benchmark.py" in workflow
    assert "python -m pytest -q" in workflow
    assert "actions/upload-artifact" in workflow
    assert "git push origin" in workflow


def test_cpu_workflow_has_weekly_remote_schedule() -> None:
    workflow = (
        REPO_ROOT / ".github" / "workflows" / "full-backtest-cpu.yml"
    ).read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert 'cron: "30 9 * * 5"' in workflow
    assert "workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow
    assert "TUSHARE_TOKEN: ${{ secrets.TUSHARE_TOKEN }}" in workflow
    assert "contents: write" in workflow
