from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "weekly-incremental-cpu.yml"
sys.path.insert(0, str(BACKTEST_DIR))

from dataclasses import replace
from pricing_engine import Contract,price_mc


def _contract():
    return Contract(parity=80.,maturity=.25,redemption=110.,sigma=0.,spread=.01,
                    curve_rates=np.array([.02,.02]))


def test_cpu_backend_matches_deterministic_hold_to_maturity_value():
    actual=price_mc(_contract(),method="ZL",paths=32,seed=7)[0]
    assert abs(actual-110*math.exp(-.03*.25))<1e-10


def test_cpu_backend_is_reproducible_for_the_same_seed():
    c=replace(_contract(),sigma=.25)
    assert price_mc(c,method="ZL",paths=64,seed=20260730)==price_mc(c,method="ZL",paths=64,seed=20260730)


def test_cpu_backend_executes_observed_put_trigger():
    c=replace(_contract(),parity=60.,spread=0.,curve_rates=np.zeros(2),put_start=0.,put_ratio=.7,put_days=2)
    assert price_mc(c,method="ZL",paths=8,seed=11)[0]==100.


def test_cpu_backend_executes_observed_redemption_trigger():
    c=replace(_contract(),parity=150.,spread=0.,curve_rates=np.zeros(2),call_ratio=1.3,call_notice_days=0)
    assert price_mc(c,method="ZL",paths=8,seed=13)[0]==150.


def test_github_manual_fallback_runs_complete_incremental_pipeline_on_cpu() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert 'workflow_dispatch:' in workflow
    assert '  schedule:' not in workflow
    assert 'cron:' not in workflow
    assert "PIPELINE_START=" in workflow
    assert (
        'data_pipeline.py\n          --start "${PIPELINE_START}"\n'
        in workflow
    )
    assert "--reuse-clause-cache" in workflow
    assert "--reuse-conversion-event-cache" in workflow
    assert "B-S_backtest.py --weekly" in workflow
    assert (
        "Z-L_backtest_CPU_prod.py --weekly --offline-inputs"
        in workflow
    )
    assert "rebuild_research_outputs.py" in workflow
    assert "rebuild_research_outputs.py --full-history --refresh-inputs --promote" in workflow
    assert 'if: ${{ false }}' not in workflow
    assert "--rebuild-all" not in workflow
    assert "git push origin" in workflow


def test_python_setup_does_not_require_a_missing_dependency_manifest() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "cache: pip" not in workflow


def test_cpu_cloud_environment_does_not_install_or_import_cuda() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    driver = (
        BACKTEST_DIR / "Z-L_backtest_GPU_prod.py"
    ).read_text(encoding="utf-8")

    assert "numba-cuda" not in workflow
    assert "from numba import cuda" not in driver
    assert (BACKTEST_DIR / "zl_cuda_backend.py").exists()


def test_cpu_backend_uses_the_versioned_fingerprint_contract():
    source=(BACKTEST_DIR/"pricing_backtest.py").read_text(encoding="utf-8")
    assert 'CONTRACT_VERSION = "cashflow-pricing-v2"' in source
    assert "input_fingerprint" in source and "artifact_sha256" in source
    assert "refusing full rebuild" in source


def test_local_sync_is_fail_closed_and_fast_forward_only() -> None:
    sync_source = (
        BACKTEST_DIR / "sync_main_from_github.ps1"
    ).read_text(encoding="utf-8")
    setup_source = (
        BACKTEST_DIR / "setup_main_sync_task.ps1"
    ).read_text(encoding="utf-8")

    assert "git status --porcelain" in sync_source
    assert "git merge --ff-only origin/main" in sync_source
    assert "Register-ScheduledTask" in setup_source
    assert "AtLogOn" in setup_source
