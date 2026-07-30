from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "full-backtest-cpu.yml"
sys.path.insert(0, str(BACKTEST_DIR))

from zl_cpu_backend import price_batch_cpu  # noqa: E402


def _single_bond_params(*, sigma: float = 0.0) -> dict[str, np.ndarray]:
    return {
        "S0": np.array([80.0]),
        "X0": np.array([100.0]),
        "r": np.array([0.02]),
        "cs": np.array([0.01]),
        "sigma": np.array([sigma]),
        "T": np.array([0.25]),
        "maturity_redem": np.array([110.0]),
        "call_price": np.array([130.0]),
        "put_price": np.array([100.0]),
        "put_barrier": np.array([0.5]),
        "put_window": np.array([30.0]),
        "put_years": np.array([2.0]),
        "redeem_ratio": np.array([2.0]),
        "redeem_window": np.array([30.0]),
        "redeem_required": np.array([30.0]),
        "initial_put_count": np.array([0], dtype=np.int32),
        "initial_redeem_count": np.array([0], dtype=np.int32),
        "initial_redeem_flags": np.zeros((1, 64), dtype=np.int8),
    }


def test_cpu_backend_matches_deterministic_hold_to_maturity_value() -> None:
    params = _single_bond_params()

    actual = price_batch_cpu(params, paths=32, seed=7)

    expected = 110.0 * math.exp(-(0.02 + 0.01) * 0.25)
    np.testing.assert_allclose(actual, [expected], rtol=1e-12, atol=1e-12)


def test_cpu_backend_is_reproducible_for_the_same_seed() -> None:
    params = _single_bond_params(sigma=0.25)

    first = price_batch_cpu(params, paths=64, seed=20260730)
    second = price_batch_cpu(params, paths=64, seed=20260730)

    np.testing.assert_array_equal(first, second)


def test_cpu_backend_executes_observed_put_trigger() -> None:
    params = _single_bond_params()
    params["S0"][:] = 60.0
    params["r"][:] = 0.0
    params["cs"][:] = 0.0
    params["put_barrier"][:] = 0.7
    params["put_window"][:] = 2
    params["redeem_ratio"][:] = 10.0

    actual = price_batch_cpu(params, paths=8, seed=11)

    np.testing.assert_allclose(actual, [100.0], rtol=0.0, atol=0.0)


def test_cpu_backend_executes_observed_redemption_trigger() -> None:
    params = _single_bond_params()
    params["S0"][:] = 150.0
    params["r"][:] = 0.0
    params["cs"][:] = 0.0
    params["redeem_window"][:] = 3
    params["redeem_required"][:] = 2

    actual = price_batch_cpu(params, paths=8, seed=13)

    np.testing.assert_allclose(actual, [150.0], rtol=0.0, atol=0.0)


def test_github_cron_runs_complete_incremental_pipeline_on_cpu() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert 'cron: "30 9 * * 5"' in workflow
    assert "data_pipeline.py --weekly" in workflow
    assert "B-S_backtest.py --weekly" in workflow
    assert (
        "Z-L_backtest_CPU_prod.py --weekly --offline-inputs"
        in workflow
    )
    assert "rebuild_research_outputs.py" in workflow
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


def test_cpu_backend_preserves_existing_verified_history_fingerprint() -> None:
    source = (
        BACKTEST_DIR / "Z-L_backtest_GPU_prod.py"
    ).read_text(encoding="utf-8")

    assert (
        'ZL_MODEL_IMPLEMENTATION_VERSION = ('
        in source
    )
    assert (
        "digest.update(ZL_MODEL_IMPLEMENTATION_VERSION.encode(\"ascii\"))"
        in source
    )


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
