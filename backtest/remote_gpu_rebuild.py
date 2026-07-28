"""Run the CUDA-dependent full backtest stage on remote GPU compute.

The free GitHub-hosted stage rebuilds BS outputs and the benchmark first. This
module then rebuilds ZL from scratch, validates its coverage, regenerates all
downstream research outputs, and persists a result archive to Hugging Face Hub
when ``HF_RESULTS_REPO`` is configured.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from market_data_contracts import select_completed_weekly_dates


BACKTEST_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKTEST_DIR.parent
OUTPUT_DIR = REPO_ROOT / "outputs"

RESULT_PATTERNS = (
    "backtest/BS_Model_*",
    "backtest/ZL_Model_*",
    "backtest/Market_Prices.csv",
    "backtest/ZL_Market_Prices.csv",
    "backtest/Fig*_BS_*.png",
    "backtest/Fig*_ZL_*.png",
    "backtest/remote_cpu_stage_manifest.json",
    "mispricing factor/*.csv",
    "mispricing factor/*.png",
    "long-short strategy/*.csv",
    "long-short strategy/*.png",
)
CPU_HANDOFF_FILES = {
    "backtest/cb_price_cache.csv": BACKTEST_DIR / "cb_price_cache.csv",
    "backtest/cb_convert_val_cache.csv": (
        BACKTEST_DIR / "cb_convert_val_cache.csv"
    ),
    "backtest/cb_bond_floor_cache.csv": (
        BACKTEST_DIR / "cb_bond_floor_cache.csv"
    ),
    "backtest/cb_maturity_cache.csv": BACKTEST_DIR / "cb_maturity_cache.csv",
    "backtest/cb_credit_spread_cache.csv": (
        BACKTEST_DIR / "cb_credit_spread_cache.csv"
    ),
    "backtest/cb_amount_cache.csv": BACKTEST_DIR / "cb_amount_cache.csv",
    "backtest/cb_balance_cache.csv": BACKTEST_DIR / "cb_balance_cache.csv",
    "backtest/cb_rating_cache.csv": BACKTEST_DIR / "cb_rating_cache.csv",
    "backtest/cb_stock_mv_cache.csv": BACKTEST_DIR / "cb_stock_mv_cache.csv",
    "backtest/cb_bps_cache.csv": BACKTEST_DIR / "cb_bps_cache.csv",
    "backtest/cb_conversion_price_cache.csv": (
        BACKTEST_DIR / "cb_conversion_price_cache.csv"
    ),
    "backtest/cb_basic_info.csv": BACKTEST_DIR / "cb_basic_info.csv",
    "backtest/cb_clause_terms.csv": BACKTEST_DIR / "cb_clause_terms.csv",
    "backtest/rf_yield_cache.csv": BACKTEST_DIR / "rf_yield_cache.csv",
    "backtest/bs_volatility_cache.csv": (
        BACKTEST_DIR / "bs_volatility_cache.csv"
    ),
    "backtest/BS_Model_Prices.csv": BACKTEST_DIR / "BS_Model_Prices.csv",
    "backtest/BS_Model_Deviation_Abs.csv": (
        BACKTEST_DIR / "BS_Model_Deviation_Abs.csv"
    ),
    "backtest/BS_Model_Deviation_Pct.csv": (
        BACKTEST_DIR / "BS_Model_Deviation_Pct.csv"
    ),
    "backtest/BS_Model_Summary.xlsx": BACKTEST_DIR / "BS_Model_Summary.xlsx",
    "backtest/Market_Prices.csv": BACKTEST_DIR / "Market_Prices.csv",
    "long-short strategy/000832_CSI_close_price.csv": (
        REPO_ROOT / "long-short strategy" / "000832_CSI_close_price.csv"
    ),
}


def _cuda_available() -> bool:
    from numba import cuda

    return bool(cuda.is_available())


def _run_script(
    script: Path,
    *arguments: str,
    python_executable: str,
) -> None:
    command = [python_executable, str(script), *arguments]
    print(f"[remote GPU] running {script.name}", flush=True)
    result = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"{script.stem} failed with exit code {result.returncode}"
        )


def validate_cpu_stage_handoff(
    *,
    manifest_path: Path = BACKTEST_DIR / "remote_cpu_stage_manifest.json",
    required_files: dict[str, Path] | None = None,
    expected_run_id: str | None = None,
    expected_source_sha: str | None = None,
    expected_branch: str | None = None,
) -> dict:
    if required_files is None:
        required_files = CPU_HANDOFF_FILES
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(
            f"CPU-stage handoff manifest unavailable: {manifest_path}"
        ) from exc
    expectations = {
        "workflow_run_id": (
            expected_run_id or os.environ.get("EXPECTED_CPU_RUN_ID")
        ),
        "workflow_source_sha": (
            expected_source_sha
            or os.environ.get("EXPECTED_WORKFLOW_SOURCE_SHA")
        ),
        "branch": expected_branch or os.environ.get("EXPECTED_CPU_BRANCH"),
    }
    for field, expected_value in expectations.items():
        if not expected_value:
            raise RuntimeError(
                f"CPU-stage handoff requires expected {field}"
            )
        if str(manifest.get(field)) != str(expected_value):
            raise RuntimeError(
                f"CPU-stage handoff {field} mismatch: "
                f"{manifest.get(field)!r} != {expected_value!r}"
            )
    recorded = manifest.get("files")
    if not isinstance(recorded, dict):
        raise RuntimeError("CPU-stage handoff manifest has no file hashes")
    for relative_path, path in required_files.items():
        expected = recorded.get(relative_path)
        if not isinstance(expected, str):
            raise RuntimeError(
                f"CPU-stage handoff missing hash for {relative_path}"
            )
        if not path.is_file():
            raise RuntimeError(
                f"CPU-stage handoff file missing: {relative_path}"
            )
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(
                f"CPU-stage handoff hash mismatch for {relative_path}"
            )
    return manifest


def validate_zl_coverage(
    *,
    market_path: Path = BACKTEST_DIR / "cb_price_cache.csv",
    model_path: Path = BACKTEST_DIR / "ZL_Model_Prices.csv",
    minimum: float = 0.90,
) -> float:
    market = pd.read_csv(market_path, index_col=0)
    model = pd.read_csv(model_path, index_col=0)
    market.index = pd.to_datetime(market.index, errors="coerce")
    model.index = pd.to_datetime(model.index, errors="coerce")
    weekly_dates = select_completed_weekly_dates(market.index)
    market = market.reindex(index=weekly_dates)
    model = model.reindex(index=market.index, columns=market.columns)

    expected = market.notna()
    priced = expected & np.isfinite(model)
    expected_cells = int(expected.sum().sum())
    priced_cells = int(priced.sum().sum())
    coverage = priced_cells / expected_cells if expected_cells else 0.0
    print(
        "[remote GPU] ZL coverage "
        f"{priced_cells}/{expected_cells} ({coverage:.2%})",
        flush=True,
    )
    if coverage < minimum:
        raise RuntimeError(
            f"ZL coverage {coverage:.2%} is below required "
            f"{minimum:.2%}"
        )
    return coverage


def _result_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in RESULT_PATTERNS:
        files.update(
            path for path in REPO_ROOT.glob(pattern) if path.is_file()
        )
    return sorted(files)


def build_results_archive(
    *,
    coverage: float,
    output_path: Path | None = None,
) -> Path:
    if output_path is None:
        output_path = OUTPUT_DIR / "full-backtest-results.zip"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    files = _result_files()
    if not files:
        raise RuntimeError("No full-backtest result files were produced")

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "zl_coverage": coverage,
        "files": {
            str(path.relative_to(REPO_ROOT)).replace("\\", "/"): {
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in files
        },
    }
    with zipfile.ZipFile(
        output_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for path in files:
            archive.write(path, path.relative_to(REPO_ROOT))
        archive.writestr(
            "full-backtest-manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2),
        )
    return output_path


def _upload_results(archive_path: Path, results_repo: str) -> None:
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError(
            "HF_TOKEN is required to persist remote GPU results"
        )

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    api.create_repo(
        repo_id=results_repo,
        repo_type="dataset",
        private=True,
        exist_ok=True,
    )
    api.upload_file(
        path_or_fileobj=archive_path,
        path_in_repo=archive_path.name,
        repo_id=results_repo,
        repo_type="dataset",
    )
    print(
        f"[remote GPU] uploaded results to hf://datasets/{results_repo}",
        flush=True,
    )


def run_remote_gpu_stage(
    *,
    gpu_available: bool | None = None,
    python_executable: str = sys.executable,
    results_repo: str | None = None,
) -> Path:
    if gpu_available is None:
        gpu_available = _cuda_available()
    if not gpu_available:
        raise RuntimeError(
            "CUDA is unavailable; remote GPU stage stopped before mutation"
        )

    validate_cpu_stage_handoff()
    _run_script(
        BACKTEST_DIR / "Z-L_backtest_GPU_prod.py",
        "--rebuild-all",
        "--weekly",
        python_executable=python_executable,
    )
    coverage = validate_zl_coverage()
    _run_script(
        BACKTEST_DIR / "rebuild_research_outputs.py",
        python_executable=python_executable,
    )
    archive_path = build_results_archive(coverage=coverage)

    if results_repo:
        _upload_results(archive_path, results_repo)
    return archive_path


if __name__ == "__main__":
    run_remote_gpu_stage(results_repo=os.environ.get("HF_RESULTS_REPO"))
