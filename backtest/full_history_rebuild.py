"""GPU-gated, fail-closed full-history research rebuild."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKTEST_DIR.parent


def _cuda_available() -> bool:
    from numba import cuda

    return bool(cuda.is_available())


def run_full_rebuild(
    *,
    gpu_available: bool | None = None,
    python_executable: str = sys.executable,
) -> None:
    if gpu_available is None:
        gpu_available = _cuda_available()
    if not gpu_available:
        raise RuntimeError(
            "CUDA is unavailable; full-history rebuild stopped before "
            "downloading data or replacing research outputs"
        )

    steps = [
        [python_executable, str(BACKTEST_DIR / "data_pipeline.py"), "--rebuild-all"],
        [python_executable, str(BACKTEST_DIR / "B-S_backtest.py"), "--rebuild-all"],
        [
            python_executable,
            str(BACKTEST_DIR / "Z-L_backtest_GPU_prod.py"),
            "--rebuild-all",
        ],
        [
            python_executable,
            str(
                REPO_ROOT
                / "long-short strategy"
                / "update_benchmark.py"
            ),
        ],
        [
            python_executable,
            str(BACKTEST_DIR / "rebuild_research_outputs.py"),
        ],
    ]
    for command in steps:
        script_name = Path(command[1]).name
        print(f"[full rebuild] running {script_name}", flush=True)
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{Path(script_name).stem} failed with exit code "
                f"{result.returncode}"
            )


if __name__ == "__main__":
    run_full_rebuild()
