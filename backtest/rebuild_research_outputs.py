"""Fail-closed rebuild of factors, strategies, and README figures."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BACKTEST_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKTEST_DIR.parent
STEPS = [
    BACKTEST_DIR / "build_observed_factors.py",
    REPO_ROOT / "mispricing factor" / "B-S_mispricing_factor.py",
    REPO_ROOT / "mispricing factor" / "Z-L_mispricing_factor.py",
    REPO_ROOT / "mispricing factor" / "LSM_mispricing_factor.py",
    REPO_ROOT / "long-short strategy" / "BS_ZL_LSM_strategy.py",
    BACKTEST_DIR / "regenerate_plots.py",
]


def run_checked_steps(
    *,
    python_executable: str = sys.executable,
) -> None:
    for script in STEPS:
        print(f"[research rebuild] running {script.name}", flush=True)
        result = subprocess.run(
            [python_executable, str(script)],
            cwd=REPO_ROOT,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{script.stem} failed with exit code "
                f"{result.returncode}"
            )


if __name__ == "__main__":
    run_checked_steps()
