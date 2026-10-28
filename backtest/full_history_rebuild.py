"""Explicit cache-only rebuild of the corrected pricing history.

Market-data acquisition and research/weekly-report rebuilding are separate tasks.
"""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

BACKTEST_DIR=Path(__file__).resolve().parent


def run_full_rebuild(*,python_executable=sys.executable):
    subprocess.run([python_executable,str(BACKTEST_DIR/"reprice_history.py"),
        "--resume","--promote"],check=True,cwd=BACKTEST_DIR.parent)


if __name__=="__main__": run_full_rebuild()
