"""CPU entrypoint for the production ZL weekly incremental backtest."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


DRIVER = Path(__file__).with_name("Z-L_backtest_GPU_prod.py")


def main() -> None:
    forwarded = [
        argument
        for argument in sys.argv[1:]
        if argument not in {"--backend", "cpu", "cuda"}
    ]
    sys.argv = [str(DRIVER), "--backend", "cpu", *forwarded]
    runpy.run_path(str(DRIVER), run_name="__main__")


if __name__ == "__main__":
    main()
