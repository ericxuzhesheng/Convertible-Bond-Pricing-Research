"""Disabled experimental ZL GPU entrypoint.

Use ``Z-L_backtest_GPU_prod.py``; it enforces observed point-in-time inputs and
fails closed when CUDA or a required market-data field is unavailable.
"""

from __future__ import annotations


LEGACY_GPU_DISABLED = True


def main() -> None:
    raise SystemExit(
        "Z-L_backtest_GPU.py is disabled because it used constant market-data "
        "fallbacks. Run Z-L_backtest_GPU_prod.py instead."
    )


if __name__ == "__main__":
    main()
