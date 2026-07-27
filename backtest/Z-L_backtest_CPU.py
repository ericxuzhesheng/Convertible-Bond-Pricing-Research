"""Disabled legacy ZL CPU entrypoint.

The former implementation embedded constant volatility, yield, credit-spread,
redemption, and clause fallbacks.  Those values cannot be distinguished from
observed point-in-time inputs and must not produce research outputs.
"""

from __future__ import annotations


LEGACY_CPU_DISABLED = True


def main() -> None:
    raise SystemExit(
        "Z-L_backtest_CPU.py is disabled because its historical constant "
        "fallbacks are not data-valid. Run data_pipeline.py --rebuild-all "
        "and Z-L_backtest_GPU_prod.py --rebuild-all instead."
    )


if __name__ == "__main__":
    main()
