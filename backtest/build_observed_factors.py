"""Rebuild the five non-pricing factors from observed daily market caches."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from market_data_contracts import DataContractError


PIPELINE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PIPELINE_DIR.parent
FACTOR_DIR = REPO_ROOT / "mispricing factor"

OUTPUT_FILES = {
    "liquidity": "流动性因子等权和.csv",
    "volatility": "波动率因子等权和.csv",
    "price_volume": "量价相关性因子等权和.csv",
    "valuation": "估值因子等权和.csv",
    "momentum": "动量因子等权和.csv",
}


def _numeric_aligned(
    frame: pd.DataFrame,
    *,
    index: pd.Index,
    columns: pd.Index,
) -> pd.DataFrame:
    numeric = frame.apply(pd.to_numeric, errors="coerce").copy()
    numeric.index = pd.to_datetime(numeric.index, errors="coerce")
    numeric = numeric.loc[numeric.index.notna()]
    return numeric.reindex(index=index, columns=columns)


def build_observed_factors(
    *,
    price: pd.DataFrame,
    amount: pd.DataFrame,
    conversion_value: pd.DataFrame,
    bond_floor: pd.DataFrame,
    lookback: int = 20,
    min_observations: int = 10,
) -> dict[str, pd.DataFrame]:
    """Calculate factors without constants, interpolation, or forward filling."""

    if lookback < 2:
        raise DataContractError("lookback must be at least 2")
    if not 2 <= min_observations <= lookback:
        raise DataContractError(
            "min_observations must be between 2 and lookback"
        )

    px = price.apply(pd.to_numeric, errors="coerce").copy()
    px.index = pd.to_datetime(px.index, errors="coerce")
    px = px.loc[px.index.notna()].sort_index()
    if px.index.has_duplicates:
        raise DataContractError("price index contains duplicate dates")
    active = px.notna()
    if (px.where(active) <= 0).any().any():
        raise DataContractError("price contains nonpositive observations")

    aligned = {
        "amount": _numeric_aligned(
            amount, index=px.index, columns=px.columns
        ),
        "conversion_value": _numeric_aligned(
            conversion_value, index=px.index, columns=px.columns
        ),
        "bond_floor": _numeric_aligned(
            bond_floor, index=px.index, columns=px.columns
        ),
    }
    for name in ("conversion_value", "bond_floor"):
        invalid = active & aligned[name].notna() & (aligned[name] <= 0)
        if invalid.any().any():
            raise DataContractError(
                f"{name} contains nonpositive observations"
            )

    observed_amount = aligned["amount"].where(aligned["amount"] >= 0)
    returns = px.pct_change(fill_method=None)
    log_amount_change = np.log(
        observed_amount.where(observed_amount > 0)
    ).diff()

    liquidity = np.log1p(
        observed_amount.rolling(
            lookback, min_periods=min_observations
        ).mean()
    )
    volatility = (
        returns.rolling(
            lookback, min_periods=min_observations
        ).std()
        * np.sqrt(252.0)
    )
    price_volume = returns.rolling(
        lookback, min_periods=min_observations
    ).corr(log_amount_change)

    conversion_premium = (
        px / aligned["conversion_value"].where(
            aligned["conversion_value"] > 0
        )
        - 1.0
    )
    floor_premium = (
        px / aligned["bond_floor"].where(aligned["bond_floor"] > 0)
        - 1.0
    )
    valuation = (conversion_premium + floor_premium) / 2.0
    momentum = px.pct_change(periods=lookback, fill_method=None)

    factors = {
        "liquidity": liquidity,
        "volatility": volatility,
        "price_volume": price_volume,
        "valuation": valuation,
        "momentum": momentum,
    }
    return {
        name: factor.replace([np.inf, -np.inf], np.nan).where(active)
        for name, factor in factors.items()
    }


def _load_cache(filename: str) -> pd.DataFrame:
    path = PIPELINE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"required market cache not found: {path}")
    return pd.read_csv(path, index_col=0, parse_dates=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--min-observations", type=int, default=10)
    args = parser.parse_args()

    factors = build_observed_factors(
        price=_load_cache("cb_price_cache.csv"),
        amount=_load_cache("cb_amount_cache.csv"),
        conversion_value=_load_cache("cb_convert_val_cache.csv"),
        bond_floor=_load_cache("cb_bond_floor_cache.csv"),
        lookback=args.lookback,
        min_observations=args.min_observations,
    )
    FACTOR_DIR.mkdir(parents=True, exist_ok=True)
    for name, frame in factors.items():
        output = FACTOR_DIR / OUTPUT_FILES[name]
        frame.index.name = "date"
        frame.to_csv(output, encoding="utf-8-sig")
        print(
            f"{name}: {output.name}, coverage="
            f"{frame.notna().mean().mean():.2%}"
        )

    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "price": "backtest/cb_price_cache.csv (Tushare cb_daily)",
            "amount": "backtest/cb_amount_cache.csv (Tushare cb_daily)",
            "conversion_value": (
                "backtest/cb_convert_val_cache.csv "
                "(Tushare cb_daily bond_value)"
            ),
            "bond_floor": (
                "backtest/cb_bond_floor_cache.csv "
                "(Tushare cb_daily cb_value)"
            ),
        },
        "lookback_trading_days": args.lookback,
        "min_observations": args.min_observations,
        "formulas": {
            "liquidity": "log1p(rolling mean amount)",
            "volatility": "rolling std of daily price returns * sqrt(252)",
            "price_volume": (
                "rolling correlation of price return and log amount change"
            ),
            "valuation": (
                "mean of conversion premium and pure-bond premium"
            ),
            "momentum": "lookback price return",
        },
        "missing_data_policy": "no forward fill and no constant fallback",
    }
    (FACTOR_DIR / "observed_factor_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
