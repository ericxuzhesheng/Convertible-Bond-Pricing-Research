"""Strict-incremental LSM-enhanced convertible-bond pricing backtest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import zlib
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

from lsm_backend import price_lsm_enhanced_zl_batch
from market_data_contracts import (
    DataContractError,
    PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    build_risk_free_rate_matrix,
    validate_pricing_coverage,
)


PIPELINE_DIR = Path(__file__).resolve().parent
MODEL_FILE = PIPELINE_DIR / "LSM_Model_Prices.csv"
MARKET_FILE = PIPELINE_DIR / "LSM_Market_Prices.csv"
ABS_FILE = PIPELINE_DIR / "LSM_Model_Deviation_Abs.csv"
PCT_FILE = PIPELINE_DIR / "LSM_Model_Deviation_Pct.csv"
SUMMARY_FILE = PIPELINE_DIR / "LSM_Model_Summary.xlsx"
MANIFEST_FILE = PIPELINE_DIR / "LSM_Model_Manifest.json"
FIGURE_FILE = PIPELINE_DIR / "Fig1_LSM_Price_Time_Series.png"
CONTRACT_VERSION = "weekly-lsm-v1"
IMPLEMENTATION_VERSION = "lsm-zl-max-quadratic-v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weekly", action="store_true")
    parser.add_argument(
        "--initialize-history",
        action="store_true",
        help="explicitly create the first certified LSM history",
    )
    parser.add_argument("--paths", type=int, default=256)
    parser.add_argument("--exercise-steps", type=int, default=48)
    return parser.parse_args()


def _load_wide(name: str) -> pd.DataFrame:
    path = PIPELINE_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"required LSM input is missing: {path}")
    frame = pd.read_csv(path, index_col=0, parse_dates=True)
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[frame.index.notna()]
    frame = frame.loc[~frame.index.duplicated(keep="last")]
    return frame.apply(pd.to_numeric, errors="coerce")


def _load_manifest() -> dict:
    try:
        payload = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _update_frame_digest(
    digest: "hashlib._Hash",
    label: str,
    frame: pd.DataFrame,
) -> None:
    normalized = frame.sort_index().sort_index(axis=1)
    digest.update(label.encode("utf-8"))
    digest.update(b"\0")
    digest.update(
        json.dumps(
            [str(column) for column in normalized.columns],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    digest.update(b"\0")
    row_hashes = pd.util.hash_pandas_object(
        normalized, index=True, categorize=False
    )
    digest.update(row_hashes.to_numpy(dtype=np.uint64).tobytes())
    digest.update(b"\0")


def _input_fingerprint(
    cutoff: pd.Timestamp,
    *,
    frames: dict[str, pd.DataFrame],
    redemption: pd.Series,
    parameters: dict,
) -> str:
    digest = hashlib.sha256()
    digest.update(IMPLEMENTATION_VERSION.encode("ascii"))
    digest.update(
        json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    )
    active = frames["zl_price"].loc[:cutoff]
    active_columns = active.columns[active.notna().any(axis=0)]
    for label, frame in frames.items():
        _update_frame_digest(
            digest,
            label,
            frame.loc[:cutoff].reindex(columns=active_columns),
        )
    _update_frame_digest(
        digest,
        "maturity_redemption",
        redemption.reindex(active_columns).to_frame(),
    )
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary)
    os.replace(temporary, path)


def _write_summary(
    model: pd.DataFrame,
    market: pd.DataFrame,
    absolute: pd.DataFrame,
    relative: pd.DataFrame,
) -> None:
    temporary = SUMMARY_FILE.with_suffix(".tmp.xlsx")
    with pd.ExcelWriter(temporary) as writer:
        model.to_excel(writer, sheet_name="理论价格")
        market.to_excel(writer, sheet_name="市场价格")
        absolute.to_excel(writer, sheet_name="绝对偏差")
        relative.to_excel(writer, sheet_name="相对偏差")
    os.replace(temporary, SUMMARY_FILE)


def _plot(model: pd.DataFrame, market: pd.DataFrame) -> None:
    finite = np.isfinite(model) & np.isfinite(market) & (market > 0)
    model_mean = model.where(finite).mean(axis=1)
    market_mean = market.where(finite).mean(axis=1)
    deviation = ((model - market) / market.replace(0, np.nan)).where(
        finite
    ).mean(axis=1) * 100.0
    fig, axis = plt.subplots(figsize=(12, 6))
    line_model, = axis.plot(model_mean.index, model_mean, "k-", label="LSM模型")
    line_market, = axis.plot(
        market_mean.index, market_mean, "k--", label="市场价格"
    )
    axis.set_xlabel("年份")
    axis.set_ylabel("转债平均价格 (元)")
    second = axis.twinx()
    second.fill_between(
        deviation.index, deviation, 0, color="gray", alpha=0.45
    )
    second.set_ylabel("平均定价错误 (%)")
    axis.legend(handles=[line_model, line_market], loc="upper center")
    plt.title("LSM模型定价结果与市场价格对比")
    plt.tight_layout()
    plt.savefig(FIGURE_FILE, dpi=300, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    if not args.weekly:
        raise SystemExit("LSM production output requires --weekly")
    if args.paths < 8 or args.paths % 2:
        raise SystemExit("--paths must be an even integer of at least 8")
    if args.exercise_steps < 2:
        raise SystemExit("--exercise-steps must be at least 2")

    parameters = {
        "paths": int(args.paths),
        "exercise_steps": int(args.exercise_steps),
        "basis": "quadratic-1-S-S2",
        "antithetic": True,
        "seed_scheme": "crc32-date-v1",
        "combination": "max-ZL-clause-aware-LSM-voluntary-conversion",
    }
    zl_price = _load_wide("ZL_Model_Prices.csv")
    market = _load_wide("ZL_Market_Prices.csv").reindex_like(zl_price)
    conversion = _load_wide("cb_convert_val_cache.csv").reindex_like(zl_price)
    maturity = _load_wide("cb_maturity_cache.csv").reindex_like(zl_price)
    volatility = _load_wide("bs_volatility_cache.csv").reindex_like(zl_price)
    spread = _load_wide("cb_credit_spread_cache.csv").reindex_like(zl_price)

    yield_curve = pd.read_csv(
        PIPELINE_DIR / "rf_yield_cache.csv", index_col=0, parse_dates=True
    )
    yield_curve.columns = [float(str(column).split("Y")[0]) for column in yield_curve]
    risk_free = build_risk_free_rate_matrix(
        curve=yield_curve,
        maturity=maturity,
    ).reindex_like(zl_price)

    basic = pd.read_csv(PIPELINE_DIR / "cb_basic_info.csv")
    basic = basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")
    redemption = pd.to_numeric(
        basic["maturity_call_price"], errors="coerce"
    ).reindex(zl_price.columns)
    frames = {
        "zl_price": zl_price,
        "conversion_value": conversion,
        "maturity": maturity,
        "volatility": volatility,
        "risk_free_rate": risk_free,
        "credit_spread": spread,
    }

    manifest = _load_manifest()
    verified_dates = pd.to_datetime(
        manifest.get("verified_dates", []), errors="coerce"
    )
    verified_dates = pd.DatetimeIndex(verified_dates).dropna()
    can_reuse = False
    previous_model = zl_price.copy()
    if manifest and not args.initialize_history:
        if manifest.get("contract_version") != CONTRACT_VERSION:
            raise DataContractError("LSM manifest contract version mismatch")
        if manifest.get("model_parameters") != parameters:
            raise DataContractError("LSM model parameters changed; explicit reinitialization required")
        if not SUMMARY_FILE.exists() or not verified_dates.size:
            raise DataContractError("LSM verified history is incomplete")
        cutoff = verified_dates.max()
        expected_fingerprint = _input_fingerprint(
            cutoff,
            frames=frames,
            redemption=redemption,
            parameters=parameters,
        )
        checks = {
            "input_cutoff": manifest.get("input_cutoff") == cutoff.date().isoformat(),
            "input_fingerprint": manifest.get("input_fingerprint") == expected_fingerprint,
            "output_sha256": manifest.get("output_sha256") == _sha256_file(SUMMARY_FILE),
        }
        if not all(checks.values()):
            failed = [name for name, passed in checks.items() if not passed]
            raise DataContractError(
                "LSM weekly history verification failed; refusing full rebuild: "
                + ", ".join(failed)
            )
        history = pd.read_excel(
            SUMMARY_FILE, sheet_name="理论价格", index_col=0
        )
        history.index = pd.to_datetime(history.index, errors="coerce")
        history = history.loc[history.index.notna()].apply(
            pd.to_numeric, errors="coerce"
        )
        aligned_history = history.reindex_like(previous_model)
        preserved_dates = previous_model.index.intersection(verified_dates)
        previous_model.loc[preserved_dates] = aligned_history.loc[
            preserved_dates
        ]
        can_reuse = True
    elif not args.initialize_history:
        raise DataContractError(
            "LSM history is not initialized; use --initialize-history once"
        )

    calculation_dates = zl_price.index[zl_price.notna().any(axis=1)]
    if can_reuse:
        calculation_dates = calculation_dates[calculation_dates > verified_dates.max()]
    if can_reuse and len(calculation_dates) == 0:
        print(
            "LSM incremental update: 0 new dates; verified history "
            f"preserved through {verified_dates.max().date()}"
        )
        return
    model = previous_model
    print(f"LSM dates to calculate: {len(calculation_dates)}")

    for date in tqdm(
        calculation_dates,
        desc="LSM Backtest (vectorized)",
        disable=not sys.stderr.isatty(),
    ):
        valid = (
            zl_price.loc[date].notna()
            & market.loc[date].notna()
            & conversion.loc[date].notna()
            & maturity.loc[date].notna()
            & volatility.loc[date].notna()
            & risk_free.loc[date].notna()
            & spread.loc[date].notna()
            & redemption.notna()
            & (conversion.loc[date] > 0)
            & (maturity.loc[date] > 0)
            & (volatility.loc[date] >= 0)
            & (spread.loc[date] >= 0)
        )
        codes = valid.index[valid]
        if len(codes) == 0:
            continue
        params = {
            "S0": conversion.loc[date, codes].to_numpy(float),
            "r": risk_free.loc[date, codes].to_numpy(float),
            "cs": spread.loc[date, codes].to_numpy(float),
            "sigma": volatility.loc[date, codes].to_numpy(float),
            "T": maturity.loc[date, codes].to_numpy(float),
            "maturity_redem": redemption.loc[codes].to_numpy(float),
        }
        seed = zlib.crc32(str(date).encode("utf-8")) & 0x7FFFFFFF
        model.loc[date, codes] = price_lsm_enhanced_zl_batch(
            zl_price.loc[date, codes].to_numpy(float),
            params,
            paths=args.paths,
            exercise_steps=args.exercise_steps,
            seed=seed,
        )

    model = model.reindex_like(zl_price)
    absolute = model - market
    relative = absolute / market.replace(0, np.nan)
    validation_dates = calculation_dates if len(calculation_dates) else zl_price.index[-1:]
    validate_pricing_coverage(
        market_price=zl_price,
        model_price=model,
        dates=validation_dates,
        min_coverage=1.0,
        min_count=1,
        label="LSM preservation of ZL-priced cells",
        min_count_enforced_from=PUBLIC_CB_MIN_COUNT_ENFORCED_FROM,
    )

    _atomic_csv(model, MODEL_FILE)
    _atomic_csv(market, MARKET_FILE)
    _atomic_csv(absolute, ABS_FILE)
    _atomic_csv(relative, PCT_FILE)
    _write_summary(model, market, absolute, relative)
    _plot(model, market)

    output_dates = model.index[model.notna().any(axis=1)]
    cutoff = output_dates.max()
    fingerprint = _input_fingerprint(
        cutoff,
        frames=frames,
        redemption=redemption,
        parameters=parameters,
    )
    payload = {
        "contract_version": CONTRACT_VERSION,
        "execution_backend": "numpy-vectorized",
        "verified_dates": [date.date().isoformat() for date in output_dates],
        "input_cutoff": cutoff.date().isoformat(),
        "input_fingerprint": fingerprint,
        "output_sha256": _sha256_file(SUMMARY_FILE),
        "model_parameters": parameters,
    }
    temporary_manifest = MANIFEST_FILE.with_suffix(".json.tmp")
    temporary_manifest.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary_manifest, MANIFEST_FILE)

    finite = np.isfinite(model) & np.isfinite(market)
    error = (model - market).where(finite).stack()
    print(f"LSM priced cells: {int(finite.sum().sum())}")
    print(f"LSM mean error: {error.mean():.4f}")
    print(f"LSM MAE: {error.abs().mean():.4f}")
    print(f"LSM output cutoff: {cutoff.date()}")


if __name__ == "__main__":
    main()
