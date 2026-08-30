"""Vectorized Longstaff-Schwartz early-conversion premium for convertible bonds."""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np


REQUIRED_FIELDS = (
    "S0",
    "r",
    "cs",
    "sigma",
    "T",
    "maturity_redem",
)


def _validated_params(
    params: Mapping[str, np.ndarray],
) -> dict[str, np.ndarray]:
    missing = [field for field in REQUIRED_FIELDS if field not in params]
    if missing:
        raise ValueError(f"missing LSM parameters: {missing}")
    normalized = {
        field: np.asarray(params[field], dtype=np.float64).reshape(-1)
        for field in REQUIRED_FIELDS
    }
    count = len(normalized["S0"])
    if any(len(values) != count for values in normalized.values()):
        raise ValueError("all LSM parameter vectors must have equal length")
    if np.any(normalized["S0"] <= 0):
        raise ValueError("S0 must be positive")
    if np.any(normalized["sigma"] < 0):
        raise ValueError("sigma must be non-negative")
    if np.any(normalized["T"] <= 0):
        raise ValueError("T must be positive")
    if np.any(normalized["cs"] < 0):
        raise ValueError("credit spread must be non-negative")
    if np.any(normalized["maturity_redem"] <= 0):
        raise ValueError("maturity redemption must be positive")
    return normalized


def price_early_conversion_premium_batch(
    params: Mapping[str, np.ndarray],
    *,
    paths: int = 256,
    exercise_steps: int = 48,
    seed: int = 42,
) -> np.ndarray:
    """Estimate the American early-conversion premium with vectorized LSM.

    The simulated underlying is conversion value. The terminal payoff is the
    greater of conversion value and contractual maturity redemption. At each
    exercise date, continuation value is regressed on ``1, S, S^2`` using
    in-the-money paths, matching the quadratic specification in the Northeast
    Securities report. Antithetic normal draws reduce Monte Carlo noise.

    The returned standalone premium is non-negative. The combined model uses
    the full LSM value separately so the conversion option is not double-counted.
    """

    if paths < 8 or paths % 2:
        raise ValueError("paths must be an even integer of at least 8")
    if exercise_steps < 2:
        raise ValueError("exercise_steps must be at least 2")

    premium, _ = _price_lsm_components(
        params,
        paths=paths,
        exercise_steps=exercise_steps,
        seed=seed,
    )
    return premium


def _price_lsm_components(
    params: Mapping[str, np.ndarray],
    *,
    paths: int,
    exercise_steps: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    if paths < 8 or paths % 2:
        raise ValueError("paths must be an even integer of at least 8")
    if exercise_steps < 2:
        raise ValueError("exercise_steps must be at least 2")
    values = _validated_params(params)
    bond_count = len(values["S0"])
    if bond_count == 0:
        empty = np.empty(0, dtype=np.float64)
        return empty, empty

    s0 = values["S0"]
    rate = values["r"]
    spread = values["cs"]
    sigma = values["sigma"]
    maturity = values["T"]
    redemption = values["maturity_redem"]
    discount_rate = rate + spread
    dt = maturity / float(exercise_steps)
    step_discount = np.exp(-discount_rate * dt)

    rng = np.random.default_rng(int(seed))
    half = paths // 2
    normals_half = rng.standard_normal(
        (bond_count, half, exercise_steps), dtype=np.float64
    )
    normals = np.concatenate((normals_half, -normals_half), axis=1)
    drift = (rate - 0.5 * sigma * sigma) * dt
    increments = (
        drift[:, None, None]
        + sigma[:, None, None]
        * np.sqrt(dt)[:, None, None]
        * normals
    )
    stock_paths = s0[:, None, None] * np.exp(
        np.cumsum(increments, axis=2)
    )

    terminal = np.maximum(stock_paths[:, :, -1], redemption[:, None])
    european = terminal.mean(axis=1) * np.exp(-discount_rate * maturity)
    path_value = terminal

    for step in range(exercise_steps - 2, -1, -1):
        discounted = path_value * step_discount[:, None]
        immediate = stock_paths[:, :, step]
        remaining = maturity * (
            1.0 - float(step + 1) / float(exercise_steps)
        )
        discounted_floor = redemption * np.exp(-discount_rate * remaining)
        in_the_money = immediate > discounted_floor[:, None]

        scaled = immediate / s0[:, None]
        weights = in_the_money.astype(np.float64)
        z1 = scaled
        z2 = z1 * z1
        z3 = z2 * z1
        z4 = z2 * z2

        moments = np.empty((bond_count, 3, 3), dtype=np.float64)
        moments[:, 0, 0] = weights.sum(axis=1)
        moments[:, 0, 1] = moments[:, 1, 0] = (weights * z1).sum(axis=1)
        moments[:, 0, 2] = moments[:, 2, 0] = (weights * z2).sum(axis=1)
        moments[:, 1, 1] = (weights * z2).sum(axis=1)
        moments[:, 1, 2] = moments[:, 2, 1] = (weights * z3).sum(axis=1)
        moments[:, 2, 2] = (weights * z4).sum(axis=1)
        rhs = np.stack(
            (
                (weights * discounted).sum(axis=1),
                (weights * discounted * z1).sum(axis=1),
                (weights * discounted * z2).sum(axis=1),
            ),
            axis=1,
        )

        ridge = np.maximum(moments[:, 0, 0], 1.0) * 1e-10
        moments[:, 0, 0] += ridge
        moments[:, 1, 1] += ridge
        moments[:, 2, 2] += ridge
        coefficients = np.linalg.solve(
            moments, rhs[:, :, None]
        )[:, :, 0]
        continuation = (
            coefficients[:, 0, None]
            + coefficients[:, 1, None] * z1
            + coefficients[:, 2, None] * z2
        )
        enough_paths = moments[:, 0, 0] > 3.0
        exercise = (
            in_the_money
            & enough_paths[:, None]
            & np.isfinite(continuation)
            & (immediate > continuation)
        )
        path_value = np.where(exercise, immediate, discounted)

    american = path_value.mean(axis=1) * step_discount
    return np.maximum(american - european, 0.0), american


def price_lsm_enhanced_zl_batch(
    base_zl_price: np.ndarray,
    params: Mapping[str, np.ndarray],
    *,
    paths: int = 256,
    exercise_steps: int = 48,
    seed: int = 42,
) -> np.ndarray:
    """Combine clause-aware ZL and voluntary-conversion LSM values.

    Taking the larger standalone value avoids adding the same conversion
    option twice while retaining both ZL clause value and LSM exercise value.
    """

    base = np.asarray(base_zl_price, dtype=np.float64).reshape(-1)
    _, lsm_value = _price_lsm_components(
        params,
        paths=paths,
        exercise_steps=exercise_steps,
        seed=seed,
    )
    if len(base) != len(lsm_value):
        raise ValueError("base ZL prices and LSM parameters must align")
    return np.maximum(base, lsm_value)
