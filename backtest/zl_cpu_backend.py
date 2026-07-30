"""Deterministic CPU execution backend for the production ZL Monte Carlo model."""

from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
from numba import njit, prange


_REQUIRED_VECTOR_FIELDS = (
    "S0",
    "X0",
    "r",
    "cs",
    "sigma",
    "T",
    "maturity_redem",
    "call_price",
    "put_price",
    "put_barrier",
    "put_window",
    "put_years",
    "redeem_ratio",
    "redeem_window",
    "redeem_required",
    "initial_put_count",
    "initial_redeem_count",
)
_MASK_64 = np.uint64(0xFFFFFFFFFFFFFFFF)
_GOLDEN_GAMMA = np.uint64(0x9E3779B97F4A7C15)
_MIX_1 = np.uint64(0xBF58476D1CE4E5B9)
_MIX_2 = np.uint64(0x94D049BB133111EB)
_TWO_PI = 2.0 * math.pi
_U53_SCALE = 1.0 / 9007199254740992.0


@njit(inline="always")
def _next_uniform(state: np.uint64) -> tuple[np.uint64, float]:
    state = (state + _GOLDEN_GAMMA) & _MASK_64
    mixed = state
    mixed = ((mixed ^ (mixed >> np.uint64(30))) * _MIX_1) & _MASK_64
    mixed = ((mixed ^ (mixed >> np.uint64(27))) * _MIX_2) & _MASK_64
    mixed = mixed ^ (mixed >> np.uint64(31))
    uniform = (float(mixed >> np.uint64(11)) + 0.5) * _U53_SCALE
    return state, uniform


@njit(inline="always")
def _next_normal(state: np.uint64) -> tuple[np.uint64, float]:
    state, first = _next_uniform(state)
    state, second = _next_uniform(state)
    normal = math.sqrt(-2.0 * math.log(first)) * math.cos(
        _TWO_PI * second
    )
    return state, normal


@njit(parallel=True, cache=True)
def _price_paths_cpu(
    s0_values,
    x0_values,
    rate_values,
    spread_values,
    sigma_values,
    maturity_values,
    redemption_values,
    call_values,
    put_values,
    put_barriers,
    put_windows,
    put_years_values,
    redeem_ratios,
    redeem_windows,
    redeem_required_values,
    initial_put_counts,
    initial_redeem_counts,
    initial_redeem_flags,
    paths,
    seed,
):
    bond_count = s0_values.shape[0]
    output = np.empty(bond_count * paths, dtype=np.float64)

    for path_index in prange(bond_count * paths):
        bond_index = path_index // paths
        s0 = s0_values[bond_index]
        x0 = x0_values[bond_index]
        rate = rate_values[bond_index]
        spread = spread_values[bond_index]
        sigma = sigma_values[bond_index]
        maturity = maturity_values[bond_index]
        redemption = redemption_values[bond_index]
        call_price = call_values[bond_index]
        put_price = put_values[bond_index]
        put_barrier = put_barriers[bond_index]
        put_window = int(put_windows[bond_index])
        put_years = put_years_values[bond_index]
        redeem_ratio = redeem_ratios[bond_index]
        redeem_window = int(redeem_windows[bond_index])
        redeem_required = int(redeem_required_values[bond_index])

        steps = max(50, int(maturity * 240.0))
        dt = maturity / steps
        put_start_time = max(0.0, maturity - put_years)
        put_start_index = int(put_start_time / dt)
        drift = (rate - 0.5 * sigma * sigma) * dt
        volatility_step = sigma * math.sqrt(dt)

        stock_price = s0
        conversion_price = x0
        put_count = int(initial_put_counts[bond_index])
        redeem_count = int(initial_redeem_counts[bond_index])
        redeem_flags = np.empty(64, dtype=np.int8)
        for flag_index in range(64):
            redeem_flags[flag_index] = initial_redeem_flags[
                bond_index, flag_index
            ]

        state = (
            np.uint64(seed)
            ^ (np.uint64(path_index + 1) * _GOLDEN_GAMMA)
        )
        end_time = maturity
        end_value = 0.0
        active = True

        for step in range(1, steps + 1):
            state, normal = _next_normal(state)
            stock_price *= math.exp(
                drift + volatility_step * normal
            )

            if step <= put_start_index:
                put_count = 0
            elif stock_price < put_barrier * conversion_price:
                put_count += 1
            else:
                put_count = 0

            if put_count >= put_window:
                end_time = step * dt
                end_value = put_price
                active = False
                break

            slot = (step - 1) % redeem_window
            redeem_count -= redeem_flags[slot]
            flag = (
                1
                if stock_price >= redeem_ratio * conversion_price
                else 0
            )
            redeem_flags[slot] = flag
            redeem_count += flag
            if redeem_count >= redeem_required:
                end_time = step * dt
                conversion_value = stock_price * (
                    x0 / conversion_price
                )
                end_value = max(conversion_value, call_price)
                active = False
                break

        if active:
            conversion_value = x0 / conversion_price * stock_price
            end_value = max(conversion_value, redemption)

        output[path_index] = end_value * math.exp(
            -(rate + spread) * end_time
        )

    return output


def _validated_params(
    params: Mapping[str, np.ndarray],
) -> dict[str, np.ndarray]:
    missing = [
        field for field in _REQUIRED_VECTOR_FIELDS if field not in params
    ]
    if "initial_redeem_flags" not in params:
        missing.append("initial_redeem_flags")
    if missing:
        raise ValueError(f"missing ZL CPU parameters: {missing}")

    normalized = {
        field: np.ascontiguousarray(params[field])
        for field in _REQUIRED_VECTOR_FIELDS
    }
    bond_count = len(normalized["S0"])
    if any(len(values) != bond_count for values in normalized.values()):
        raise ValueError("all ZL CPU parameter vectors must have equal length")

    flags = np.ascontiguousarray(
        params["initial_redeem_flags"], dtype=np.int8
    )
    if flags.shape != (bond_count, 64):
        raise ValueError(
            "initial_redeem_flags must have shape (bond_count, 64)"
        )
    normalized["initial_redeem_flags"] = flags

    redeem_windows = normalized["redeem_window"]
    if np.any(redeem_windows < 1) or np.any(redeem_windows > 64):
        raise ValueError("redeem_window must be between 1 and 64")
    if np.any(normalized["put_window"] < 1):
        raise ValueError("put_window must be positive")
    if np.any(normalized["T"] <= 0):
        raise ValueError("maturity T must be positive")
    return normalized


def price_batch_cpu(
    params: Mapping[str, np.ndarray],
    *,
    paths: int = 10_000,
    seed: int = 42,
) -> np.ndarray:
    """Price one valuation-date batch on CPU using deterministic paths."""
    if paths < 1:
        raise ValueError("paths must be positive")
    normalized = _validated_params(params)
    bond_count = len(normalized["S0"])
    if bond_count == 0:
        return np.empty(0, dtype=np.float64)

    path_values = _price_paths_cpu(
        normalized["S0"].astype(np.float64),
        normalized["X0"].astype(np.float64),
        normalized["r"].astype(np.float64),
        normalized["cs"].astype(np.float64),
        normalized["sigma"].astype(np.float64),
        normalized["T"].astype(np.float64),
        normalized["maturity_redem"].astype(np.float64),
        normalized["call_price"].astype(np.float64),
        normalized["put_price"].astype(np.float64),
        normalized["put_barrier"].astype(np.float64),
        normalized["put_window"].astype(np.int32),
        normalized["put_years"].astype(np.float64),
        normalized["redeem_ratio"].astype(np.float64),
        normalized["redeem_window"].astype(np.int32),
        normalized["redeem_required"].astype(np.int32),
        normalized["initial_put_count"].astype(np.int32),
        normalized["initial_redeem_count"].astype(np.int32),
        normalized["initial_redeem_flags"],
        int(paths),
        np.uint64(seed),
    )
    return path_values.reshape(bond_count, paths).mean(axis=1)
