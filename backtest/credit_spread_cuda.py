"""Batched implied-credit-spread solvers for ragged contractual cash flows."""

from __future__ import annotations

import math
from typing import Final

import numpy as np

try:
    from numba import cuda
except (ImportError, OSError):
    cuda = None


STATUS_OK: Final = 0
STATUS_ABOVE_RISK_FREE: Final = 1
STATUS_SPREAD_TOO_HIGH: Final = 2
STATUS_INVALID_INPUT: Final = 3

DEFAULT_MAX_SPREAD: Final = 5.0
DEFAULT_BISECTION_ITERATIONS: Final = 64


def cuda_is_available() -> bool:
    """Return whether Numba can access a CUDA device in this process."""

    if cuda is None:
        return False
    try:
        return bool(cuda.is_available())
    except Exception:
        return False


def _validated_inputs(
    observed_values: np.ndarray,
    cashflow_offsets: np.ndarray,
    cashflow_times: np.ndarray,
    cashflow_amounts: np.ndarray,
    risk_free_rates: np.ndarray,
) -> tuple[np.ndarray, ...]:
    observed = np.ascontiguousarray(observed_values, dtype=np.float64)
    offsets = np.ascontiguousarray(cashflow_offsets, dtype=np.int64)
    times = np.ascontiguousarray(cashflow_times, dtype=np.float64)
    amounts = np.ascontiguousarray(cashflow_amounts, dtype=np.float64)
    rates = np.ascontiguousarray(risk_free_rates, dtype=np.float64)

    if observed.ndim != 1:
        raise ValueError("observed_values must be one-dimensional")
    if offsets.ndim != 1 or len(offsets) != len(observed) + 1:
        raise ValueError("cashflow_offsets must have one entry per problem plus one")
    if offsets[0] != 0 or offsets[-1] != len(times):
        raise ValueError("cashflow_offsets do not span the cash-flow arrays")
    if np.any(offsets[1:] < offsets[:-1]):
        raise ValueError("cashflow_offsets must be non-decreasing")
    if len(times) != len(amounts) or len(times) != len(rates):
        raise ValueError("cash-flow arrays must have equal lengths")
    return observed, offsets, times, amounts, rates


def solve_credit_spreads_bisection(
    observed_values: np.ndarray,
    cashflow_offsets: np.ndarray,
    cashflow_times: np.ndarray,
    cashflow_amounts: np.ndarray,
    risk_free_rates: np.ndarray,
    *,
    max_spread: float = DEFAULT_MAX_SPREAD,
    iterations: int = DEFAULT_BISECTION_ITERATIONS,
) -> tuple[np.ndarray, np.ndarray]:
    """Solve independent ragged cash-flow problems on the CPU."""

    observed, offsets, times, amounts, rates = _validated_inputs(
        observed_values,
        cashflow_offsets,
        cashflow_times,
        cashflow_amounts,
        risk_free_rates,
    )
    spreads = np.full(len(observed), np.nan, dtype=np.float64)
    statuses = np.full(len(observed), STATUS_INVALID_INPUT, dtype=np.int8)

    for problem in range(len(observed)):
        start = int(offsets[problem])
        stop = int(offsets[problem + 1])
        value = float(observed[problem])
        problem_times = times[start:stop]
        problem_amounts = amounts[start:stop]
        problem_rates = rates[start:stop]
        if (
            not np.isfinite(value)
            or value <= 0
            or start == stop
            or not np.isfinite(problem_times).all()
            or not np.isfinite(problem_amounts).all()
            or not np.isfinite(problem_rates).all()
            or np.any(problem_times <= 0)
            or np.any(problem_amounts <= 0)
        ):
            continue

        risk_free_value = float(
            np.sum(problem_amounts * np.exp(-problem_rates * problem_times))
        )
        tolerance = max(1e-8, risk_free_value * 1e-8)
        if value > risk_free_value + tolerance:
            statuses[problem] = STATUS_ABOVE_RISK_FREE
            continue
        if abs(value - risk_free_value) <= tolerance:
            spreads[problem] = 0.0
            statuses[problem] = STATUS_OK
            continue
        max_spread_value = float(
            np.sum(
                problem_amounts
                * np.exp(-(problem_rates + float(max_spread)) * problem_times)
            )
        )
        if max_spread_value > value:
            statuses[problem] = STATUS_SPREAD_TOO_HIGH
            continue

        lower = 0.0
        upper = float(max_spread)
        for _ in range(int(iterations)):
            midpoint = (lower + upper) * 0.5
            present_value = float(
                np.sum(
                    problem_amounts
                    * np.exp(-(problem_rates + midpoint) * problem_times)
                )
            )
            if present_value > value:
                lower = midpoint
            else:
                upper = midpoint
        spreads[problem] = (lower + upper) * 0.5
        statuses[problem] = STATUS_OK
    return spreads, statuses


if cuda is not None:  # pragma: no cover - device code is verified on the GPU

    @cuda.jit
    def _credit_spread_kernel(
        observed,
        offsets,
        times,
        amounts,
        rates,
        max_spread,
        iterations,
        spreads,
        statuses,
    ):
        problem = cuda.grid(1)
        if problem >= observed.size:
            return

        start = offsets[problem]
        stop = offsets[problem + 1]
        value = observed[problem]
        if not math.isfinite(value) or value <= 0.0 or start == stop:
            statuses[problem] = STATUS_INVALID_INPUT
            return

        risk_free_value = 0.0
        max_spread_value = 0.0
        for cashflow in range(start, stop):
            time = times[cashflow]
            amount = amounts[cashflow]
            rate = rates[cashflow]
            if (
                not math.isfinite(time)
                or not math.isfinite(amount)
                or not math.isfinite(rate)
                or time <= 0.0
                or amount <= 0.0
            ):
                statuses[problem] = STATUS_INVALID_INPUT
                return
            risk_free_value += amount * math.exp(-rate * time)
            max_spread_value += amount * math.exp(-(rate + max_spread) * time)

        tolerance = max(1e-8, risk_free_value * 1e-8)
        if value > risk_free_value + tolerance:
            statuses[problem] = STATUS_ABOVE_RISK_FREE
            return
        if abs(value - risk_free_value) <= tolerance:
            spreads[problem] = 0.0
            statuses[problem] = STATUS_OK
            return
        if max_spread_value > value:
            statuses[problem] = STATUS_SPREAD_TOO_HIGH
            return

        lower = 0.0
        upper = max_spread
        for _ in range(iterations):
            midpoint = (lower + upper) * 0.5
            present_value = 0.0
            for cashflow in range(start, stop):
                present_value += amounts[cashflow] * math.exp(
                    -(rates[cashflow] + midpoint) * times[cashflow]
                )
            if present_value > value:
                lower = midpoint
            else:
                upper = midpoint
        spreads[problem] = (lower + upper) * 0.5
        statuses[problem] = STATUS_OK


def solve_credit_spreads_cuda(
    observed_values: np.ndarray,
    cashflow_offsets: np.ndarray,
    cashflow_times: np.ndarray,
    cashflow_amounts: np.ndarray,
    risk_free_rates: np.ndarray,
    *,
    max_spread: float = DEFAULT_MAX_SPREAD,
    iterations: int = DEFAULT_BISECTION_ITERATIONS,
) -> tuple[np.ndarray, np.ndarray]:
    """Solve independent ragged cash-flow problems with one CUDA thread each."""

    if not cuda_is_available():
        raise RuntimeError("CUDA is unavailable")
    observed, offsets, times, amounts, rates = _validated_inputs(
        observed_values,
        cashflow_offsets,
        cashflow_times,
        cashflow_amounts,
        risk_free_rates,
    )
    if len(observed) == 0:
        return (
            np.empty(0, dtype=np.float64),
            np.empty(0, dtype=np.int8),
        )

    device_observed = cuda.to_device(observed)
    device_offsets = cuda.to_device(offsets)
    device_times = cuda.to_device(times)
    device_amounts = cuda.to_device(amounts)
    device_rates = cuda.to_device(rates)
    device_spreads = cuda.to_device(
        np.full(len(observed), np.nan, dtype=np.float64)
    )
    device_statuses = cuda.to_device(
        np.full(len(observed), STATUS_INVALID_INPUT, dtype=np.int8)
    )

    threads_per_block = 128
    blocks = (len(observed) + threads_per_block - 1) // threads_per_block
    _credit_spread_kernel[blocks, threads_per_block](
        device_observed,
        device_offsets,
        device_times,
        device_amounts,
        device_rates,
        float(max_spread),
        int(iterations),
        device_spreads,
        device_statuses,
    )
    cuda.synchronize()
    return device_spreads.copy_to_host(), device_statuses.copy_to_host()
