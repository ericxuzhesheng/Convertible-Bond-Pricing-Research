import numpy as np
import pytest

from backtest.credit_spread_cuda import (
    STATUS_ABOVE_RISK_FREE,
    STATUS_OK,
    STATUS_SPREAD_TOO_HIGH,
    cuda_is_available,
    solve_credit_spreads_bisection,
    solve_credit_spreads_cuda,
)
from backtest.market_data_contracts import implied_credit_spread


def _ragged_problems() -> tuple[np.ndarray, ...]:
    observed = np.asarray([95.0, 101.0, 120.0, 1.0], dtype=np.float64)
    offsets = np.asarray([0, 1, 3, 4, 5], dtype=np.int64)
    times = np.asarray([1.0, 0.5, 1.5, 1.0, 1.0], dtype=np.float64)
    amounts = np.asarray([100.0, 2.0, 102.0, 100.0, 100.0], dtype=np.float64)
    rates = np.asarray([0.02, 0.02, 0.025, 0.02, 0.02], dtype=np.float64)
    return observed, offsets, times, amounts, rates


def test_batched_bisection_matches_brentq_for_ragged_cashflows() -> None:
    observed, offsets, times, amounts, rates = _ragged_problems()

    spreads, statuses = solve_credit_spreads_bisection(
        observed[:2],
        offsets[:3],
        times[:3],
        amounts[:3],
        rates[:3],
    )

    expected = [
        implied_credit_spread(
            observed_bond_value=observed[index],
            cashflow_times=times[offsets[index] : offsets[index + 1]],
            cashflow_amounts=amounts[offsets[index] : offsets[index + 1]],
            risk_free_rates=rates[offsets[index] : offsets[index + 1]],
        )
        for index in range(2)
    ]
    assert statuses.tolist() == [STATUS_OK, STATUS_OK]
    assert spreads == pytest.approx(expected, abs=1e-10)


def test_batched_solver_preserves_existing_boundary_rules() -> None:
    observed, offsets, times, amounts, rates = _ragged_problems()

    spreads, statuses = solve_credit_spreads_bisection(
        observed,
        offsets,
        times,
        amounts,
        rates,
    )

    assert np.isfinite(spreads[:2]).all()
    assert np.isnan(spreads[2])
    assert np.isnan(spreads[3])
    assert statuses.tolist() == [
        STATUS_OK,
        STATUS_OK,
        STATUS_ABOVE_RISK_FREE,
        STATUS_SPREAD_TOO_HIGH,
    ]


@pytest.mark.skipif(not cuda_is_available(), reason="CUDA device unavailable")
def test_cuda_solver_matches_cpu_bisection() -> None:
    observed, offsets, times, amounts, rates = _ragged_problems()

    expected_spreads, expected_statuses = solve_credit_spreads_bisection(
        observed,
        offsets,
        times,
        amounts,
        rates,
    )
    actual_spreads, actual_statuses = solve_credit_spreads_cuda(
        observed,
        offsets,
        times,
        amounts,
        rates,
    )

    assert actual_statuses.tolist() == expected_statuses.tolist()
    assert actual_spreads[:2] == pytest.approx(expected_spreads[:2], abs=1e-10)
    assert np.isnan(actual_spreads[2:]).all()
