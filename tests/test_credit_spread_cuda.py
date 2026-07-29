import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

from credit_spread_cuda import (  # noqa: E402
    STATUS_ABOVE_RISK_FREE,
    STATUS_OK,
    STATUS_SPREAD_TOO_HIGH,
    cuda_is_available,
    solve_credit_spreads_bisection,
    solve_credit_spreads_cuda,
)
from market_data_contracts import (  # noqa: E402
    build_implied_credit_spread_matrix,
    implied_credit_spread,
)


def _ragged_problems() -> tuple[np.ndarray, ...]:
    observed = np.asarray([95.0, 98.0, 120.0, 0.1], dtype=np.float64)
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


def test_batched_solver_handles_zero_spread_and_invalid_problem() -> None:
    observed = np.asarray([100.0 * np.exp(-0.02), np.nan])
    offsets = np.asarray([0, 1, 2])
    times = np.asarray([1.0, 1.0])
    amounts = np.asarray([100.0, 100.0])
    rates = np.asarray([0.02, 0.02])

    spreads, statuses = solve_credit_spreads_bisection(
        observed,
        offsets,
        times,
        amounts,
        rates,
    )

    assert spreads[0] == 0.0
    assert statuses[0] == STATUS_OK
    assert np.isnan(spreads[1])


@pytest.mark.parametrize(
    ("offsets", "times", "amounts", "rates", "message"),
    [
        ([0], [], [], [], "one entry per problem"),
        ([1, 1], [], [], [], "do not span"),
        ([0, 2], [1.0], [100.0], [0.02], "do not span"),
        ([0, 1], [1.0], [], [0.02], "equal lengths"),
    ],
)
def test_batched_solver_rejects_malformed_ragged_inputs(
    offsets: list[int],
    times: list[float],
    amounts: list[float],
    rates: list[float],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        solve_credit_spreads_bisection(
            np.asarray([95.0]),
            np.asarray(offsets),
            np.asarray(times),
            np.asarray(amounts),
            np.asarray(rates),
        )


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


def _matrix_problem() -> tuple[pd.DataFrame, ...]:
    dates = pd.to_datetime(["2024-01-02", "2024-06-03"])
    maturity = pd.DataFrame({"123001.SZ": [1.0, 0.5]}, index=dates)
    observed = pd.DataFrame({"123001.SZ": [95.0, 98.0]}, index=dates)
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "par_value": [100.0],
            "interest_freq": [1],
            "value_date": ["20240102"],
            "maturity_date": ["20250102"],
            "maturity_call_price": [100.0],
            "rate_clause": ["20240102-20250101,票面利率:0.00%"],
        }
    )
    curve = pd.DataFrame(
        {1.0: [0.02, 0.021], 3.0: [0.025, 0.026]},
        index=dates,
    )
    return observed, maturity, basic, curve


def test_matrix_builder_accepts_explicit_cpu_backend() -> None:
    observed, maturity, basic, curve = _matrix_problem()

    result = build_implied_credit_spread_matrix(
        observed_bond_value=observed,
        maturity=maturity,
        cb_basic=basic,
        government_curve=curve,
        backend="cpu",
    )

    assert result.shape == maturity.shape
    assert result.notna().all().all()


@pytest.mark.skipif(not cuda_is_available(), reason="CUDA device unavailable")
def test_matrix_builder_cuda_matches_cpu_backend() -> None:
    observed, maturity, basic, curve = _matrix_problem()

    cpu_result = build_implied_credit_spread_matrix(
        observed_bond_value=observed,
        maturity=maturity,
        cb_basic=basic,
        government_curve=curve,
        backend="cpu",
    )
    cuda_result = build_implied_credit_spread_matrix(
        observed_bond_value=observed,
        maturity=maturity,
        cb_basic=basic,
        government_curve=curve,
        backend="cuda",
    )

    assert cuda_result.to_numpy() == pytest.approx(
        cpu_result.to_numpy(),
        abs=1e-10,
    )
