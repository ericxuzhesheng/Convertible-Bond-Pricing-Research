from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

from market_data_contracts import (  # noqa: E402
    DataContractError,
    ZL_HISTORICAL_MIN_COVERAGE,
    ZL_MIN_COVERAGE_ENFORCED_FROM,
    ZL_MIN_PRICING_COVERAGE,
    build_active_market_mask,
    build_credit_spread_matrix,
    build_implied_credit_spread_matrix,
    build_conversion_price_matrix,
    build_point_in_time_balance_matrix,
    build_point_in_time_rating_matrix,
    build_contractual_par_matrix,
    build_observed_volatility,
    build_risk_free_rate_matrix,
    build_clause_history_state,
    calculate_accrued_interest,
    extract_clause_terms,
    implied_credit_spread,
    load_rebuildable_matrix_cache,
    merge_incremental_history,
    observed_average_risk_free_rate,
    select_completed_weekly_dates,
    select_dates_after_checkpoint,
    select_input_refresh_dates,
    select_pending_calculation_dates,
    validate_pricing_coverage,
    validate_balance_wan_units,
    validate_observed_source_coverage,
    validate_stock_market_value_wan_units,
    interpolate_observed_yield_curve,
    parse_coupon_schedule,
    point_in_time_fundamental_matrix,
)


def test_merge_incremental_history_preserves_old_rows_and_adds_new_columns() -> None:
    old = pd.DataFrame(
        {"A": [1.0, 2.0]},
        index=pd.to_datetime(["2026-07-17", "2026-07-24"]),
    )
    increment = pd.DataFrame(
        {"A": [3.0], "B": [4.0]},
        index=pd.to_datetime(["2026-07-31"]),
    )

    merged = merge_incremental_history(old, increment)

    assert merged.index.tolist() == pd.to_datetime(
        ["2026-07-17", "2026-07-24", "2026-07-31"]
    ).tolist()
    assert merged.columns.tolist() == ["A", "B"]
    assert merged.loc[pd.Timestamp("2026-07-24"), "A"] == 2.0
    assert pd.isna(merged.loc[pd.Timestamp("2026-07-24"), "B"])
    assert merged.loc[pd.Timestamp("2026-07-31"), "B"] == 4.0


def test_merge_incremental_history_replaces_only_increment_rows() -> None:
    old = pd.DataFrame(
        {"A": [1.0, 2.0]},
        index=pd.to_datetime(["2026-07-17", "2026-07-24"]),
    )
    increment = pd.DataFrame(
        {"A": [20.0]},
        index=pd.to_datetime(["2026-07-24"]),
    )

    merged = merge_incremental_history(old, increment)

    assert merged.loc[pd.Timestamp("2026-07-17"), "A"] == 1.0
    assert merged.loc[pd.Timestamp("2026-07-24"), "A"] == 20.0


def test_pending_calculation_dates_align_daily_mask_to_weekly_dates() -> None:
    daily_dates = pd.date_range("2024-01-01", periods=10, freq="D")
    weekly_dates = pd.DatetimeIndex([daily_dates[4], daily_dates[9]])
    pending_mask = pd.DataFrame(
        {
            "123001.SZ": [
                False,
                False,
                False,
                False,
                True,
                False,
                False,
                False,
                False,
                False,
            ]
        },
        index=daily_dates,
    )

    result = select_pending_calculation_dates(
        calculation_dates=weekly_dates,
        pending_mask=pending_mask,
    )

    assert result.equals(pd.DatetimeIndex([daily_dates[4]]))


def test_resume_dates_start_strictly_after_checkpoint_cutoff() -> None:
    dates = pd.date_range("2026-06-05", periods=4, freq="7D")

    result = select_dates_after_checkpoint(
        calculation_dates=dates,
        checkpoint_cutoff=dates[1],
    )

    assert result.equals(pd.DatetimeIndex(dates[2:]))


def test_conversion_price_matrix_uses_effective_historical_changes() -> None:
    dates = pd.to_datetime(["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"])
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "first_conv_price": [10.0],
            # This is the current price and must not contaminate history.
            "conv_price": [6.0],
        }
    )
    changes = pd.DataFrame(
        {
            "ts_code": ["123001.SZ", "123001.SZ"],
            "change_date": ["20190101", "20190103"],
            "convert_price_initial": [10.0, 10.0],
            "convertprice_bef": [np.nan, 10.0],
            "convertprice_aft": [np.nan, 8.0],
        }
    )

    result = build_conversion_price_matrix(
        dates=dates,
        bonds=["123001.SZ"],
        cb_basic=basic,
        change_events=changes,
    )

    assert result["123001.SZ"].tolist() == [10.0, 10.0, 8.0, 8.0]


def test_conversion_price_matrix_rejects_missing_initial_history() -> None:
    dates = pd.to_datetime(["2019-01-01"])
    basic = pd.DataFrame({"ts_code": ["123001.SZ"], "conv_price": [6.0]})

    with pytest.raises(DataContractError, match="initial conversion price"):
        build_conversion_price_matrix(
            dates=dates,
            bonds=["123001.SZ"],
            cb_basic=basic,
            change_events=pd.DataFrame(),
        )


def test_clause_parser_extracts_real_trigger_terms_and_maturity_redemption() -> None:
    resale = (
        "最后两个计息年度，如果公司股票在任意连续三十个交易日的收盘价格"
        "低于当期转股价的70.00%，持有人有权按面值加当期应计利息回售。"
    )
    redeem = (
        "期满后五个交易日内按票面面值上浮8%(含最后一期利息)赎回。"
        "转股期内，任意连续三十个交易日中至少十五个交易日的收盘价格"
        "不低于当期转股价格的130.00%，或未转股余额不足3,000.00万元。"
    )

    terms = extract_clause_terms(resale, redeem, par_value=100.0)

    assert terms.put_trigger_ratio == pytest.approx(0.70)
    assert terms.put_window_days == 30
    assert terms.put_eligible_years == 2
    assert terms.redeem_trigger_ratio == pytest.approx(1.30)
    assert terms.redeem_window_days == 30
    assert terms.redeem_required_days == 15
    assert terms.redeem_balance_threshold_wan == pytest.approx(3000.0)
    assert terms.maturity_redemption_price == pytest.approx(108.0)


def test_clause_parser_handles_absolute_maturity_ratio_and_optional_you() -> None:
    redeem = (
        "公司将以本次发行可转债的票面面值108%(含最后一期年度利息)的价格赎回。"
        "如果公司股票连续三十个交易日中至少有十五个交易日的收盘价格"
        "不低于当期转股价格的130%。"
    )

    terms = extract_clause_terms("", redeem, par_value=100.0)

    assert terms.redeem_window_days == 30
    assert terms.redeem_required_days == 15
    assert terms.maturity_redemption_price == pytest.approx(108.0)


def test_coupon_schedule_and_accrual_use_contractual_rate_period() -> None:
    schedule = parse_coupon_schedule(
        "20200101-20201231,票面利率:0.30%;"
        "20210101-20211231,票面利率:0.50%"
    )

    assert schedule.loc[pd.Timestamp("2020-06-30")] == pytest.approx(0.003)
    assert schedule.loc[pd.Timestamp("2021-06-30")] == pytest.approx(0.005)
    assert calculate_accrued_interest(
        as_of=pd.Timestamp("2021-07-02"),
        value_date=pd.Timestamp("2020-01-01"),
        par_value=100.0,
        coupon_schedule=schedule,
    ) == pytest.approx(100.0 * 0.005 * 182 / 365)


def test_yield_curve_has_no_silent_two_percent_fallback() -> None:
    curve = pd.DataFrame(
        {1.0: [0.018], 3.0: [0.021], 5.0: [0.024]},
        index=pd.to_datetime(["2024-01-02"]),
    )

    assert interpolate_observed_yield_curve(
        curve, pd.Timestamp("2024-01-02"), 2.0
    ) == pytest.approx(0.0195)

    with pytest.raises(DataContractError, match="yield curve"):
        interpolate_observed_yield_curve(
            curve, pd.Timestamp("2024-01-01"), 2.0
        )


def test_fundamentals_become_available_on_announcement_date_not_period_end() -> None:
    dates = pd.to_datetime(["2024-03-31", "2024-04-29", "2024-04-30"])
    events = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "end_date": ["20240331"],
            "ann_date": ["20240430"],
            "bps": [12.5],
        }
    )

    matrix = point_in_time_fundamental_matrix(
        events=events,
        dates=dates,
        securities=["000001.SZ"],
        value_column="bps",
    )

    assert pd.isna(matrix.loc[pd.Timestamp("2024-03-31"), "000001.SZ"])
    assert pd.isna(matrix.loc[pd.Timestamp("2024-04-29"), "000001.SZ"])
    assert matrix.loc[pd.Timestamp("2024-04-30"), "000001.SZ"] == pytest.approx(12.5)


def test_active_market_mask_requires_actual_market_and_model_inputs() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    price = pd.DataFrame({"123001.SZ": [100.0, np.nan]}, index=dates)
    conversion_value = pd.DataFrame({"123001.SZ": [90.0, 91.0]}, index=dates)
    maturity = pd.DataFrame({"123001.SZ": [2.0, 2.0]}, index=dates)
    volatility = pd.DataFrame({"123001.SZ": [0.25, 0.25]}, index=dates)
    risk_free = pd.DataFrame({"123001.SZ": [0.02, 0.02]}, index=dates)

    mask = build_active_market_mask(
        price=price,
        required_inputs=[conversion_value, maturity, volatility, risk_free],
    )

    assert mask["123001.SZ"].tolist() == [True, False]


def test_balance_uses_only_published_cb_share_observations() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-09", "2024-01-10"])
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "issue_size": [1_000_000_000.0],
            "list_date": ["20240102"],
        }
    )
    share_events = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "end_date": ["20240105"],
            "publish_date": ["20240110"],
            "remain_size": [800_000_000.0],
        }
    )

    matrix = build_point_in_time_balance_matrix(
        dates=dates,
        bonds=["123001.SZ"],
        cb_basic=basic,
        share_events=share_events,
    )

    assert matrix["123001.SZ"].tolist() == [100_000.0, 100_000.0, 80_000.0]


def test_rating_uses_publication_date_not_rating_date() -> None:
    dates = pd.to_datetime(["2024-06-14", "2024-06-19", "2024-06-20"])
    events = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "rating_date": ["20240614"],
            "ann_date": ["20240620"],
            "rating": ["AA-"],
        }
    )

    matrix = build_point_in_time_rating_matrix(
        dates=dates,
        bonds=["123001.SZ"],
        rating_events=events,
    )

    assert pd.isna(matrix.loc[pd.Timestamp("2024-06-14"), "123001.SZ"])
    assert pd.isna(matrix.loc[pd.Timestamp("2024-06-19"), "123001.SZ"])
    assert matrix.loc[pd.Timestamp("2024-06-20"), "123001.SZ"] == "AA-"


def test_credit_spread_comes_from_observed_rating_curve() -> None:
    dates = pd.to_datetime(["2024-01-02"])
    maturity = pd.DataFrame(
        {"123001.SZ": [2.0]},
        index=dates,
    )
    ratings = pd.DataFrame(
        {"123001.SZ": ["AAA"]},
        index=dates,
    )
    government = pd.DataFrame(
        {1.0: [0.018], 3.0: [0.022]},
        index=dates,
    )
    corporate = {
        "AAA": pd.DataFrame(
            {1.0: [0.025], 3.0: [0.030]},
            index=dates,
        )
    }

    spread = build_credit_spread_matrix(
        maturity=maturity,
        ratings=ratings,
        government_curve=government,
        corporate_curves=corporate,
    )

    expected = ((0.025 + 0.030) / 2) - ((0.018 + 0.022) / 2)
    assert spread.loc[pd.Timestamp("2024-01-02"), "123001.SZ"] == pytest.approx(
        expected
    )


def test_credit_spread_rejects_missing_rating_curve() -> None:
    date = pd.Timestamp("2024-01-02")
    maturity = pd.DataFrame({"123001.SZ": [2.0]}, index=[date])
    ratings = pd.DataFrame({"123001.SZ": ["AA-"]}, index=[date])
    government = pd.DataFrame({1.0: [0.018], 3.0: [0.022]}, index=[date])

    with pytest.raises(DataContractError, match="credit curve"):
        build_credit_spread_matrix(
            maturity=maturity,
            ratings=ratings,
            government_curve=government,
            corporate_curves={},
        )


def test_implied_credit_spread_is_solved_from_observed_bond_value() -> None:
    spread = implied_credit_spread(
        observed_bond_value=95.0,
        cashflow_times=np.array([1.0]),
        cashflow_amounts=np.array([100.0]),
        risk_free_rates=np.array([0.02]),
    )

    assert spread == pytest.approx(-np.log(0.95) - 0.02)


def test_implied_credit_spread_rejects_inconsistent_bond_value() -> None:
    with pytest.raises(DataContractError, match="bond value"):
        implied_credit_spread(
            observed_bond_value=110.0,
            cashflow_times=np.array([1.0]),
            cashflow_amounts=np.array([100.0]),
            risk_free_rates=np.array([0.02]),
        )


def test_contractual_par_matrix_has_no_one_hundred_fallback() -> None:
    dates = pd.to_datetime(["2024-01-02"])
    basic = pd.DataFrame(
        {"ts_code": ["123001.SZ"], "par_value": [100.0]}
    )

    result = build_contractual_par_matrix(
        dates=dates,
        bonds=["123001.SZ"],
        cb_basic=basic,
    )
    assert result.iloc[0, 0] == pytest.approx(100.0)

    with pytest.raises(DataContractError, match="par value"):
        build_contractual_par_matrix(
            dates=dates,
            bonds=["123002.SZ"],
            cb_basic=basic,
        )


def test_observed_volatility_stays_missing_until_minimum_history() -> None:
    source_dates = pd.bdate_range("2024-01-01", periods=65)
    close = pd.Series(
        100.0 * np.exp(np.linspace(0.0, 0.1, len(source_dates))),
        index=source_dates,
    )

    volatility = build_observed_volatility(
        adjusted_close=close,
        target_dates=source_dates,
        window=60,
        min_observations=60,
    )

    assert volatility.iloc[:59].isna().all()
    assert np.isfinite(volatility.iloc[59])


def test_risk_free_matrix_rejects_dates_before_actual_curve() -> None:
    dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
    maturity = pd.DataFrame({"123001.SZ": [2.0, 2.0]}, index=dates)
    curve = pd.DataFrame(
        {1.0: [0.018], 3.0: [0.022]},
        index=pd.to_datetime(["2024-01-02"]),
    )

    with pytest.raises(DataContractError, match="yield curve"):
        build_risk_free_rate_matrix(curve=curve, maturity=maturity)


def test_risk_free_matrix_vectorizes_the_observed_curve_interpolation() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-05"])
    maturity = pd.DataFrame(
        {
            "short": [0.5, 1.0],
            "middle": [2.0, 4.0],
            "long": [8.0, np.nan],
            "invalid": [-1.0, 0.0],
        },
        index=dates,
    )
    curve = pd.DataFrame(
        {
            1.0: [0.018, 0.019],
            3.0: [0.022, 0.023],
            5.0: [0.026, 0.027],
        },
        index=pd.to_datetime(["2024-01-02", "2024-01-04"]),
    )

    result = build_risk_free_rate_matrix(curve=curve, maturity=maturity)

    for date in dates:
        for bond in ["short", "middle", "long"]:
            term = maturity.at[date, bond]
            if pd.isna(term):
                assert pd.isna(result.at[date, bond])
            else:
                expected = interpolate_observed_yield_curve(curve, date, term)
                assert result.at[date, bond] == pytest.approx(expected)
    assert result["invalid"].isna().all()


def test_implied_spread_matrix_uses_contractual_cashflows() -> None:
    date = pd.Timestamp("2024-01-02")
    maturity = pd.DataFrame({"123001.SZ": [1.0]}, index=[date])
    observed_floor = pd.DataFrame({"123001.SZ": [95.0]}, index=[date])
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "par_value": [100.0],
            "interest_freq": [1],
            "value_date": ["20240102"],
            "maturity_date": ["20250102"],
            "maturity_call_price": [100.0],
            "rate_clause": [
                "20240102-20250101,票面利率:0.00%"
            ],
        }
    )
    curve = pd.DataFrame({1.0: [0.02]}, index=[date])

    spread = build_implied_credit_spread_matrix(
        observed_bond_value=observed_floor,
        maturity=maturity,
        cb_basic=basic,
        government_curve=curve,
    )

    actual_365_time = 366.0 / 365.0
    assert spread.loc[date, "123001.SZ"] == pytest.approx(
        -np.log(0.95) / actual_365_time - 0.02
    )


def test_clause_history_state_inherits_observed_prevaluation_days() -> None:
    dates = pd.bdate_range("2024-01-01", periods=35)
    conversion_value = pd.Series(
        [60.0] * 30 + [140.0] * 5,
        index=dates,
    )

    state = build_clause_history_state(
        conversion_value=conversion_value,
        valuation_date=dates[-1],
        par_value=100.0,
        put_trigger_ratio=0.70,
        put_eligible_start=dates[0],
        redeem_trigger_ratio=1.30,
        redeem_window_days=30,
    )

    assert state.put_consecutive_days == 0
    assert state.redeem_count == 5
    assert state.redeem_flags.sum() == 5
    assert len(state.redeem_flags) == 64


def test_clause_history_state_does_not_carry_put_days_before_eligibility() -> None:
    dates = pd.bdate_range("2024-01-01", periods=35)
    conversion_value = pd.Series(60.0, index=dates)
    eligible_start = dates[-3]

    state = build_clause_history_state(
        conversion_value=conversion_value,
        valuation_date=dates[-1],
        par_value=100.0,
        put_trigger_ratio=0.70,
        put_eligible_start=eligible_start,
        redeem_trigger_ratio=1.30,
        redeem_window_days=30,
    )

    assert state.put_consecutive_days == 3


def test_model_rebuild_preserves_observed_input_cache_unless_refresh_requested(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "legacy_volatility.csv"
    pd.DataFrame(
        {"123001.SZ": [0.40]},
        index=[pd.Timestamp("2024-01-02")],
    ).to_csv(cache_path)

    loaded = load_rebuildable_matrix_cache(
        path=cache_path,
        index=pd.DatetimeIndex(["2024-01-02"]),
        columns=pd.Index(["123001.SZ"]),
        refresh_cache=False,
    )

    assert loaded.iloc[0, 0] == pytest.approx(0.40)

    refreshed = load_rebuildable_matrix_cache(
        path=cache_path,
        index=pd.DatetimeIndex(["2024-01-02"]),
        columns=pd.Index(["123001.SZ"]),
        refresh_cache=True,
    )
    assert refreshed.isna().all().all()


def test_completed_weekly_dates_exclude_partial_current_week() -> None:
    dates = pd.to_datetime(
        ["2024-01-05", "2024-01-12", "2024-01-15", "2024-01-16"]
    )

    selected = select_completed_weekly_dates(
        dates,
        as_of=pd.Timestamp("2024-01-16"),
    )

    assert selected.tolist() == [
        pd.Timestamp("2024-01-05"),
        pd.Timestamp("2024-01-12"),
    ]


def test_model_rebuild_reuses_inputs_unless_refresh_is_explicit() -> None:
    all_dates = pd.date_range("2024-01-01", periods=5)
    coverage_dates = pd.DatetimeIndex([all_dates[-1]])

    assert select_input_refresh_dates(
        all_dates=all_dates,
        coverage_dates=coverage_dates,
        refresh_cache=False,
    ).equals(coverage_dates)
    assert select_input_refresh_dates(
        all_dates=all_dates,
        coverage_dates=coverage_dates,
        refresh_cache=True,
    ).equals(all_dates)


def test_completed_weekly_dates_wait_for_friday_market_close() -> None:
    dates = pd.to_datetime(["2026-07-17", "2026-07-24"])

    before_close = select_completed_weekly_dates(
        dates,
        as_of=pd.Timestamp("2026-07-24 12:00:00"),
    )
    after_close = select_completed_weekly_dates(
        dates,
        as_of=pd.Timestamp("2026-07-24 18:00:00"),
    )

    assert before_close.tolist() == [pd.Timestamp("2026-07-17")]
    assert after_close.tolist() == [
        pd.Timestamp("2026-07-17"),
        pd.Timestamp("2026-07-24"),
    ]


def test_completed_weekly_dates_convert_utc_to_china_market_day() -> None:
    dates = pd.DatetimeIndex(["2026-07-23 16:30:00"], tz="UTC")

    selected = select_completed_weekly_dates(
        dates,
        as_of=pd.Timestamp("2026-07-24 18:00:00"),
    )

    assert selected.tz is None
    assert selected.tolist() == [pd.Timestamp("2026-07-24")]


def test_pricing_coverage_fails_closed_on_tiny_weekly_sample() -> None:
    date = pd.Timestamp("2024-01-12")
    market = pd.DataFrame(
        {"A": [100.0], "B": [101.0], "C": [102.0]},
        index=[date],
    )
    model = pd.DataFrame(
        {"A": [110.0], "B": [np.nan], "C": [np.nan]},
        index=[date],
    )

    with pytest.raises(DataContractError, match="coverage"):
        validate_pricing_coverage(
            market_price=market,
            model_price=model,
            dates=[date],
            min_coverage=0.90,
            min_count=2,
            label="weekly test",
        )


def test_pricing_coverage_accepts_complete_market_smaller_than_count_floor() -> None:
    date = pd.Timestamp("2017-01-06")
    market = pd.DataFrame(
        {f"B{i:02d}": [100.0 + i] for i in range(16)},
        index=[date],
    )

    validate_pricing_coverage(
        market_price=market,
        model_price=market.copy(),
        dates=[date],
        min_coverage=0.98,
        min_count=20,
        label="early weekly universe",
        min_count_enforced_from=pd.Timestamp("2017-06-30"),
    )


def test_observed_coverage_accepts_complete_market_smaller_than_count_floor() -> None:
    date = pd.Timestamp("2017-01-06")
    market = pd.DataFrame(
        {f"B{i:02d}": [100.0 + i] for i in range(16)},
        index=[date],
    )
    ratings = pd.DataFrame(
        {column: ["AA"] for column in market.columns},
        index=[date],
    )

    validate_observed_source_coverage(
        market_price=market,
        source=ratings,
        dates=[date],
        min_coverage=0.98,
        min_count=20,
        label="early rating source",
        min_count_enforced_from=pd.Timestamp("2017-06-30"),
    )


def test_pricing_coverage_rejects_modern_market_smaller_than_count_floor() -> None:
    date = pd.Timestamp("2024-01-12")
    market = pd.DataFrame(
        {f"B{i:02d}": [100.0 + i] for i in range(16)},
        index=[date],
    )

    with pytest.raises(DataContractError, match="16/16"):
        validate_pricing_coverage(
            market_price=market,
            model_price=market.copy(),
            dates=[date],
            min_coverage=0.98,
            min_count=20,
            label="truncated modern universe",
            min_count_enforced_from=pd.Timestamp("2017-06-30"),
        )


def test_text_source_coverage_fails_closed_on_missing_ratings() -> None:
    date = pd.Timestamp("2024-01-12")
    market = pd.DataFrame(
        {"A": [100.0], "B": [101.0], "C": [102.0]},
        index=[date],
    )
    ratings = pd.DataFrame(
        {"A": ["AA"], "B": [None], "C": [""]},
        index=[date],
    )

    with pytest.raises(DataContractError, match="rating source coverage"):
        validate_observed_source_coverage(
            market_price=market,
            source=ratings,
            dates=[date],
            min_coverage=0.90,
            min_count=2,
            label="rating source",
        )


def test_pricing_coverage_allows_bounded_historical_threshold_only() -> None:
    columns = [f"B{index:02d}" for index in range(40)]
    dates = pd.to_datetime(["2019-04-12", "2024-04-12"])
    market = pd.DataFrame(100.0, index=dates, columns=columns)
    model = market.copy()
    model.loc[:, columns[-1]] = np.nan

    validate_pricing_coverage(
        market_price=market,
        model_price=model,
        dates=[dates[0]],
        min_coverage=0.98,
        min_count=20,
        label="historical",
        historical_min_coverage=0.975,
        min_coverage_enforced_from=pd.Timestamp("2020-01-01"),
    )

    with pytest.raises(DataContractError, match="39/40"):
        validate_pricing_coverage(
            market_price=market,
            model_price=model,
            dates=[dates[1]],
            min_coverage=0.98,
            min_count=20,
            label="modern",
            historical_min_coverage=0.975,
            min_coverage_enforced_from=pd.Timestamp("2020-01-01"),
        )


def test_zl_coverage_policy_is_bounded_and_modern_fail_closed() -> None:
    columns = [f"B{index:03d}" for index in range(100)]
    dates = pd.to_datetime(["2020-01-03", "2024-01-05"])
    market = pd.DataFrame(100.0, index=dates, columns=columns)
    model = market.copy()
    model.loc[dates[0], columns[84:]] = np.nan
    model.loc[dates[1], columns[89:]] = np.nan

    validate_pricing_coverage(
        market_price=market,
        model_price=model,
        dates=[dates[0]],
        min_coverage=ZL_MIN_PRICING_COVERAGE,
        min_count=20,
        label="historical ZL",
        historical_min_coverage=ZL_HISTORICAL_MIN_COVERAGE,
        min_coverage_enforced_from=ZL_MIN_COVERAGE_ENFORCED_FROM,
    )

    with pytest.raises(DataContractError, match="89/100"):
        validate_pricing_coverage(
            market_price=market,
            model_price=model,
            dates=[dates[1]],
            min_coverage=ZL_MIN_PRICING_COVERAGE,
            min_count=20,
            label="modern ZL",
            historical_min_coverage=ZL_HISTORICAL_MIN_COVERAGE,
            min_coverage_enforced_from=ZL_MIN_COVERAGE_ENFORCED_FROM,
        )


def test_maturity_redemption_price_is_used_as_final_contractual_cashflow() -> None:
    date = pd.Timestamp("2024-01-02")
    maturity = pd.DataFrame({"123001.SZ": [1.0]}, index=[date])
    observed_floor = pd.DataFrame({"123001.SZ": [103.0]}, index=[date])
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "par_value": [100.0],
            "interest_freq": [1],
            "value_date": ["20240102"],
            "maturity_date": ["20250102"],
            "maturity_call_price": [110.0],
            "rate_clause": [
                "20240102-20250101,票面利率:0.00%"
            ],
        }
    )
    curve = pd.DataFrame({1.0: [0.02]}, index=[date])

    spread = build_implied_credit_spread_matrix(
        observed_bond_value=observed_floor,
        maturity=maturity,
        cb_basic=basic,
        government_curve=curve,
    )

    actual_365_time = 366.0 / 365.0
    assert spread.loc[date, "123001.SZ"] == pytest.approx(
        -np.log(103.0 / 110.0) / actual_365_time - 0.02
    )


def test_average_risk_free_rate_uses_observed_curve_window() -> None:
    curve = pd.DataFrame(
        {
            1.0: [0.018, 0.020, 0.022],
            3.0: [0.025, 0.026, 0.027],
        },
        index=pd.to_datetime(
            ["2024-01-02", "2024-01-03", "2024-02-01"]
        ),
    )

    rate = observed_average_risk_free_rate(
        curve=curve,
        start=pd.Timestamp("2024-01-01"),
        end=pd.Timestamp("2024-01-31"),
        tenor_years=1.0,
    )

    assert rate == pytest.approx(0.019)


def test_balance_unit_contract_rejects_raw_yuan_values() -> None:
    balance = pd.DataFrame(
        {"123001.SZ": [500_000_000.0]},
        index=pd.DatetimeIndex(["2024-01-02"]),
    )
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "issue_size": [500_000_000.0],
        }
    )

    with pytest.raises(DataContractError, match="not 万元"):
        validate_balance_wan_units(balance=balance, cb_basic=basic)


def test_stock_market_value_contract_rejects_yi_labeled_as_wan() -> None:
    market_value = pd.DataFrame(
        {"000001.SZ": [50.0]},
        index=pd.DatetimeIndex(["2024-01-02"]),
    )

    with pytest.raises(DataContractError, match="not 万元"):
        validate_stock_market_value_wan_units(market_value)
