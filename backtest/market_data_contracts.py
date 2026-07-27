"""Point-in-time market-data contracts shared by the pricing pipeline.

The functions in this module deliberately fail when an observed input is
missing.  Pricing code must not silently replace absent market data with a
constant such as 2% risk-free yield or 40% volatility.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from scipy.optimize import brentq


class DataContractError(RuntimeError):
    """Raised when point-in-time market data is unavailable or inconsistent."""


@dataclass(frozen=True)
class ClauseTerms:
    put_trigger_ratio: float | None
    put_window_days: int | None
    put_eligible_years: int | None
    redeem_trigger_ratio: float | None
    redeem_window_days: int | None
    redeem_required_days: int | None
    redeem_balance_threshold_wan: float | None
    maturity_redemption_price: float | None


@dataclass(frozen=True)
class ClauseHistoryState:
    """Observed clause-trigger state carried into a valuation date."""

    put_consecutive_days: int
    redeem_count: int
    redeem_flags: np.ndarray


_CHINESE_INTEGERS = {
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
    "十五": 15,
    "二十": 20,
    "三十": 30,
}


def _to_number(text: str | None) -> int | None:
    if not text:
        return None
    normalized = text.strip()
    if normalized.isdigit():
        return int(normalized)
    return _CHINESE_INTEGERS.get(normalized)


def _first_positive(values: Iterable[object]) -> float | None:
    for value in values:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric) and float(numeric) > 0:
            return float(numeric)
    return None


def build_clause_history_state(
    *,
    conversion_value: pd.Series,
    valuation_date: pd.Timestamp,
    par_value: float,
    put_trigger_ratio: float,
    put_eligible_start: pd.Timestamp,
    redeem_trigger_ratio: float,
    redeem_window_days: int,
) -> ClauseHistoryState:
    """Build clause counters from observed conversion values through valuation.

    The returned redemption ring is always length 64 because that is the fixed
    local-array capacity used by the CUDA kernel.  Put observations before the
    contractual eligibility date never contribute to the carried counter.
    """

    if not np.isfinite(par_value) or par_value <= 0:
        raise DataContractError("par_value must be positive")
    if not 1 <= int(redeem_window_days) <= 64:
        raise DataContractError("redeem_window_days must be between 1 and 64")

    observed = pd.to_numeric(conversion_value, errors="coerce").copy()
    observed.index = pd.to_datetime(observed.index, errors="coerce")
    observed = observed.loc[
        observed.index.notna()
        & (observed.index <= pd.Timestamp(valuation_date))
    ].sort_index()
    observed = observed.dropna()

    redeem_flags = np.zeros(64, dtype=np.int32)
    redeem_history = (
        observed.tail(int(redeem_window_days)).to_numpy(dtype=float)
        >= float(redeem_trigger_ratio) * float(par_value)
    ).astype(np.int32)
    redeem_flags[: len(redeem_history)] = redeem_history

    eligible = observed.loc[
        observed.index >= pd.Timestamp(put_eligible_start)
    ]
    put_hits = (
        eligible.to_numpy(dtype=float)
        <= float(put_trigger_ratio) * float(par_value)
    )
    put_consecutive_days = 0
    for hit in put_hits[::-1]:
        if not hit:
            break
        put_consecutive_days += 1

    return ClauseHistoryState(
        put_consecutive_days=put_consecutive_days,
        redeem_count=int(redeem_history.sum()),
        redeem_flags=redeem_flags,
    )


def build_conversion_price_matrix(
    *,
    dates: Sequence[pd.Timestamp],
    bonds: Sequence[str],
    cb_basic: pd.DataFrame,
    change_events: pd.DataFrame,
) -> pd.DataFrame:
    """Build the historical conversion price that was effective on each date.

    ``cb_basic.conv_price`` is intentionally never used as the historical
    starting value because it is the current conversion price.  The initial
    price must come from ``cb_price_chg.convert_price_initial`` or
    ``cb_basic.first_conv_price``.
    """

    index = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values()
    columns = pd.Index(bonds, dtype=object)
    result = pd.DataFrame(index=index, columns=columns, dtype=float)

    basic = cb_basic.copy()
    if "ts_code" not in basic:
        raise DataContractError("cb_basic is missing ts_code")
    basic = basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")

    events = change_events.copy()
    if not events.empty:
        required = {"ts_code", "change_date"}
        missing = required.difference(events.columns)
        if missing:
            raise DataContractError(
                f"conversion price events missing columns: {sorted(missing)}"
            )
        events["change_date"] = pd.to_datetime(
            events["change_date"], errors="coerce"
        )
        events = events.dropna(subset=["ts_code", "change_date"])

    missing_initial: list[str] = []
    for bond in columns:
        bond_events = (
            events.loc[events["ts_code"] == bond].sort_values("change_date")
            if not events.empty
            else pd.DataFrame()
        )
        initial = _first_positive(
            [
                *(
                    bond_events.get(
                        "convert_price_initial", pd.Series(dtype=float)
                    ).tolist()
                    if not bond_events.empty
                    else []
                ),
                (
                    basic.at[bond, "first_conv_price"]
                    if bond in basic.index and "first_conv_price" in basic
                    else np.nan
                ),
            ]
        )
        if initial is None:
            missing_initial.append(str(bond))
            continue

        series = pd.Series(initial, index=index, dtype=float)
        if not bond_events.empty:
            for event in bond_events.itertuples(index=False):
                effective_date = pd.Timestamp(event.change_date)
                after = _first_positive(
                    [getattr(event, "convertprice_aft", np.nan)]
                )
                if after is not None:
                    series.loc[series.index >= effective_date] = after
        result[bond] = series

    if missing_initial:
        preview = ", ".join(missing_initial[:10])
        suffix = " ..." if len(missing_initial) > 10 else ""
        raise DataContractError(
            f"initial conversion price unavailable for {preview}{suffix}"
        )

    return result


def _extract_percentage(text: str, anchor: str) -> float | None:
    match = re.search(
        rf"{anchor}.{{0,80}}?(\d+(?:\.\d+)?)\s*[%％]",
        text,
        flags=re.S,
    )
    if not match:
        return None
    return float(match.group(1)) / 100.0


def _extract_window(text: str, anchor: str) -> int | None:
    match = re.search(
        rf"{anchor}.{{0,20}}?连续([一二两三四五六七八九十\d]+)个交易日",
        text,
    )
    return _to_number(match.group(1)) if match else None


def extract_clause_terms(
    resale_clause: object,
    redeem_clause: object,
    *,
    par_value: float,
) -> ClauseTerms:
    """Extract numerical terms from observed Eastmoney/AkShare clause text."""

    resale = "" if pd.isna(resale_clause) else str(resale_clause)
    redeem = "" if pd.isna(redeem_clause) else str(redeem_clause)

    put_trigger = _extract_percentage(resale, r"(?:低于|不高于)")
    put_window = _extract_window(resale, r"(?:如果|当)")
    put_year_match = re.search(
        r"最后([一二两三四五六七八九十\d]+)个计息年度", resale
    )
    put_years = _to_number(put_year_match.group(1)) if put_year_match else None

    redeem_trigger = _extract_percentage(redeem, r"(?:不低于|高于)")
    redeem_window_match = re.search(
        r"连续([一二两三四五六七八九十\d]+)个交易日中"
        r"(?:至少)?(?:有)?([一二两三四五六七八九十\d]+)个交易日",
        redeem,
    )
    redeem_window = (
        _to_number(redeem_window_match.group(1))
        if redeem_window_match
        else None
    )
    redeem_required = (
        _to_number(redeem_window_match.group(2))
        if redeem_window_match
        else None
    )

    balance_match = re.search(
        r"余额不足\s*([\d,]+(?:\.\d+)?)\s*万元", redeem
    )
    balance_threshold = (
        float(balance_match.group(1).replace(",", ""))
        if balance_match
        else None
    )

    maturity_match = re.search(
        r"(?:票面面值|债券面值|面值)(?:上浮|加)\s*(\d+(?:\.\d+)?)\s*[%％]",
        redeem,
    )
    if maturity_match:
        maturity_price = float(par_value) * (
            1.0 + float(maturity_match.group(1)) / 100.0
        )
    else:
        absolute_ratio_match = re.search(
            r"(?:票面面值|债券面值|面值)(?:的)?\s*"
            r"(1\d{2}(?:\.\d+)?)\s*[%％]",
            redeem,
        )
        maturity_price = (
            float(par_value) * float(absolute_ratio_match.group(1)) / 100.0
            if absolute_ratio_match
            else None
        )

    return ClauseTerms(
        put_trigger_ratio=put_trigger,
        put_window_days=put_window,
        put_eligible_years=put_years,
        redeem_trigger_ratio=redeem_trigger,
        redeem_window_days=redeem_window,
        redeem_required_days=redeem_required,
        redeem_balance_threshold_wan=balance_threshold,
        maturity_redemption_price=maturity_price,
    )


def parse_coupon_schedule(rate_clause: object) -> pd.Series:
    """Expand Tushare's contractual coupon intervals to a daily rate series."""

    text = "" if pd.isna(rate_clause) else str(rate_clause)
    pattern = re.compile(
        r"(\d{8})\s*-\s*(\d{8}).{0,20}?票面利率\s*[:：]\s*"
        r"(\d+(?:\.\d+)?)\s*[%％]"
    )
    pieces: list[pd.Series] = []
    for start, end, rate in pattern.findall(text):
        start_ts = pd.to_datetime(start, format="%Y%m%d", errors="coerce")
        end_ts = pd.to_datetime(end, format="%Y%m%d", errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts) or end_ts < start_ts:
            continue
        pieces.append(
            pd.Series(
                float(rate) / 100.0,
                index=pd.date_range(start_ts, end_ts, freq="D"),
                dtype=float,
            )
        )
    if not pieces:
        raise DataContractError("contractual coupon schedule is unavailable")
    schedule = pd.concat(pieces)
    schedule = schedule[~schedule.index.duplicated(keep="last")].sort_index()
    return schedule


def calculate_accrued_interest(
    *,
    as_of: pd.Timestamp,
    value_date: pd.Timestamp,
    par_value: float,
    coupon_schedule: pd.Series,
) -> float:
    """Calculate actual/365 accrued interest using the contractual rate."""

    as_of = pd.Timestamp(as_of).normalize()
    value_date = pd.Timestamp(value_date).normalize()
    if as_of not in coupon_schedule.index:
        raise DataContractError(f"coupon rate unavailable on {as_of.date()}")

    anniversary = pd.Timestamp(
        year=as_of.year, month=value_date.month, day=value_date.day
    )
    if anniversary > as_of:
        anniversary = anniversary - pd.DateOffset(years=1)
    accrual_start = max(value_date, anniversary)
    elapsed_days = (as_of - accrual_start).days
    return float(par_value) * float(coupon_schedule.loc[as_of]) * elapsed_days / 365.0


def interpolate_observed_yield_curve(
    curve: pd.DataFrame,
    as_of: pd.Timestamp,
    maturity_years: float,
    *,
    max_staleness_days: int = 7,
) -> float:
    """Interpolate an observed curve without a constant-rate fallback."""

    if curve.empty:
        raise DataContractError("yield curve is empty")
    normalized = curve.copy()
    normalized.index = pd.to_datetime(normalized.index, errors="coerce")
    normalized = normalized.loc[normalized.index.notna()].sort_index()
    eligible = normalized.loc[normalized.index <= pd.Timestamp(as_of)]
    if eligible.empty:
        raise DataContractError(
            f"yield curve unavailable on or before {pd.Timestamp(as_of).date()}"
        )
    curve_date = eligible.index[-1]
    if (pd.Timestamp(as_of) - curve_date).days > max_staleness_days:
        raise DataContractError(
            f"yield curve is stale on {pd.Timestamp(as_of).date()}"
        )
    row = pd.to_numeric(eligible.iloc[-1], errors="coerce").dropna()
    if row.empty:
        raise DataContractError(f"yield curve has no tenors on {curve_date.date()}")
    try:
        tenors = np.asarray([float(value) for value in row.index], dtype=float)
    except (TypeError, ValueError) as exc:
        raise DataContractError("yield curve tenors are not numeric") from exc
    values = row.to_numpy(dtype=float)
    order = np.argsort(tenors)
    tenors = tenors[order]
    values = values[order]
    if not np.isfinite(maturity_years) or maturity_years <= 0:
        raise DataContractError("bond maturity must be positive")
    return float(np.interp(float(maturity_years), tenors, values))


def point_in_time_fundamental_matrix(
    *,
    events: pd.DataFrame,
    dates: Sequence[pd.Timestamp],
    securities: Sequence[str],
    value_column: str,
    availability_column: str = "ann_date",
) -> pd.DataFrame:
    """Align fundamentals from their publication date, never period end."""

    required = {"ts_code", availability_column, value_column}
    missing = required.difference(events.columns)
    if missing:
        raise DataContractError(
            f"fundamental events missing columns: {sorted(missing)}"
        )

    index = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values()
    result = pd.DataFrame(index=index, columns=securities, dtype=float)
    source = events.copy()
    source[availability_column] = pd.to_datetime(
        source[availability_column], errors="coerce"
    )
    source[value_column] = pd.to_numeric(source[value_column], errors="coerce")
    source = source.dropna(
        subset=["ts_code", availability_column, value_column]
    )

    for security in securities:
        subset = source.loc[source["ts_code"] == security].sort_values(
            availability_column
        )
        if subset.empty:
            continue
        series = subset.set_index(availability_column)[value_column]
        series = series[~series.index.duplicated(keep="last")]
        result[security] = series.reindex(index, method="ffill")
    return result


def build_point_in_time_balance_matrix(
    *,
    dates: Sequence[pd.Timestamp],
    bonds: Sequence[str],
    cb_basic: pd.DataFrame,
    share_events: pd.DataFrame,
) -> pd.DataFrame:
    """Build outstanding balance in CNY 10,000 using published cb_share rows."""

    index = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values()
    result = pd.DataFrame(index=index, columns=bonds, dtype=float)
    basic = cb_basic.copy()
    if "ts_code" not in basic:
        raise DataContractError("cb_basic is missing ts_code")
    basic = basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")

    events = share_events.copy()
    if not events.empty:
        required = {"ts_code", "publish_date", "remain_size"}
        missing = required.difference(events.columns)
        if missing:
            raise DataContractError(
                f"cb_share events missing columns: {sorted(missing)}"
            )
        events["publish_date"] = pd.to_datetime(
            events["publish_date"], errors="coerce"
        )
        events["remain_size"] = pd.to_numeric(
            events["remain_size"], errors="coerce"
        )
        events = events.dropna(
            subset=["ts_code", "publish_date", "remain_size"]
        )

    for bond in bonds:
        if bond not in basic.index:
            continue
        issue_size = pd.to_numeric(
            basic.at[bond, "issue_size"]
            if "issue_size" in basic
            else np.nan,
            errors="coerce",
        )
        if pd.isna(issue_size) or issue_size <= 0:
            raise DataContractError(f"issue size unavailable for {bond}")
        series = pd.Series(float(issue_size) / 10_000.0, index=index)

        if "list_date" in basic:
            list_date = pd.to_datetime(
                basic.at[bond, "list_date"], errors="coerce"
            )
            if pd.notna(list_date):
                series.loc[series.index < list_date] = np.nan

        if not events.empty:
            subset = events.loc[events["ts_code"] == bond].sort_values(
                "publish_date"
            )
            for event in subset.itertuples(index=False):
                publication = pd.Timestamp(event.publish_date)
                balance_wan = float(event.remain_size) / 10_000.0
                series.loc[series.index >= publication] = balance_wan
        result[bond] = series
    return result


def build_point_in_time_rating_matrix(
    *,
    dates: Sequence[pd.Timestamp],
    bonds: Sequence[str],
    rating_events: pd.DataFrame,
) -> pd.DataFrame:
    """Align ratings from their public announcement date."""

    required = {"ts_code", "ann_date", "rating"}
    missing = required.difference(rating_events.columns)
    if missing:
        raise DataContractError(
            f"rating events missing columns: {sorted(missing)}"
        )
    index = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values()
    result = pd.DataFrame(index=index, columns=bonds, dtype=object)
    events = rating_events.copy()
    events["ann_date"] = pd.to_datetime(events["ann_date"], errors="coerce")
    events = events.dropna(subset=["ts_code", "ann_date", "rating"])

    for bond in bonds:
        subset = events.loc[events["ts_code"] == bond].sort_values("ann_date")
        if subset.empty:
            continue
        series = subset.set_index("ann_date")["rating"].astype(str)
        series = series[~series.index.duplicated(keep="last")]
        result[bond] = series.reindex(index, method="ffill")
    return result


def build_credit_spread_matrix(
    *,
    maturity: pd.DataFrame,
    ratings: pd.DataFrame,
    government_curve: pd.DataFrame,
    corporate_curves: dict[str, pd.DataFrame],
    max_staleness_days: int = 7,
) -> pd.DataFrame:
    """Subtract the observed government curve from the observed rating curve."""

    aligned_ratings = ratings.reindex(
        index=maturity.index, columns=maturity.columns
    )
    result = pd.DataFrame(
        np.nan, index=maturity.index, columns=maturity.columns, dtype=float
    )
    normalized_curves = {
        str(rating).strip().upper(): curve
        for rating, curve in corporate_curves.items()
    }

    unavailable: set[str] = set()
    for date in maturity.index:
        for bond in maturity.columns:
            term = pd.to_numeric(maturity.at[date, bond], errors="coerce")
            rating = aligned_ratings.at[date, bond]
            if pd.isna(term) or float(term) <= 0 or pd.isna(rating):
                continue
            rating_key = str(rating).strip().upper()
            corporate_curve = normalized_curves.get(rating_key)
            if corporate_curve is None:
                unavailable.add(rating_key)
                continue
            government_yield = interpolate_observed_yield_curve(
                government_curve,
                pd.Timestamp(date),
                float(term),
                max_staleness_days=max_staleness_days,
            )
            corporate_yield = interpolate_observed_yield_curve(
                corporate_curve,
                pd.Timestamp(date),
                float(term),
                max_staleness_days=max_staleness_days,
            )
            result.at[date, bond] = max(corporate_yield - government_yield, 0.0)

    if unavailable:
        raise DataContractError(
            "credit curve unavailable for ratings: "
            + ", ".join(sorted(unavailable))
        )
    return result


def implied_credit_spread(
    *,
    observed_bond_value: float,
    cashflow_times: np.ndarray,
    cashflow_amounts: np.ndarray,
    risk_free_rates: np.ndarray,
    max_spread: float = 5.0,
) -> float:
    """Calibrate a non-negative spread to Tushare's observed pure-bond value."""

    observed = float(observed_bond_value)
    times = np.asarray(cashflow_times, dtype=float)
    amounts = np.asarray(cashflow_amounts, dtype=float)
    rates = np.asarray(risk_free_rates, dtype=float)
    if (
        not np.isfinite(observed)
        or observed <= 0
        or times.ndim != 1
        or len(times) == 0
        or len(times) != len(amounts)
        or len(times) != len(rates)
        or not np.isfinite(times).all()
        or not np.isfinite(amounts).all()
        or not np.isfinite(rates).all()
        or (times <= 0).any()
        or (amounts <= 0).any()
    ):
        raise DataContractError("observed bond value or cash-flow inputs are invalid")

    def present_value(spread: float) -> float:
        return float(np.sum(amounts * np.exp(-(rates + spread) * times)))

    risk_free_value = present_value(0.0)
    tolerance = max(1e-8, risk_free_value * 1e-8)
    if observed > risk_free_value + tolerance:
        raise DataContractError(
            "observed bond value exceeds the contractual risk-free value"
        )
    if abs(observed - risk_free_value) <= tolerance:
        return 0.0
    if present_value(max_spread) > observed:
        raise DataContractError(
            "observed bond value implies a spread beyond the calibration bound"
        )
    return float(
        brentq(
            lambda spread: present_value(spread) - observed,
            0.0,
            float(max_spread),
        )
    )


def build_contractual_par_matrix(
    *,
    dates: Sequence[pd.Timestamp],
    bonds: Sequence[str],
    cb_basic: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast observed contractual par values, rejecting missing bonds."""

    if "ts_code" not in cb_basic or "par_value" not in cb_basic:
        raise DataContractError("contractual par value fields are unavailable")
    basic = cb_basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")
    values = {}
    missing = []
    for bond in bonds:
        par = pd.to_numeric(
            basic.at[bond, "par_value"] if bond in basic.index else np.nan,
            errors="coerce",
        )
        if pd.isna(par) or float(par) <= 0:
            missing.append(str(bond))
        else:
            values[bond] = float(par)
    if missing:
        raise DataContractError(
            "contractual par value unavailable for "
            + ", ".join(missing[:10])
            + (" ..." if len(missing) > 10 else "")
        )
    index = pd.DatetimeIndex(pd.to_datetime(dates))
    return pd.DataFrame(
        {bond: values[bond] for bond in bonds},
        index=index,
        dtype=float,
    )


def build_observed_volatility(
    *,
    adjusted_close: pd.Series,
    target_dates: Sequence[pd.Timestamp],
    window: int = 250,
    min_observations: int = 60,
    annualization_days: int = 250,
) -> pd.Series:
    """Calculate realized volatility without filling missing history."""

    if min_observations < 2 or window < min_observations:
        raise ValueError("volatility window must cover minimum observations")
    close = pd.to_numeric(adjusted_close, errors="coerce")
    close.index = pd.to_datetime(close.index, errors="coerce")
    close = close.loc[close.index.notna()].sort_index()
    close = close.loc[~close.index.duplicated(keep="last")]
    close = close.where(close > 0)
    log_returns = np.log(close / close.shift(1))
    volatility = (
        log_returns.rolling(
            window=window - 1,
            min_periods=min_observations - 1,
        ).std()
        * np.sqrt(float(annualization_days))
    )
    return volatility.reindex(pd.DatetimeIndex(pd.to_datetime(target_dates)))


def build_risk_free_rate_matrix(
    *,
    curve: pd.DataFrame,
    maturity: pd.DataFrame,
    max_staleness_days: int = 7,
) -> pd.DataFrame:
    """Interpolate observed government yields for every valid bond-date cell."""

    result = pd.DataFrame(
        np.nan, index=maturity.index, columns=maturity.columns, dtype=float
    )
    for date in maturity.index:
        for bond in maturity.columns:
            term = pd.to_numeric(maturity.at[date, bond], errors="coerce")
            if pd.isna(term) or float(term) <= 0:
                continue
            result.at[date, bond] = interpolate_observed_yield_curve(
                curve,
                pd.Timestamp(date),
                float(term),
                max_staleness_days=max_staleness_days,
            )
    return result


def _future_contractual_cashflows(
    *,
    row: pd.Series,
    valuation_date: pd.Timestamp,
) -> tuple[np.ndarray, np.ndarray]:
    par = pd.to_numeric(row.get("par_value"), errors="coerce")
    frequency = pd.to_numeric(row.get("interest_freq"), errors="coerce")
    value_date = pd.to_datetime(row.get("value_date"), errors="coerce")
    maturity_date = pd.to_datetime(row.get("maturity_date"), errors="coerce")
    if (
        pd.isna(par)
        or float(par) <= 0
        or pd.isna(frequency)
        or int(frequency) <= 0
        or pd.isna(value_date)
        or pd.isna(maturity_date)
    ):
        raise DataContractError("contractual cash-flow fields are unavailable")
    frequency = int(frequency)
    if 12 % frequency:
        raise DataContractError(
            f"unsupported contractual interest frequency {frequency}"
        )
    coupon_schedule = parse_coupon_schedule(row.get("rate_clause"))
    months = 12 // frequency
    payment_dates = []
    payment = pd.Timestamp(value_date) + pd.DateOffset(months=months)
    while payment < pd.Timestamp(maturity_date):
        payment_dates.append(payment)
        payment += pd.DateOffset(months=months)
    payment_dates.append(pd.Timestamp(maturity_date))

    times = []
    amounts = []
    for payment_date in payment_dates:
        if payment_date <= valuation_date:
            continue
        rate_date = min(
            payment_date - pd.Timedelta(days=1),
            coupon_schedule.index.max(),
        )
        eligible = coupon_schedule.loc[coupon_schedule.index <= rate_date]
        if eligible.empty:
            raise DataContractError(
                f"coupon unavailable for payment {payment_date.date()}"
            )
        coupon = float(par) * float(eligible.iloc[-1]) / frequency
        amount = coupon
        if payment_date == pd.Timestamp(maturity_date):
            amount += float(par)
        times.append((payment_date - valuation_date).days / 365.0)
        amounts.append(amount)
    if not times:
        raise DataContractError("no future contractual cash flows")
    return np.asarray(times, dtype=float), np.asarray(amounts, dtype=float)


def build_implied_credit_spread_matrix(
    *,
    observed_bond_value: pd.DataFrame,
    maturity: pd.DataFrame,
    cb_basic: pd.DataFrame,
    government_curve: pd.DataFrame,
    max_staleness_days: int = 7,
) -> pd.DataFrame:
    """Calibrate each bond-date spread to Tushare's daily pure-bond value."""

    values = observed_bond_value.reindex(
        index=maturity.index, columns=maturity.columns
    )
    if "ts_code" not in cb_basic:
        raise DataContractError("cb_basic is missing ts_code")
    basic = cb_basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")
    result = pd.DataFrame(
        np.nan, index=maturity.index, columns=maturity.columns, dtype=float
    )
    for bond in maturity.columns:
        if bond not in basic.index:
            continue
        row = basic.loc[bond]
        for date in maturity.index:
            observed = pd.to_numeric(values.at[date, bond], errors="coerce")
            term = pd.to_numeric(maturity.at[date, bond], errors="coerce")
            if pd.isna(observed) or observed <= 0 or pd.isna(term) or term <= 0:
                continue
            try:
                times, amounts = _future_contractual_cashflows(
                    row=row,
                    valuation_date=pd.Timestamp(date),
                )
                risk_free_rates = np.asarray(
                    [
                        interpolate_observed_yield_curve(
                            government_curve,
                            pd.Timestamp(date),
                            float(cashflow_time),
                            max_staleness_days=max_staleness_days,
                        )
                        for cashflow_time in times
                    ],
                    dtype=float,
                )
                result.at[date, bond] = implied_credit_spread(
                    observed_bond_value=float(observed),
                    cashflow_times=times,
                    cashflow_amounts=amounts,
                    risk_free_rates=risk_free_rates,
                )
            except DataContractError:
                continue
    return result


def build_active_market_mask(
    *,
    price: pd.DataFrame,
    required_inputs: Sequence[pd.DataFrame],
) -> pd.DataFrame:
    """Return cells with an observed market price and every required input."""

    numeric_price = price.apply(pd.to_numeric, errors="coerce")
    mask = numeric_price.notna() & np.isfinite(numeric_price) & (numeric_price > 0)
    for frame in required_inputs:
        aligned = frame.reindex(index=price.index, columns=price.columns)
        numeric = aligned.apply(pd.to_numeric, errors="coerce")
        mask &= numeric.notna() & np.isfinite(numeric)
    return mask.astype(bool)
