"""Point-in-time market-data contracts shared by the pricing pipeline.

The functions in this module deliberately fail when an observed input is
missing.  Pricing code must not silently replace absent market data with a
constant such as 2% risk-free yield or 40% volatility.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.optimize import brentq


CHINA_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
WEEKLY_DATA_READY_HOUR = 16
PUBLIC_CB_MIN_COUNT_ENFORCED_FROM = pd.Timestamp("2017-06-30")


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
    history_start = int(redeem_window_days) - len(redeem_history)
    redeem_flags[
        history_start : int(redeem_window_days)
    ] = redeem_history

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


def load_rebuildable_matrix_cache(
    *,
    path: str | PathLike[str],
    index: pd.Index,
    columns: pd.Index,
    refresh_cache: bool = False,
) -> pd.DataFrame:
    """Load an aligned observed-input cache unless refresh is explicit.

    Rebuilding model outputs and redownloading market inputs are deliberately
    separate operations. A model-only rebuild must not clear valid inputs.
    """

    if refresh_cache:
        return pd.DataFrame(index=index, columns=columns, dtype=float)
    try:
        cached = pd.read_csv(path, index_col=0, parse_dates=True)
    except FileNotFoundError:
        return pd.DataFrame(index=index, columns=columns, dtype=float)
    cached.index = pd.to_datetime(cached.index, errors="coerce")
    cached = cached.loc[cached.index.notna()]
    return cached.apply(pd.to_numeric, errors="coerce").reindex(
        index=index,
        columns=columns,
    )


def select_input_refresh_dates(
    *,
    all_dates: pd.Index,
    coverage_dates: pd.Index,
    refresh_cache: bool,
) -> pd.DatetimeIndex:
    """Keep model rebuild scope independent from observed-input refresh scope."""

    normalized_all_dates = pd.DatetimeIndex(pd.to_datetime(all_dates))
    normalized_coverage_dates = pd.DatetimeIndex(pd.to_datetime(coverage_dates))
    return normalized_all_dates if refresh_cache else normalized_coverage_dates


def select_completed_weekly_dates(
    dates: Sequence[pd.Timestamp],
    *,
    as_of: pd.Timestamp | None = None,
) -> pd.DatetimeIndex:
    """Return the last observed trading date in each completed W-FRI week."""

    observed = pd.DatetimeIndex(pd.to_datetime(dates, errors="coerce"))
    if observed.tz is not None:
        observed = observed.tz_convert(CHINA_MARKET_TIMEZONE).tz_localize(None)
    observed = observed.normalize()
    observed = observed[observed.notna()].sort_values().unique()
    if len(observed) == 0:
        return pd.DatetimeIndex([])
    cutoff = (
        pd.Timestamp(datetime.now(CHINA_MARKET_TIMEZONE)).tz_localize(None)
        if as_of is None
        else pd.Timestamp(as_of)
    )
    if cutoff.tzinfo is not None:
        cutoff = cutoff.tz_convert(CHINA_MARKET_TIMEZONE).tz_localize(None)
    periods = observed.to_period("W-FRI")
    complete = np.asarray(
        [
            (
                period.end_time.normalize()
                + pd.Timedelta(hours=WEEKLY_DATA_READY_HOUR)
            )
            <= cutoff
            for period in periods
        ],
        dtype=bool,
    )
    completed_dates = observed[complete]
    completed_periods = periods[complete]
    if len(completed_dates) == 0:
        return pd.DatetimeIndex([])
    frame = pd.DataFrame(
        {"date": completed_dates, "period": completed_periods}
    )
    return pd.DatetimeIndex(
        frame.groupby("period", sort=True)["date"].max().to_numpy()
    )


def validate_pricing_coverage(
    *,
    market_price: pd.DataFrame,
    model_price: pd.DataFrame,
    dates: Sequence[pd.Timestamp],
    min_coverage: float,
    min_count: int,
    label: str,
    min_count_enforced_from: pd.Timestamp | None = None,
    historical_min_coverage: float | None = None,
    min_coverage_enforced_from: pd.Timestamp | None = None,
) -> None:
    """Fail when a published pricing date has too few observed model values."""

    if not 0.0 < float(min_coverage) <= 1.0:
        raise ValueError("min_coverage must be in (0, 1]")
    if (
        historical_min_coverage is not None
        and not 0.0 < float(historical_min_coverage) <= float(min_coverage)
    ):
        raise ValueError(
            "historical_min_coverage must be in (0, min_coverage]"
        )
    if int(min_count) < 1:
        raise ValueError("min_count must be positive")
    aligned_model = model_price.reindex(
        index=market_price.index,
        columns=market_price.columns,
    ).apply(pd.to_numeric, errors="coerce")
    numeric_market = market_price.apply(pd.to_numeric, errors="coerce")
    failures = []
    for raw_date in dates:
        date = pd.Timestamp(raw_date)
        if date not in numeric_market.index:
            failures.append(f"{date.date()}: missing market row")
            continue
        active = (
            numeric_market.loc[date].notna()
            & np.isfinite(numeric_market.loc[date])
            & (numeric_market.loc[date] > 0)
        )
        expected = int(active.sum())
        priced = int(
            (
                active
                & aligned_model.loc[date].notna()
                & np.isfinite(aligned_model.loc[date])
            ).sum()
        )
        coverage = priced / expected if expected else 0.0
        required_coverage = float(min_coverage)
        if (
            historical_min_coverage is not None
            and min_coverage_enforced_from is not None
            and date < pd.Timestamp(min_coverage_enforced_from)
        ):
            required_coverage = float(historical_min_coverage)
        required_count = int(min_count)
        if (
            min_count_enforced_from is not None
            and date < pd.Timestamp(min_count_enforced_from)
        ):
            required_count = min(required_count, expected)
        if (
            expected == 0
            or priced < required_count
            or coverage < required_coverage
        ):
            failures.append(
                f"{date.date()}: {priced}/{expected} ({coverage:.2%})"
            )
    if failures:
        raise DataContractError(
            f"{label} pricing coverage failed: " + "; ".join(failures)
        )


def validate_observed_source_coverage(
    *,
    market_price: pd.DataFrame,
    source: pd.DataFrame,
    dates: Sequence[pd.Timestamp],
    min_coverage: float,
    min_count: int,
    label: str,
    require_finite_numeric: bool = False,
    min_count_enforced_from: pd.Timestamp | None = None,
) -> None:
    """Fail when an observed weekly source does not cover the active market."""

    if not 0.0 < float(min_coverage) <= 1.0:
        raise ValueError("min_coverage must be in (0, 1]")
    if int(min_count) < 1:
        raise ValueError("min_count must be positive")
    numeric_market = market_price.apply(pd.to_numeric, errors="coerce")
    aligned = source.reindex(
        index=market_price.index,
        columns=market_price.columns,
    )
    if require_finite_numeric:
        numeric_source = aligned.apply(pd.to_numeric, errors="coerce")
        observed = numeric_source.notna() & np.isfinite(numeric_source)
    else:
        observed = aligned.notna()
        for column in aligned.columns:
            text_mask = aligned[column].map(
                lambda value: bool(str(value).strip())
                if pd.notna(value)
                else False
            )
            observed[column] &= text_mask
    failures = []
    for raw_date in dates:
        date = pd.Timestamp(raw_date)
        if date not in numeric_market.index:
            failures.append(f"{date.date()}: missing market row")
            continue
        active = (
            numeric_market.loc[date].notna()
            & np.isfinite(numeric_market.loc[date])
            & (numeric_market.loc[date] > 0)
        )
        expected = int(active.sum())
        available = int((active & observed.loc[date]).sum())
        coverage = available / expected if expected else 0.0
        required_count = int(min_count)
        if (
            min_count_enforced_from is not None
            and date < pd.Timestamp(min_count_enforced_from)
        ):
            required_count = min(required_count, expected)
        if (
            expected == 0
            or available < required_count
            or coverage < float(min_coverage)
        ):
            failures.append(
                f"{date.date()}: {available}/{expected} ({coverage:.2%})"
            )
    if failures:
        raise DataContractError(
            f"{label} coverage failed: " + "; ".join(failures)
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
    maturity_price: float | None
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


def validate_balance_wan_units(
    *,
    balance: pd.DataFrame,
    cb_basic: pd.DataFrame,
    tolerance: float = 0.01,
) -> None:
    """Reject balance matrices that are not consistently stored in 万元."""

    required = {"ts_code", "issue_size"}
    missing = required.difference(cb_basic.columns)
    if missing:
        raise DataContractError(
            f"cb_basic missing balance unit fields: {sorted(missing)}"
        )
    issue_size = (
        cb_basic.drop_duplicates("ts_code", keep="last")
        .set_index("ts_code")["issue_size"]
        .apply(pd.to_numeric, errors="coerce")
    )
    violations: list[str] = []
    for bond in balance.columns:
        if bond not in issue_size.index or pd.isna(issue_size.at[bond]):
            continue
        maximum_wan = float(issue_size.at[bond]) / 10_000.0
        observed = pd.to_numeric(balance[bond], errors="coerce").dropna()
        if (
            (observed < 0).any()
            or (observed > maximum_wan * (1.0 + tolerance)).any()
        ):
            violations.append(bond)
    if violations:
        raise DataContractError(
            "balance cache is not 万元 or exceeds observed issue size; "
            f"rebuild required for {violations[:10]}"
        )


def validate_stock_market_value_wan_units(
    market_value: pd.DataFrame,
    *,
    minimum_plausible_wan: float = 1_000.0,
) -> None:
    """Reject stock market caps stored in 亿元 while labeled as 万元."""

    numeric = market_value.apply(pd.to_numeric, errors="coerce")
    observed = numeric.stack().dropna()
    invalid = observed[
        (observed <= 0) | (observed < minimum_plausible_wan)
    ]
    if not invalid.empty:
        sample = [
            f"{date}/{code}={value:g}"
            for (date, code), value in invalid.head(5).items()
        ]
        raise DataContractError(
            "stock market value cache is not 万元 or is implausible; "
            f"rebuild required: {sample}"
        )


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


def observed_average_risk_free_rate(
    *,
    curve: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    tenor_years: float = 1.0,
) -> float:
    """Average an observed zero-curve tenor over a performance window."""

    if tenor_years <= 0:
        raise DataContractError("tenor_years must be positive")
    table = curve.copy()
    table.index = pd.to_datetime(table.index, errors="coerce")
    table = table.loc[table.index.notna()].sort_index()
    try:
        table.columns = [float(column) for column in table.columns]
    except (TypeError, ValueError) as exc:
        raise DataContractError(
            "risk-free curve tenor columns must be numeric years"
        ) from exc
    table = table.apply(pd.to_numeric, errors="coerce").sort_index(axis=1)
    window = table.loc[
        (table.index >= pd.Timestamp(start))
        & (table.index <= pd.Timestamp(end))
    ]
    observations: list[float] = []
    for _, row in window.iterrows():
        valid = row.dropna()
        if valid.empty:
            continue
        tenors = valid.index.to_numpy(dtype=float)
        yields = valid.to_numpy(dtype=float)
        finite = np.isfinite(tenors) & np.isfinite(yields)
        tenors = tenors[finite]
        yields = yields[finite]
        if len(tenors) == 0:
            continue
        exact = np.isclose(tenors, float(tenor_years))
        if exact.any():
            observations.append(float(yields[exact][0]))
        elif (
            len(tenors) >= 2
            and tenors.min() <= tenor_years <= tenors.max()
        ):
            order = np.argsort(tenors)
            observations.append(
                float(
                    np.interp(
                        tenor_years,
                        tenors[order],
                        yields[order],
                    )
                )
            )
    if not observations:
        raise DataContractError(
            f"no observed {tenor_years:g}Y risk-free yields between "
            f"{pd.Timestamp(start).date()} and {pd.Timestamp(end).date()}"
        )
    return float(np.mean(observations))


def _future_contractual_cashflows(
    *,
    row: pd.Series,
    valuation_date: pd.Timestamp,
) -> tuple[np.ndarray, np.ndarray]:
    payment_dates, payment_amounts = _contractual_cashflow_schedule(row=row)
    future = payment_dates > pd.Timestamp(valuation_date)
    if not future.any():
        raise DataContractError("no future contractual cash flows")
    if not np.isfinite(payment_amounts[future]).all():
        raise DataContractError("coupon unavailable for a future payment")
    times = (
        (payment_dates[future] - pd.Timestamp(valuation_date)).days.to_numpy()
        / 365.0
    )
    return np.asarray(times, dtype=float), payment_amounts[future]


def _contractual_cashflow_schedule(
    *,
    row: pd.Series,
) -> tuple[pd.DatetimeIndex, np.ndarray]:
    """Parse one bond's dated contractual cash flows once."""

    par = pd.to_numeric(row.get("par_value"), errors="coerce")
    frequency = pd.to_numeric(row.get("interest_freq"), errors="coerce")
    value_date = pd.to_datetime(row.get("value_date"), errors="coerce")
    maturity_date = pd.to_datetime(row.get("maturity_date"), errors="coerce")
    maturity_redemption = pd.to_numeric(
        row.get(
            "maturity_call_price",
            row.get("maturity_price", row.get("maturity_redemption_price")),
        ),
        errors="coerce",
    )
    if (
        pd.isna(par)
        or float(par) <= 0
        or pd.isna(frequency)
        or int(frequency) <= 0
        or pd.isna(value_date)
        or pd.isna(maturity_date)
        or pd.isna(maturity_redemption)
        or float(maturity_redemption) <= 0
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

    amounts = []
    for payment_date in payment_dates:
        rate_date = min(
            payment_date - pd.Timedelta(days=1),
            coupon_schedule.index.max(),
        )
        eligible = coupon_schedule.loc[coupon_schedule.index <= rate_date]
        if eligible.empty:
            amount = np.nan
        else:
            amount = float(par) * float(eligible.iloc[-1]) / frequency
        if payment_date == pd.Timestamp(maturity_date) and np.isfinite(amount):
            # Tushare defines maturity_call_price as the contractual maturity
            # redemption amount including the final interest payment.
            amount = float(maturity_redemption)
        amounts.append(amount)
    return pd.DatetimeIndex(payment_dates), np.asarray(amounts, dtype=float)


def build_implied_credit_spread_matrix(
    *,
    observed_bond_value: pd.DataFrame,
    maturity: pd.DataFrame,
    cb_basic: pd.DataFrame,
    government_curve: pd.DataFrame,
    max_staleness_days: int = 7,
    backend: str = "auto",
) -> pd.DataFrame:
    """Calibrate each bond-date spread to Tushare's daily pure-bond value."""

    from credit_spread_cuda import (
        STATUS_OK,
        cuda_is_available,
        solve_credit_spreads_bisection,
        solve_credit_spreads_cuda,
    )

    selected_backend = str(backend).lower()
    if selected_backend not in {"auto", "cpu", "cuda"}:
        raise ValueError("backend must be one of: auto, cpu, cuda")
    values = observed_bond_value.reindex(
        index=maturity.index, columns=maturity.columns
    )
    if "ts_code" not in cb_basic:
        raise DataContractError("cb_basic is missing ts_code")
    basic = cb_basic.drop_duplicates("ts_code", keep="last").set_index("ts_code")
    result = pd.DataFrame(
        np.nan, index=maturity.index, columns=maturity.columns, dtype=float
    )
    normalized_curve = government_curve.copy()
    normalized_curve.index = pd.to_datetime(
        normalized_curve.index,
        errors="coerce",
    )
    normalized_curve = normalized_curve.loc[
        normalized_curve.index.notna()
    ].sort_index()
    if normalized_curve.empty:
        return result
    curve_tenors = pd.to_numeric(
        pd.Index(normalized_curve.columns),
        errors="coerce",
    ).to_numpy(dtype=float)
    tenor_order = np.argsort(curve_tenors)
    curve_tenors = curve_tenors[tenor_order]
    curve_values = (
        normalized_curve.apply(pd.to_numeric, errors="coerce")
        .to_numpy(dtype=float)[:, tenor_order]
    )
    valuation_dates = pd.DatetimeIndex(
        pd.to_datetime(maturity.index, errors="coerce")
    )
    curve_positions = normalized_curve.index.searchsorted(
        valuation_dates,
        side="right",
    ) - 1
    observed_array = values.apply(pd.to_numeric, errors="coerce").to_numpy(
        dtype=float
    )
    maturity_array = maturity.apply(
        pd.to_numeric,
        errors="coerce",
    ).to_numpy(dtype=float)

    problem_values: list[float] = []
    problem_offsets = [0]
    cashflow_time_chunks: list[np.ndarray] = []
    cashflow_amount_chunks: list[np.ndarray] = []
    risk_free_rate_chunks: list[np.ndarray] = []
    row_positions: list[int] = []
    column_positions: list[int] = []

    for column_position, bond in enumerate(maturity.columns):
        if bond not in basic.index:
            continue
        row = basic.loc[bond]
        try:
            payment_dates, payment_amounts = _contractual_cashflow_schedule(
                row=row
            )
        except DataContractError:
            continue
        bond_time_values: list[float] = []
        bond_amount_values: list[float] = []
        bond_rate_values: list[float] = []
        valid_rows = np.flatnonzero(
            np.isfinite(observed_array[:, column_position])
            & (observed_array[:, column_position] > 0)
            & np.isfinite(maturity_array[:, column_position])
            & (maturity_array[:, column_position] > 0)
        )
        for row_position in valid_rows:
            date = valuation_dates[row_position]
            if pd.isna(date):
                continue
            curve_position = int(curve_positions[row_position])
            if curve_position < 0:
                continue
            curve_date = normalized_curve.index[curve_position]
            if (date - curve_date).days > max_staleness_days:
                continue
            available_tenors = (
                np.isfinite(curve_tenors)
                & np.isfinite(curve_values[curve_position])
            )
            if not available_tenors.any():
                continue
            future_start = int(payment_dates.searchsorted(date, side="right"))
            if future_start >= len(payment_dates):
                continue
            times = (
                (payment_dates[future_start:] - date).days.to_numpy(dtype=float)
                / 365.0
            )
            amounts = payment_amounts[future_start:]
            if not np.isfinite(amounts).all():
                continue
            rates = np.interp(
                times,
                curve_tenors[available_tenors],
                curve_values[curve_position, available_tenors],
            )
            problem_values.append(
                float(observed_array[row_position, column_position])
            )
            row_positions.append(int(row_position))
            column_positions.append(int(column_position))
            bond_time_values.extend(times.tolist())
            bond_amount_values.extend(amounts.tolist())
            bond_rate_values.extend(rates.tolist())
            problem_offsets.append(problem_offsets[-1] + len(times))
        if bond_time_values:
            cashflow_time_chunks.append(
                np.asarray(bond_time_values, dtype=np.float64)
            )
            cashflow_amount_chunks.append(
                np.asarray(bond_amount_values, dtype=np.float64)
            )
            risk_free_rate_chunks.append(
                np.asarray(bond_rate_values, dtype=np.float64)
            )

    if not problem_values:
        return result
    problem_values_array = np.asarray(problem_values, dtype=np.float64)
    problem_offsets_array = np.asarray(problem_offsets, dtype=np.int64)
    cashflow_times = np.concatenate(cashflow_time_chunks)
    cashflow_amounts = np.concatenate(cashflow_amount_chunks)
    risk_free_rates = np.concatenate(risk_free_rate_chunks)

    actual_backend = selected_backend
    if actual_backend == "auto":
        actual_backend = "cuda" if cuda_is_available() else "cpu"
    try:
        if actual_backend == "cuda":
            spreads, statuses = solve_credit_spreads_cuda(
                problem_values_array,
                problem_offsets_array,
                cashflow_times,
                cashflow_amounts,
                risk_free_rates,
            )
        else:
            spreads, statuses = solve_credit_spreads_bisection(
                problem_values_array,
                problem_offsets_array,
                cashflow_times,
                cashflow_amounts,
                risk_free_rates,
            )
    except Exception as exc:
        if selected_backend != "auto":
            raise
        print(f"   CUDA credit spread failed; falling back to CPU: {exc}")
        actual_backend = "cpu"
        spreads, statuses = solve_credit_spreads_bisection(
            problem_values_array,
            problem_offsets_array,
            cashflow_times,
            cashflow_amounts,
            risk_free_rates,
        )
    successful = (statuses == STATUS_OK) & np.isfinite(spreads)
    result_values = result.to_numpy()
    result_values[
        np.asarray(row_positions, dtype=np.int64)[successful],
        np.asarray(column_positions, dtype=np.int64)[successful],
    ] = spreads[successful]
    print(
        f"   Credit spread backend: {actual_backend}; "
        f"problems: {len(problem_values_array):,}"
    )
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
