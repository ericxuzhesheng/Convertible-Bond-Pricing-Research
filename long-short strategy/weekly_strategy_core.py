"""Weekly signals, NEXT-session open execution, DAILY portfolio accounting."""
from __future__ import annotations
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd

MODELS = ("BS", "ZL", "LSM")
FINANCIAL = ("bps", "eps", "profit_dedt", "roe", "debt_to_assets", "ocfps")

def timestamp(value):
    if pd.isna(value) or str(value) in ("", "None", "nan"):
        return pd.NaT
    text = str(value).removesuffix(".0")
    t = pd.to_datetime(text, errors="coerce")
    if not pd.isna(t) and t.tzinfo:
        t = t.tz_convert("Asia/Shanghai").tz_localize(None)
    return t

def next_session(date, calendar):
    later = calendar[calendar > pd.Timestamp(date).normalize()]
    return later[0] if len(later) else pd.NaT

def prepare_financial(events, calendar):
    data = events.copy()
    if data.empty:
        return data
    for column in ("ann_date", "end_date"):
        data[column] = data[column].map(timestamp)
    data["effective_at"] = data.ann_date.map(
        lambda d: next_session(d, calendar) if pd.notna(d) else pd.NaT)
    if "available_at" in data:
        revised = data.available_at.map(timestamp)
        data["effective_at"] = data.effective_at.where(revised.isna() | revised.le(data.effective_at), revised)
    for column in FINANCIAL:
        data[column] = pd.to_numeric(data[column], errors="coerce").replace([np.inf, -np.inf], np.nan)
    data["update_priority"] = pd.to_numeric(data.get("update_flag", pd.Series(0,index=data.index)), errors="coerce").fillna(0)
    # The last provider row is not necessarily the latest report period.
    return data.dropna(subset=["ann_date", "end_date", "effective_at"]).sort_values(
        ["ts_code", "end_date", "effective_at", "update_priority"], kind="stable")

def financial_asof(events, cutoff):
    if events.empty:
        return pd.DataFrame(columns=FINANCIAL)
    return events.loc[(events.effective_at <= cutoff) & (events.end_date <= cutoff)].drop_duplicates(
        "ts_code", keep="last").set_index("ts_code")

def names_asof(events, date):
    if events.empty:
        return pd.Series(dtype=str)
    rows = events
    if not {"start", "ann"}.issubset(rows.columns):
        rows = events.copy()
        rows["start"] = rows.start_date.map(timestamp)
        rows["ann"] = rows.ann_date.map(timestamp)
    # A missing announcement is not retrospectively supplied by today's name.
    rows = rows.loc[(rows.start <= date) & (rows.ann < date)]
    return rows.sort_values(["start", "ann"], kind="stable").drop_duplicates(
        "ts_code", keep="last").set_index("ts_code")["name"]


def redemption_asof(events, date):
    if events.empty:
        return set()
    rows = events
    if "ann" not in rows:
        rows = rows.copy()
        rows["ann"] = rows.ann_date.map(timestamp)
        rows["available"] = rows.get("available_at", pd.Series("",index=rows.index)).map(timestamp)
    rows = rows.loc[(rows.ann < date) & (rows.available.isna() | rows.available.le(date+pd.Timedelta(hours=16)))]
    rows = rows.sort_values(["ann", "available"], na_position="first").drop_duplicates("ts_code", keep="last")
    return set(rows.loc[~rows.is_call.astype(str).str.contains("不强赎|不赎回", regex=True), "ts_code"])

@dataclass(frozen=True)
class Config:
    model: str = "mean"
    frequency: str = "weekly"
    risk_mode: str = "research"
    fundamentals: bool = True
    transaction_bps: float = 10.0
    borrow_rate: float = 0.03
    single_cap: float = 0.10
    select_ratio: float = 0.20

    def __post_init__(self):
        if self.model not in (*MODELS, "mean") or self.frequency not in ("weekly", "monthly"):
            raise ValueError("Unknown model/frequency")
        if self.risk_mode not in ("research", "tracking"):
            raise ValueError("Unknown risk mode")
        if self.transaction_bps < 0 or self.borrow_rate < 0:
            raise ValueError("Costs must be nonnegative")

@dataclass(frozen=True)
class ValueConfig(Config):
    """Independent cost-and-margin strategy; legacy Config stays unchanged."""
    transaction_bps: float = 5.0
    long_safety_margin: float = .02
    balance_legs: bool = True

    def __post_init__(self):
        super().__post_init__()
        if not np.isfinite(self.transaction_bps) or not 0 <= self.transaction_bps < 10000:
            raise ValueError("Transaction cost must be finite and below 10000bp")
        if not np.isfinite(self.long_safety_margin) or self.long_safety_margin < 0:
            raise ValueError("Safety margin must be finite and nonnegative")


def net_valuation_upside(signal, transaction_bps):
    fee = transaction_bps / 10000
    return (1 + signal) * (1 - fee) / (1 + fee) - 1


def select_targets(ranking, ratio=.2, cap=.1, *, transaction_bps=10.,
                   safety_margin=None, balance_legs=False):
    rows = ranking.loc[ranking.eligible].copy()
    rows["code_sort"] = rows.index.astype(str)
    rows = rows.sort_values(["signal", "code_sort"], ascending=[False, True], kind="stable")
    n = math.floor(len(rows) * ratio)
    if n == 0:
        return {}, {}
    longs, shorts = rows.iloc[:n], rows.iloc[-n:]
    if safety_margin is not None:
        net = net_valuation_upside(longs.signal, transaction_bps)
        longs = longs.loc[np.isfinite(net) & net.ge(safety_margin - 1e-12)]
    long_weight = min(1/len(longs), cap) if len(longs) else 0.
    long = {str(code): long_weight for code in longs.index}
    short_weight = min(1/n, cap)
    if balance_legs:
        short_weight = min(short_weight, sum(long.values())/n)
    short = {str(code): -short_weight for code in shorts.index} if short_weight else {}
    assert not set(long).intersection(short)
    return long, {**long, **short}

def read_matrix(path):
    frame = pd.read_csv(path, index_col=0, parse_dates=True, low_memory=False).sort_index()
    if frame.index.has_duplicates or frame.columns.has_duplicates:
        raise ValueError(f"Duplicate matrix axes: {path}")
    return frame

class Inputs:
    def __init__(self, base, input_dir):
        self.base, self.input_dir = Path(base), Path(input_dir)
        self._ranking_cache, self._daily_context = {}, {}
        metadata = self.input_dir / "acquisition.json"
        if not metadata.exists():
            raise ValueError("Missing strategy acquisition manifest")
        self.metadata = json.loads(metadata.read_text(encoding="utf-8-sig"))
        if self.metadata["status"] != "complete":
            raise ValueError("Daily/financial acquisition failed, not an empty eligible universe")
        self.calendar = pd.DatetimeIndex(pd.read_csv(
            self.input_dir / "calendar.csv", dtype=str).cal_date.map(timestamp)).sort_values()
        self.model = {m: read_matrix(self.base / f"{m}_Model_Prices.csv") for m in MODELS}
        self.model_market = {m: read_matrix(self.base / f).apply(pd.to_numeric, errors="coerce")
            for m, f in zip(MODELS, ["Market_Prices.csv", "ZL_Market_Prices.csv", "LSM_Market_Prices.csv"])}
        for model in MODELS:
            if not self.model[model].index.equals(self.model_market[model].index):
                raise ValueError(f"{model}: model and market date axes differ")
            if self.model[model].index.max() < pd.Timestamp(self.metadata["cutoff"]):
                raise ValueError(f"{model}: pricing is behind daily acquisition cutoff")
        for model in ("ZL", "LSM"):
            manifest = json.loads((self.base / f"{model}_Model_Manifest.json").read_text(encoding="utf-8"))
            if pd.Timestamp(manifest["input_cutoff"]) != self.model[model].index.max():
                raise ValueError(f"{model}: model manifest cutoff mismatch")
        self.price = read_matrix(self.base / "cb_price_cache.csv")
        files = {"rating": "cb_rating_cache.csv", "floor": "cb_bond_floor_cache.csv",
                 "term": "cb_maturity_cache.csv", "balance": "cb_balance_cache.csv"}
        self.features = {k: read_matrix(self.base / f) for k, f in files.items()}
        self.basic = pd.read_csv(self.base / "cb_basic_info.csv").drop_duplicates(
            "ts_code").set_index("ts_code")
        expected = self.calendar[(self.calendar >= self.metadata["start"]) & (self.calendar <= self.metadata["cutoff"])]
        parts = []
        for date in expected:
            file = self.input_dir / "daily" / f"{date:%Y%m%d}.csv"
            if not file.exists():
                raise ValueError(f"Missing DAILY partition {date.date()}")
            part = pd.read_csv(file)
            part["date"] = pd.to_datetime(part.trade_date.astype(str), format="%Y%m%d", errors="raise")
            parts.append(part)
        bars = pd.concat(parts, ignore_index=True)
        if bars.duplicated(["date", "ts_code"]).any():
            raise ValueError("Duplicate daily OHLCV")
        self.bars = {k: bars.pivot(index="date", columns="ts_code", values=k).apply(
            pd.to_numeric, errors="coerce") for k in ("open", "high", "low", "close", "vol", "amount")}
        self.financial = prepare_financial(pd.read_csv(
            self.input_dir / "financial_events.csv", dtype={"ann_date": str, "end_date": str}), self.calendar)
        names = self.input_dir / "name_events.csv"
        self.names = pd.read_csv(names, dtype=str) if names.exists() else pd.DataFrame()
        if not self.names.empty:
            self.names["start"] = self.names.start_date.map(timestamp)
            self.names["ann"] = self.names.ann_date.map(timestamp)
        cash = self.input_dir / "cashflow_events.csv"
        self.cashflows = pd.read_csv(cash) if cash.exists() else pd.DataFrame()
        halt_path = self.input_dir / "suspensions.csv"
        self.suspensions = pd.read_csv(halt_path) if halt_path.exists() else pd.DataFrame()
        self.calls = pd.DataFrame()
        calls = self.input_dir / "call_events.csv"
        if calls.exists():
            self.calls = pd.read_csv(calls, dtype=str)
        notices = self.input_dir / "redemption_notices_v2.csv"
        if not notices.exists():
            notices = self.input_dir / "redemption_notices.csv"
        if notices.exists():
            self.calls = pd.concat([self.calls,pd.read_csv(notices,dtype=str)],ignore_index=True)
        if not self.calls.empty:
            self.calls["ann"] = self.calls.ann_date.map(timestamp)
            self.calls["available"] = self.calls.get("available_at",pd.Series("",index=self.calls.index)).map(timestamp)
        event_state = self.input_dir / "event_sources.json"
        event_metadata = json.loads(event_state.read_text(encoding="utf-8")) if event_state.exists() else {}
        self.input_vintage = {"daily":self.metadata.get("fetched_at"),
                              "events":event_metadata.get("fetched_at"),
                              "event_normalization":event_metadata.get("normalization_revision"),
                              "cashflows":cash.stat().st_mtime_ns if cash.exists() else None,
                              "suspensions":halt_path.stat().st_mtime_ns if halt_path.exists() else None}
        benchmark = pd.read_csv(self.input_dir / "benchmark_daily.csv")
        benchmark["date"] = benchmark.trade_date.map(timestamp)
        self.benchmark = benchmark.set_index("date").sort_index()
        self.rf = read_matrix(self.base / "rf_yield_cache.csv")
        prior_amount = read_matrix(self.base / "cb_amount_cache.csv")
        prior_amount = prior_amount.loc[prior_amount.index < self.bars["amount"].index.min()]
        daily_amount = pd.concat([prior_amount, self.bars["amount"]]).sort_index()
        self.amount_mean = daily_amount.rolling(20, min_periods=15).mean()
        self.amount_count = daily_amount.rolling(20, min_periods=1).count()

    def signal_dates(self, start, end, frequency):
        from market_data_contracts import select_completed_weekly_dates
        dates = select_completed_weekly_dates(self.price.index)
        for frame in self.model.values():
            dates = dates.intersection(frame.index)
        dates = dates[(dates >= start) & (dates <= end)]
        if frequency == "monthly":
            dates = pd.DatetimeIndex(pd.Series(dates, index=dates).groupby(dates.to_period("M")).last())
            dates = dates[dates.to_period("M").end_time.normalize() <= pd.Timestamp(end).normalize()]
        return dates

    def risk_snapshot(self, date):
        path = self.input_dir / "risk" / f"{date.date()}.json"
        if not path.exists():
            return pd.DataFrame(), pd.NaT
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = pd.DataFrame([r["cell"] for r in raw["data"]["rows"]])
        rows["ts_code"] = rows.bond_id.astype(str).map(
            lambda x: x + (".SH" if x.startswith(("11", "13")) else ".SZ"))
        return rows.set_index("ts_code"), timestamp(raw["fetched_at"])

    def daily_alerts(self, date, held, fundamentals=True):
        """Known alerts; absence does not establish historical event coverage."""
        if date not in self._daily_context:
            names = names_asof(self.names, date)
            bad_names = set(names.index[names.fillna("").str.contains("ST|退", case=False, regex=True)])
            f = financial_asof(self.financial, date + pd.Timedelta(hours=16))
            bank_stocks = set(self.basic.loc[self.basic.bond_full_name.astype(str).str.contains("银行", regex=False), "stk_cd"])
            common_bad = f[["bps", "eps", "profit_dedt"]].le(0).any(axis=1)
            nonbank_bad = ~f.index.isin(bank_stocks) & (f.debt_to_assets.gt(75) | f.ocfps.le(0))
            bad_financials = set(f.index[common_bad | nonbank_bad])
            call_codes = redemption_asof(self.calls, date)
            self._daily_context[date] = bad_names, bad_financials, call_codes
        bad_names, bad_financials, call_codes = self._daily_context[date]
        alerts = {}
        for code in held:
            stock = self.basic.at[code, "stk_cd"]
            if stock in bad_names:
                alerts[code] = "stock_risk"
            if fundamentals and stock in bad_financials:
                alerts[code] = "fundamental_deterioration"
            if code in call_codes:
                alerts[code] = "redemption_event"
        return alerts

def build_ranking(data, date, config):
    cache = getattr(data, "_ranking_cache", None)
    cache_key = (date, config.risk_mode, config.fundamentals)
    if cache is not None and cache_key in cache:
        result = cache[cache_key].copy()
        fair = result.average_price if config.model == "mean" else result[config.model]
        result["signal"] = fair/result.market_price-1
        return result
    price = data.price.loc[date]
    out = pd.DataFrame({"market_price": price})
    for model in MODELS:
        out[model] = data.model[model].loc[date].reindex(out.index)
    model_ok = np.isfinite(out[list(MODELS)]).all(axis=1) & out[list(MODELS)].gt(0).all(axis=1)
    market_ok = price.gt(0) & np.isfinite(price)
    for model in MODELS:
        quoted = data.model_market[model].loc[date].reindex(out.index)
        market_ok &= np.isclose(quoted, price, rtol=1e-8, atol=1e-6, equal_nan=False)
    daily_close = data.bars["close"].loc[date].reindex(out.index)
    market_ok &= np.isclose(daily_close, price, rtol=1e-8, atol=1e-6, equal_nan=False)
    out["average_price"] = out[list(MODELS)].mean(axis=1, skipna=False)
    fair = out.average_price if config.model == "mean" else out[config.model]
    out["signal"] = fair / price - 1
    out["mean_upside"] = out.average_price / price - 1
    out["valuation_gap_yuan"] = out.average_price-price
    out["model_dispersion"] = out[list(MODELS)].std(axis=1, ddof=0) / out.average_price
    for label, frame in data.features.items():
        out[label] = frame.loc[date].reindex(out.index)
    out["bond_premium"] = price / out.floor - 1
    out["amount_20d"] = data.amount_mean.loc[date]
    out["amount_observations"] = data.amount_count.loc[date]
    out["name"] = data.basic.bond_short_name.reindex(out.index)
    stocks = data.basic.stk_cd.reindex(out.index)
    out["stock_code"] = stocks
    names = names_asof(data.names, date)
    out["stock_name"] = stocks.map(names)
    # Bank issuer identity is stable; do not use today's stock name to infer past ST status.
    out["is_bank"] = out.index.to_series().map(
        data.basic.bond_full_name.astype(str).str.contains("银行", regex=False)).fillna(False)
    financial = financial_asof(data.financial, date + pd.Timedelta(hours=16))
    for field in (*FINANCIAL, "ann_date", "end_date"):
        out[field] = stocks.map(financial[field]) if field in financial else np.nan
    listing = data.basic.list_date.map(timestamp).reindex(out.index)
    conditions = {
        "finite_risk_inputs": np.isfinite(out[["floor", "term", "balance"]]).all(axis=1),
        "three_models_present": model_ok,
        "market_prices_match": market_ok,
        "rating_at_least_AA": out.rating.astype(str).str.strip().isin(["AAA", "AA+", "AA"]),
        "price_at_most_150": price.le(150),
        "bond_premium_at_most_40pct": out.bond_premium.le(.4) & out.floor.gt(0),
        "maturity_at_least_half_year": out.term.ge(.5),
        "balance_at_least_3yi": out.balance.ge(30000),
        "listed_over_30_days": (date-listing).dt.days.gt(30),
        "liquidity": out.amount_20d.ge(1000) & out.amount_observations.ge(15)
                     & data.bars["amount"].loc[date].reindex(out.index).gt(0),
    }
    out["market_eligible"] = pd.DataFrame(conditions).fillna(False).all(axis=1)
    fin_ok = out[["bps", "eps", "profit_dedt"]].gt(0).all(axis=1)
    fin_ok &= out.is_bank | (out.debt_to_assets.le(75) & out.ocfps.gt(0))
    out["financial_known"] = out[["bps", "eps", "profit_dedt"]].notna().all(axis=1) & (
        out.is_bank | out[["debt_to_assets", "ocfps"]].notna().all(axis=1))
    if config.fundamentals:
        conditions["fundamentals"] = fin_ok
    st_known = out.stock_name.notna()
    st_safe = ~out.stock_name.fillna("").str.contains("ST|退", case=False, regex=True)
    out["st_known"] = st_known
    snapshot, fetched = data.risk_snapshot(date)
    execution = next_session(date, data.calendar)
    out["event_known"] = False
    out["call_status"] = ""
    out["risk_snapshot_at"] = str(fetched)
    if not snapshot.empty and pd.notna(execution) and fetched < execution + pd.Timedelta(hours=9, minutes=30):
        out["event_known"] = out.index.isin(snapshot.index)
        out["call_status"] = snapshot.redeem_icon.reindex(out.index).fillna("")
        delist = snapshot.delist_dt.map(timestamp).reindex(out.index)
        out["last_trading_day"] = delist
        event_safe = out.event_known & out.call_status.isin(["", "G"]) & (delist.isna() | delist.gt(date+pd.Timedelta(days=180)))
    else:
        event_safe = pd.Series(False, index=out.index)
    if config.risk_mode == "tracking":
        conditions["stock_risk_known_clear"] = st_known & st_safe
        conditions["event_known_clear"] = event_safe
    else:
        # Explicit incomplete historical research: no historical event absence inference.
        conditions["known_stock_risk_clear"] = st_safe
    known_calls = redemption_asof(getattr(data,"calls",pd.DataFrame()),date)
    conditions["known_redemption_clear"] = ~out.index.isin(known_calls)
    masks = pd.DataFrame(conditions).fillna(False)
    out["eligible"] = masks.all(axis=1)
    out["excluded_by"] = masks.apply(lambda r: ";".join(r.index[~r]), axis=1)
    out["signal_date"] = str(date.date())
    out["execution_date"] = str(execution.date()) if pd.notna(execution) else ""
    result = out.loc[price.notna()].sort_index()
    if cache is not None:
        cache[cache_key] = result.copy()
    return result

def validate_cashflows(events):
    if events.empty:
        return events.copy()
    required = {"event_id", "ts_code", "kind", "record_date", "ex_date", "payment_date", "amount_per_bond", "verified", "source"}
    if not required.issubset(events) or events.event_id.duplicated().any():
        raise ValueError("Invalid cashflow schema or duplicate event")
    rows = events.copy()
    for col in ("record_date", "ex_date", "payment_date"):
        rows[col] = rows[col].map(timestamp)
    rows["amount_per_bond"] = pd.to_numeric(rows.amount_per_bond, errors="coerce")
    rows = rows.loc[rows.verified.astype(str).str.lower().isin(["true", "1"])]
    bad = (rows[["record_date", "ex_date", "payment_date"]].isna().any(axis=1)
           | rows.record_date.ge(rows.ex_date) | rows.ex_date.gt(rows.payment_date)
           | rows.amount_per_bond.le(0) | ~np.isfinite(rows.amount_per_bond)
           | ~rows.kind.isin(["coupon", "redemption"]) | rows.source.isna())
    if bad.any():
        raise ValueError("Invalid verified cashflow dates/amount/source")
    return rows

def simulate(bars, calendar, targets, *, config, mode, cashflows=None, alert_fn=None, suspensions=None, entry_limits=None):
    """Signed quantities + cash + receivables. No ffill execution prices."""
    dates = bars["close"].index.sort_values()
    execution_targets = {}
    for signal_date, weights in sorted(targets.items()):
        execute = next_session(signal_date, calendar)
        if pd.notna(execute):
            execution_targets[execute] = (signal_date, weights)
    if not execution_targets or min(execution_targets) > dates.max():
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    dates = dates[dates >= min(execution_targets)]
    events = validate_cashflows(cashflows if cashflows is not None else pd.DataFrame())
    halts = suspensions.copy() if suspensions is not None else pd.DataFrame()
    if not halts.empty:
        for column in ("start_date","end_date"):
            halts[column] = halts[column].map(timestamp)
        halts = halts.loc[halts.verified.astype(str).str.lower().isin(["true","1"])]
        if (halts.source.isna() | halts.start_date.isna() | halts.end_date.isna() | halts.start_date.gt(halts.end_date)).any():
            raise ValueError("Invalid verified suspension evidence")
    units, marks, mark_dates, entitlements, receivables = {}, {}, {}, {}, {}
    cash, initial, previous_nav = 1_000_000., 1_000_000., 1_000_000.
    previous_short, previous_day = 0., dates[0]
    trades, holdings, ledger, issues = [], [], [], []
    forced = set()
    cost_rate = config.transaction_bps / 10000
    for date in dates:
        trade_start = len(trades)
        day_verified = True
        days = (date-previous_day).days
        borrow = previous_short * config.borrow_rate * days / 365.
        cash -= borrow
        for row in events.itertuples():
            if row.ex_date == date:
                entitlement = entitlements.get(row.event_id, 0.)
                receivables[row.event_id] = entitlement * row.amount_per_bond
                if row.kind == "redemption":
                    # Principal is exchanged for a receivable, not a fabricated market sale.
                    quantity = units.get(row.ts_code, 0.)
                    if abs(quantity - entitlement) > 1e-10:
                        raise ValueError("Redemption entitlement differs from extinguished holding")
                    units.pop(row.ts_code, None)
                    forced.discard(row.ts_code)
                    trades.append({"date": date, "code": row.ts_code, "quantity": -quantity,
                        "price": row.amount_per_bond, "cost": 0., "reason": "verified_redemption", "status": "settled_to_receivable"})
            if row.payment_date == date:
                cash += receivables.pop(row.event_id, 0.)
        open_prices = bars["open"].loc[date]
        for code in units:
            value = open_prices.get(code, np.nan)
            if np.isfinite(value) and value > 0:
                marks[code], mark_dates[code] = value, date
        nav_open = cash + sum(units[c]*marks[c] for c in units) + sum(receivables.values())
        if nav_open <= 0:
            raise ValueError(f"Portfolio insolvent on {date.date()}")
        target_info = execution_targets.get(date)
        desired = None
        if target_info:
            signal_date, desired = target_info
            desired = dict(desired)
        elif forced:
            desired = {c: q*marks[c]/nav_open for c, q in units.items()}
            signal_date = previous_day
        if desired is not None:
            for code in forced:
                desired.pop(code, None)
            codes = sorted(set(units) | set(desired))
            tradable = {}
            for code in codes:
                p = open_prices.get(code, np.nan)
                volume = bars["vol"].loc[date].get(code, np.nan)
                high, low = bars["high"].loc[date].get(code, np.nan), bars["low"].loc[date].get(code, np.nan)
                tradable[code] = bool(np.isfinite(p) and p > 0 and volume > 0 and high > low)
            locked_long = sum(max(units.get(c, 0)*marks.get(c, 0), 0) for c in codes if not tradable[c])
            locked_short = sum(max(-units.get(c, 0)*marks.get(c, 0), 0) for c in codes if not tradable[c])
            # Use a lower bound on AFTER-fee NAV for caps. This also covers turning
            # over BOTH legs; sale proceeds never expand the long budget.
            old_tradable_gross = sum(abs(units.get(c,0)*marks.get(c,0)) for c in codes if tradable[c])
            target_gross_limit = 2. if mode == "long_short" else 1.
            allocation_nav = max(0., (nav_open-cost_rate*old_tradable_gross)/(1+cost_rate*target_gross_limit))
            budgets = {1: max(0., allocation_nav-locked_long),
                       -1: max(0., allocation_nav-locked_short)}
            desired_values = {c: float(desired.get(c, 0))*allocation_nav for c in codes if tradable[c]}
            for sign in (1, -1):
                gross = sum(abs(v) for v in desired_values.values() if np.sign(v) == sign)
                factor = min(1., budgets[sign]/gross) if gross else 1.
                for code, value in list(desired_values.items()):
                    if np.sign(value) == sign:
                        desired_values[code] = value*factor
            if target_info and isinstance(config, ValueConfig):
                limits = (entry_limits or {}).get(signal_date, {})
                for code, value in list(desired_values.items()):
                    existing_long = max(units.get(code, 0.) * float(open_prices[code]), 0.)
                    if value > existing_long + 1e-7:
                        limit = limits.get(code, np.nan)
                        if not np.isfinite(limit) or limit <= 0:
                            raise ValueError(f"Missing frozen entry limit: {signal_date} {code}")
                        if float(open_prices[code]) > limit + 1e-10:
                            # A rejected increase may still close an old short.
                            desired_values[code] = existing_long
                            issues.append({"date": date, "code": code,
                                "reason": "open_above_margin_buy_limit", "open": float(open_prices[code]),
                                "entry_limit": limit})
                if config.balance_legs and mode == "long_short":
                    final_long = locked_long + sum(max(v, 0.) for v in desired_values.values())
                    short_gross = sum(max(-v, 0.) for v in desired_values.values())
                    short_budget = max(0., final_long - locked_short)
                    factor = min(1., short_budget/short_gross) if short_gross else 1.
                    for code, value in list(desired_values.items()):
                        if value < 0:
                            desired_values[code] = value * factor
            if not target_info:
                # A daily risk exit must leave every unaffected quantity unchanged.
                desired_values = {c: (0. if c in forced else units.get(c,0)*float(open_prices[c]))
                                  for c in codes if tradable[c]}
            for code in codes:
                if not tradable[code]:
                    if desired.get(code, 0) or units.get(code, 0):
                        issues.append({"date": date, "code": code, "reason": "no_executable_open_or_one_price_session"})
                    continue
                price = float(open_prices[code])
                target_quantity = desired_values[code]/price
                change = target_quantity-units.get(code, 0.)
                if abs(change*price) > 1e-7:
                    fee = abs(change)*price*cost_rate
                    cash -= change*price+fee
                    units[code], marks[code], mark_dates[code] = target_quantity, price, date
                    trades.append({"date": date, "signal_date": signal_date, "code": code,
                        "quantity": change, "price": price, "cost": fee,
                        "notional": abs(change)*price, "reason": "risk_exit" if code in forced else "rebalance",
                        "status": "simulated"})
                if abs(units.get(code, 0)) < 1e-12:
                    units.pop(code, None)
                    forced.discard(code)
        close = bars["close"].loc[date]
        stale_count = 0
        halted = set(halts.loc[halts.start_date.le(date) & halts.end_date.ge(date),"ts_code"]) if not halts.empty else set()
        for code in units:
            p = close.get(code, np.nan)
            if np.isfinite(p) and p > 0:
                marks[code], mark_dates[code] = p, date
            else:
                stale_count += 1
                explained = code in halted
                day_verified &= explained
                issues.append({"date": date, "code": code, "reason": "verified_suspension_stale_mark" if explained else "unverified_close_carry_mark_not_exit"})
        # Entitlement is determined at the record-date close, including short obligations.
        for row in events.itertuples():
            if row.record_date == date:
                entitlements[row.event_id] = units.get(row.ts_code, 0.)
        alerts = alert_fn(date, set(units)) if alert_fn else {}
        for code, reason in alerts.items():
            forced.add(code)
            issues.append({"date": date, "code": code, "reason": reason})
        long_value = sum(max(q*marks[c], 0) for c, q in units.items())
        short_value = sum(max(-q*marks[c], 0) for c, q in units.items())
        nav = cash+long_value-short_value+sum(receivables.values())
        daily_cost = sum(t["cost"] for t in trades[trade_start:])
        turnover = sum(t.get("notional", 0) for t in trades[trade_start:])/nav_open
        ledger.append({"date": date, "nav": nav/initial, "return": nav/previous_nav-1,
            "cash": cash/initial, "receivables": sum(receivables.values())/initial,
            "long_exposure": long_value/nav, "short_exposure": short_value/nav,
            "net_exposure": (long_value-short_value)/nav,
            "idle_cash_weight": max(0., 1-long_value/nav),
            "cost": daily_cost/initial, "borrow_cost": borrow/initial, "turnover": turnover,
            "marked_prices_verified": day_verified, "stale_mark_count": stale_count, "mode": mode})
        for code, quantity in sorted(units.items()):
            holdings.append({"date": date, "code": code, "quantity": quantity,
                "mark": marks[code], "weight": quantity*marks[code]/nav,
                "mark_date": mark_dates[code], "quote_age_days": (date-mark_dates[code]).days,
                "observed_close": bool(np.isfinite(close.get(code, np.nan)) and close.get(code, 0)>0)})
        previous_nav, previous_short, previous_day = nav, short_value, date
    return pd.DataFrame(ledger), pd.DataFrame(trades), pd.DataFrame(holdings), pd.DataFrame(issues)

def performance(nav, rf=0.):
    if nav.empty:
        return {"observations": 0}
    rows = nav.sort_values("date").copy()
    returns = rows["return"]
    days = max(1, (pd.Timestamp(rows.date.iloc[-1])-pd.Timestamp(rows.date.iloc[0])).days+1)
    annual = rows.nav.iloc[-1] ** (365.25/days)-1
    vol = returns.std(ddof=1)*np.sqrt(252)
    peak = rows.nav.cummax().clip(lower=1)
    dated_returns = returns.set_axis(pd.DatetimeIndex(rows.date))
    weekly = (1+dated_returns).resample("W-FRI").prod()-1
    weekly = weekly.loc[dated_returns.resample("W-FRI").count().gt(0)]
    metrics = {"observations": len(rows), "annual_return": annual, "annual_volatility": vol,
        "sharpe": (returns.mean()*252-rf)/vol if vol > 0 else None,
        "max_drawdown": (rows.nav/peak-1).min(), "weekly_win_rate": weekly.gt(0).mean(),
        "annual_turnover": rows.turnover.sum()*365.25/days,
        "average_cash_weight": rows.idle_cash_weight.mean(),
        "unverified_mark_days": int((~rows.marked_prices_verified).sum()),
        "stale_mark_days": int(rows.get("stale_mark_count",pd.Series(0,index=rows.index)).gt(0).sum())}


    metrics["performance_status"] = "valuation_path_explained_cashflows_incomplete"
    if metrics["unverified_mark_days"]:
        metrics["performance_status"] = "unverified_valuation_path"
        for field in ("annual_return", "annual_volatility", "sharpe", "max_drawdown", "weekly_win_rate"):
            metrics["diagnostic_"+field] = metrics[field]
            metrics[field] = None
    return metrics
