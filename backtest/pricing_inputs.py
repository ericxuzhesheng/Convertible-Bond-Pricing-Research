"""Observed inputs for the corrected models; never downloads or rewrites caches."""
from __future__ import annotations
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import numpy as np
import pandas as pd
from market_data_contracts import (DataContractError, _contractual_cashflow_schedule,
    build_clause_history_state, implied_credit_spread, select_completed_weekly_dates, _to_number)
from pricing_engine import Contract


IMPLEMENTATION = "cashflow-stopping-v2.3"


def contract_date(value):
    if pd.isna(value):
        return pd.NaT
    text = re.sub(r"\.0$", "", str(value).strip())
    return pd.to_datetime(text, format="%Y%m%d" if re.fullmatch(r"\d{8}", text) else "mixed", errors="coerce")


class PricingInputs:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.frames = {}
        for key, name in [("market", "cb_price_cache.csv"), ("parity", "cb_convert_val_cache.csv"),
                          ("floor", "cb_bond_floor_cache.csv"), ("volatility", "bs_volatility_cache.csv")]:
            frame = pd.read_csv(self.directory/name, index_col=0, parse_dates=True,
                                float_precision="round_trip", low_memory=False)
            if frame.index.has_duplicates or frame.columns.has_duplicates:
                raise DataContractError(f"duplicate axes in {name}")
            self.frames[key] = frame.apply(pd.to_numeric, errors="coerce").sort_index()
        self.market = self.frames["market"]
        self.basic = pd.read_csv(self.directory/"cb_basic_info.csv", dtype=str).drop_duplicates("ts_code").set_index("ts_code")
        for field in ["value_date", "maturity_date", "conv_start_date", "conv_end_date", "list_date", "delist_date"]:
            self.basic[field] = self.basic[field].map(contract_date)
        self.clauses = pd.read_csv(self.directory/"cb_clause_terms.csv").drop_duplicates("ts_code").set_index("ts_code")
        self.curve = pd.read_csv(self.directory/"rf_yield_cache.csv", index_col=0, parse_dates=True, float_precision="round_trip").sort_index()
        self.curve.columns = self.curve.columns.astype(float)
        self._curve_cache = {}
        self.static = {}
        self.static_errors = {}
        for code, row in self.basic.iterrows():
            try:
                dates, amounts = _contractual_cashflow_schedule(row=row)
                intervals = re.findall(r"(\d{8})\s*-\s*(\d{8}).{0,20}?票面利率\s*[:：]\s*(\d+(?:\.\d+)?)\s*[%％]", str(row.rate_clause))
                if not intervals or not np.isfinite(amounts).all():
                    raise DataContractError("incomplete contractual cash flows")
                starts = pd.DatetimeIndex([contract_date(x[0]) for x in intervals])
                ends = pd.DatetimeIndex([contract_date(x[1])+pd.Timedelta(days=1) for x in intervals])
                rates = np.array([float(x[2])/100*float(row.par_value) for x in intervals])
                # Accrual restarts at each contractual payment, including
                # semiannual contracts, not just at a change in coupon rate.
                payment_starts = pd.DatetimeIndex([row.value_date,*dates[:-1]])
                pieces = []
                for payment_start, payment_end in zip(payment_starts, dates):
                    for start,end,rate in zip(starts,ends,rates):
                        left,right=max(payment_start,start),min(payment_end,end)
                        if left<right: pieces.append((left,right,rate))
                self.static[code] = (dates, amounts,
                    pd.DatetimeIndex([x[0] for x in pieces]),
                    pd.DatetimeIndex([x[1] for x in pieces]),
                    np.array([x[2] for x in pieces]))
            except (DataContractError, ValueError, TypeError, OverflowError) as exc:
                self.static_errors[code] = str(exc)

    def dates(self, start="2019-01-01", end=None):
        dates = select_completed_weekly_dates(self.market.index)
        return dates[(dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end or dates.max()))]

    def observed_curve(self, date):
        if date in self._curve_cache:
            return self._curve_cache[date]
        rows = self.curve.loc[:date]
        if rows.empty or (date-rows.index[-1]).days > 7:
            raise DataContractError("observed yield curve missing or stale")
        row = rows.iloc[-1].dropna().sort_index()
        if (row <= -1).any():
            raise DataContractError("invalid effective government yield")
        # Annual effective source yields -> continuous zero-rate proxies.
        self._curve_cache[date] = (row.index.to_numpy(float), np.log1p(row.to_numpy(float)))
        return self._curve_cache[date]

    def contract(self, code, date, *, call_notice_days=30):
        if code not in self.static:
            raise DataContractError(self.static_errors.get(code, "contract unavailable"))
        row = self.basic.loc[code]
        if pd.isna(row.list_date) or row.list_date > date or (pd.notna(row.delist_date) and row.delist_date <= date):
            raise DataContractError("not listed on valuation date")
        values = {key: frame.at[date, code] if date in frame.index and code in frame else np.nan
                  for key, frame in self.frames.items()}
        if not np.isfinite(list(values.values())).all() or min(values["market"], values["parity"], values["floor"]) <= 0:
            raise DataContractError("missing observed market, parity, floor or volatility")
        if values["volatility"] < 0 or pd.isna(row.conv_start_date) or pd.isna(row.maturity_date):
            raise DataContractError("invalid volatility or contractual dates")
        tenors, rates = self.observed_curve(date)
        dates, amounts, starts, ends, accrual_rates = self.static[code]
        remaining = dates > date
        flow_times = (dates[remaining]-date).days.to_numpy(float)/365.
        flow_amounts = amounts[remaining]
        if not len(flow_times):
            raise DataContractError("no remaining contractual cash flows")
        # Converted source yields are used as zero proxies consistently in both
        # calibration and valuation; never recalibrate against the CB market price.
        spread = implied_credit_spread(observed_bond_value=values["floor"],
            cashflow_times=flow_times, cashflow_amounts=flow_amounts,
            risk_free_rates=np.interp(flow_times, tenors, rates))
        end = row.conv_end_date if pd.notna(row.conv_end_date) else row.maturity_date
        base = Contract(parity=float(values["parity"]), maturity=float(flow_times[-1]),
            redemption=float(flow_amounts[-1]), sigma=float(values["volatility"]), spread=spread,
            par=float(row.par_value), conversion_start=max(0., (row.conv_start_date-date).days/365.),
            conversion_end=(end-date).days/365.,
            coupon_times=flow_times[:-1], coupon_amounts=flow_amounts[:-1],
            accrual_starts=(starts-date).days.to_numpy(float)/365.,
            accrual_ends=(ends-date).days.to_numpy(float)/365., accrual_rates=accrual_rates,
            curve_tenors=tenors, curve_rates=rates)
        return base

    def clause_contract(self, code, date, base, *, call_notice_days=30):
        if code not in self.clauses.index:
            raise DataContractError("clause source unavailable")
        row = self.clauses.loc[code]
        names = ["put_trigger_ratio", "put_window_days", "put_eligible_years",
                 "redeem_trigger_ratio", "redeem_window_days", "redeem_required_days"]
        terms = pd.to_numeric(row[names], errors="coerce")
        put_text = re.sub(r"\s+","",str(row.get("resale_clause", "")))
        call_text = re.sub(r"\s+","",str(row.get("redeem_clause", "")))
        no_price_put = bool(re.search(r"除此之外.{0,20}不可.{0,20}主动回售|不设(?:置)?有条件回售|不设(?:置)?回售条款",put_text)) or ('改变募集资金用途' in put_text and not re.search(r'收盘|低于|有条件回售',put_text))
        no_price_call = '余额不足' in call_text and not re.search(r'收盘|股价|转股价格的',call_text)
        ordinal = re.search(r'第([一二两三四五六七八九十\d]+)个计息年度起',put_text)
        put_valid = np.isfinite(terms.iloc[:2]).all() and (np.isfinite(terms.put_eligible_years) or ordinal is not None)
        if str(row.source_ok).lower() != "true" or (not no_price_call and not np.isfinite(terms.iloc[3:]).all()) or (not no_price_put and not put_valid):
            raise DataContractError("incomplete observed clause terms")
        basic = self.basic.loc[code]
        put_start = basic.maturity_date if no_price_put else (basic.value_date+pd.DateOffset(years=_to_number(ordinal[1])-1) if ordinal else basic.maturity_date-pd.DateOffset(years=int(terms.put_eligible_years)))
        put_ratio = 0. if no_price_put else float(terms.put_trigger_ratio)
        call_ratio = np.inf if no_price_call else float(terms.redeem_trigger_ratio)
        window = 1 if no_price_call else int(terms.redeem_window_days)
        # These older contracts quote a fixed full put amount, including interest.
        fixed_put = re.search(r'(?:按(?:照)?|以)(?:债券)?(?:票面)?面值的?(\d+(?:\.\d+)?)%[（(]含',put_text)
        fixed_yuan = re.search(r'(\d+(?:\.\d+)?)元[（(]含当期(?:应计|计息年度)?利息',put_text)
        put_redemption = base.par*float(fixed_put[1])/100 if fixed_put else (float(fixed_yuan[1]) if fixed_yuan else base.par)
        observed = self.frames["parity"].loc[:date, code]
        # Counts before the conversion period cannot qualify for issuer calls.
        call_observed = observed.loc[observed.index >= basic.conv_start_date]
        history = build_clause_history_state(conversion_value=call_observed, valuation_date=date,
            par_value=base.par, put_trigger_ratio=put_ratio,
            put_eligible_start=put_start, redeem_trigger_ratio=call_ratio,
            redeem_window_days=window)
        return replace(base, call_start=base.conversion_start, call_ratio=call_ratio,
            call_window=window, call_required=1 if no_price_call else int(terms.redeem_required_days),
            call_notice_days=call_notice_days, call_history=history.redeem_flags[:window].astype(np.int8),
            put_start=np.inf if no_price_put else max(0., (put_start-date).days/365.), put_ratio=put_ratio,
            put_days=1 if no_price_put else int(terms.put_window_days), initial_put_count=history.put_consecutive_days,
            put_redemption=put_redemption, put_includes_accrued=bool(fixed_put or fixed_yuan))

    def fingerprint(self, cutoff, parameters):
        digest = hashlib.sha256((IMPLEMENTATION+json.dumps(parameters,sort_keys=True)).encode())
        active = self.market.loc[:cutoff].notna().any()
        codes = active.index[active]
        for frame in [*self.frames.values(), self.curve]:
            selected = frame.loc[:cutoff]
            if frame is not self.curve:
                selected = selected.reindex(columns=codes)
            digest.update(selected.to_csv(float_format="%.17g",lineterminator="\n").encode())
        fields = ["par_value","value_date","maturity_date","maturity_call_price","rate_clause",
                  "interest_freq","conv_start_date","conv_end_date","list_date","delist_date"]
        selected=self.basic.reindex(codes)[fields].copy()
        # Future listing/delisting events cannot affect a verified past prefix.
        # Preserve events on/before cutoff, which do change historical eligibility.
        for field in ('list_date','delist_date'):
            dates=pd.to_datetime(selected[field],errors='coerce')
            selected[field]=dates.where(dates<=cutoff)
        digest.update(selected.to_csv(lineterminator="\n").encode())
        clause_fields = ["source_ok","put_trigger_ratio","put_window_days","put_eligible_years",
                         "redeem_trigger_ratio","redeem_window_days","redeem_required_days","resale_clause","redeem_clause"]
        selected=self.clauses.reindex(codes)[clause_fields].copy()
        for field in ('resale_clause','redeem_clause'):
            selected[field]=selected[field].fillna('').map(lambda value:re.sub(r'\s+','',str(value)))
        digest.update(selected.to_csv(lineterminator="\n").encode())
        return digest.hexdigest()
