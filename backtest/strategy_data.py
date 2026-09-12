"""Independent DAILY strategy inputs. Never changes model caches.
Initialize explicitly with --initialize-history; routine runs are incremental.
"""
from __future__ import annotations
import argparse
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
import requests
from token_loader import load_tushare_token

BASE = Path(__file__).resolve().parent
INPUTS = BASE / "strategy_inputs"
FIN_FIELDS = "ts_code,ann_date,end_date,bps,eps,profit_dedt,roe,debt_to_assets,ocfps,update_flag"
OHLC_FIELDS = "ts_code,trade_date,open,high,low,close,vol,amount"

def now_local():
    return pd.Timestamp.now(tz="Asia/Shanghai").isoformat()

def write_text(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == payload:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)

def write_json(path, value):
    write_text(path, json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))

def write_csv(path, frame):
    write_text(path, frame.to_csv(index=False, lineterminator="\n"))

class SourceError(RuntimeError):
    pass

class TushareReader:
    """Documented HTTP interface; credentials never enter cache metadata."""
    def __init__(self):
        self.token = load_tushare_token()
        self.local = threading.local()
        self.rate_lock = threading.Lock()
        self.next_request = 0.

    def query(self, api, fields="", **params):
        for attempt in range(3):
            try:
                with self.rate_lock:
                    time.sleep(max(0., self.next_request-time.monotonic()))
                    self.next_request = time.monotonic()+0.5
                if not hasattr(self.local, "session"):
                    self.local.session = requests.Session()
                response = self.local.session.post("https://api.tushare.pro", json={
                    "api_name": api, "token": self.token, "params": params,
                    "fields": fields}, timeout=45)
                response.raise_for_status()
                data = response.json()
                if data.get("code") != 0:
                    message = str(data.get("msg", "API error")).replace(self.token, "[redacted]")
                    raise SourceError(f"{api}: {message}")
                result = data.get("data") or {}
                return pd.DataFrame(result.get("items", []), columns=result.get("fields", []))
            except (requests.RequestException, ValueError) as exc:
                if attempt == 2:
                    raise SourceError(f"{api}: transport failure ({type(exc).__name__})") from None
                time.sleep(2 ** attempt)
            finally:
                time.sleep(0.5)

def validate_daily(frame, date):
    if frame.empty or not set(OHLC_FIELDS.split(",")).issubset(frame):
        raise SourceError(f"daily OHLCV unavailable: {date}")
    if frame.duplicated(["ts_code", "trade_date"]).any():
        raise SourceError(f"duplicate daily bars: {date}")
    if not frame.trade_date.astype(str).eq(date).all():
        raise SourceError(f"wrong daily partition: {date}")
    values = frame[["open", "high", "low", "close", "vol", "amount"]].apply(pd.to_numeric, errors="coerce")
    bars = values[["open", "high", "low", "close"]]
    close_only = values[["open", "high", "low"]].eq(0).all(axis=1) & values.close.gt(0)
    frame["bar_quality"] = close_only.map({True: "close_only_no_open_quote", False: "ohlc"})
    bad = values.vol.gt(0) & ~close_only & (
        bars.isna().any(axis=1) | bars.le(0).any(axis=1)
        | values.high.lt(bars.max(axis=1)) | values.low.gt(bars.min(axis=1)))
    if bad.any() or values[["vol", "amount"]].lt(0).any().any():
        raise SourceError(f"invalid OHLCV: {date}")

def cached_query(reader, root, key, api, fields="", **params):
    path = root / "raw" / f"{key}.json"
    if path.exists():
        value = json.loads(path.read_text(encoding="utf-8"))
    else:
        frame = reader.query(api, fields, **params)
        value = {"api": api, "params": params, "fetched_at": now_local(),
                 "records": json.loads(frame.to_json(orient="records"))}
        write_json(path, value)
    return pd.DataFrame(value["records"]), value["fetched_at"]

def merge_financial_versions(old, new):
    """A changed same-key record becomes available only when observed."""
    if old.empty:
        return new.copy()
    fields = ["bps", "eps", "profit_dedt", "roe", "debt_to_assets", "ocfps"]
    additions = []
    for record in new.to_dict("records"):
        previous = old
        for key in ["ts_code", "ann_date", "end_date"]:
            previous = previous.loc[previous[key].astype(str).eq(str(record[key]))]
        if previous.empty:
            additions.append(record)
        else:
            latest = previous.iloc[-1]
            same = all((pd.isna(latest[k]) and pd.isna(record[k]))
                       or latest[k] == record[k] for k in fields)
            if not same:
                record["available_at"] = record["fetched_at"]
                record["vintage"] = "observed_revision"
                additions.append(record)
    return pd.concat([old, pd.DataFrame(additions)], ignore_index=True)

def download_daily(reader, root, dates):
    missing = [str(d) for d in dates if not (root / "daily" / f"{d}.csv").exists()]
    def fetch(block):
        if not block:
            return
        frame = reader.query("cb_daily", OHLC_FIELDS, start_date=block[0], end_date=block[-1])
        if len(frame) >= 2000:
            if len(block) == 1:
                raise SourceError("Daily endpoint row limit exceeded")
            middle = len(block)//2
            fetch(block[:middle])
            fetch(block[middle:])
            return
        for date in block:
            daily = frame.loc[frame.trade_date.astype(str).eq(date)].copy() if "trade_date" in frame else pd.DataFrame()
            validate_daily(daily, date)
            write_csv(root / "daily" / f"{date}.csv", daily.sort_values("ts_code"))
    # Three bounded workers hide network latency; the reader enforces one shared rate limit.
    blocks = [missing[i:i+2] for i in range(0, len(missing), 2)]
    with ThreadPoolExecutor(max_workers=3) as executor:
        pending = {executor.submit(fetch, block): len(block) for block in blocks}
        completed, next_progress = 0, 50
        try:
            for future in as_completed(pending):
                future.result()
                completed += pending[future]
                if completed >= next_progress or completed == len(missing):
                    print(f"Daily OHLCV added {completed}/{len(missing)}", flush=True)
                    next_progress += 50
        except Exception:
            for future in pending:
                future.cancel()
            raise

def financial_query(reader, root, stock, first, last):
    f, fetched = cached_query(reader, root, f"financial/{stock}/{first}-{last}",
        "fina_indicator", FIN_FIELDS, ts_code=stock, start_date=first, end_date=last)
    if len(f) < 100:
        return [(f, fetched)]
    lower, upper = pd.Timestamp(first), pd.Timestamp(last)
    if (upper-lower).days < 2:
        raise SourceError("Financial endpoint truncated on a single date")
    middle = lower + (upper-lower)//2
    return financial_query(reader, root, stock, first, middle.strftime("%Y%m%d")) + financial_query(
        reader, root, stock, (middle+pd.Timedelta(days=1)).strftime("%Y%m%d"), last)

def update_inputs(root, start, end, initialize=False):
    state_path = root / "acquisition.json"
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    if not state and not initialize:
        raise SourceError("Strategy data needs --initialize-history (no model repricing)")
    if state.get("cutoff") == end and state.get("status") == "complete" and start >= state["start"]:
        from strategy_events import update_events
        update_events(root, end)
        print("Strategy inputs current; no new daily requests.", flush=True)
        return state
    if state:
        if end < state["cutoff"]:
            raise SourceError("Cannot truncate existing strategy input history")
        start = min(start, state["start"])
    reader = TushareReader()
    calendar, _ = cached_query(reader, root, f"calendar/{start}-{end}", "trade_cal",
        "cal_date,is_open", exchange="SSE", start_date=start.replace("-", ""),
        end_date=(pd.Timestamp(end) + pd.Timedelta(days=20)).strftime("%Y%m%d"))
    if calendar.empty or not {"cal_date", "is_open"}.issubset(calendar):
        raise SourceError("Trading calendar unavailable")
    calendar = calendar.loc[pd.to_numeric(calendar.is_open).eq(1)].sort_values("cal_date")
    write_csv(root / "calendar.csv", calendar)
    dates = calendar.cal_date.astype(str)
    dates = dates.loc[dates.le(end.replace("-", ""))]
    # EVERY trading day is stored; bounded requests split if the API row limit is hit.
    download_daily(reader, root, dates)
    benchmark_parts = []
    for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year+1):
        last = min(end.replace("-", ""), f"{year}1231")
        frame, _ = cached_query(reader, root, f"benchmark/{year}-{last}", "index_daily",
            OHLC_FIELDS, ts_code="000832.CSI", start_date=f"{year}0101", end_date=last)
        if frame.empty:
            raise SourceError(f"Benchmark missing: {year}")
        benchmark_parts.append(frame)
    benchmark = pd.concat(benchmark_parts).drop_duplicates("trade_date").sort_values("trade_date")
    if not set(dates).issubset(set(benchmark.trade_date.astype(str))):
        raise SourceError("Benchmark daily coverage incomplete")
    write_csv(root / "benchmark_daily.csv", benchmark)
    prices = pd.read_csv(BASE / "cb_price_cache.csv", index_col=0, parse_dates=True)
    basic = pd.read_csv(BASE / "cb_basic_info.csv", dtype={"stk_cd": str, "ts_code": str})
    active = prices.loc[start:end]
    rating = pd.read_csv(BASE / "cb_rating_cache.csv", index_col=0, parse_dates=True, low_memory=False).reindex_like(active)
    floor = pd.read_csv(BASE / "cb_bond_floor_cache.csv", index_col=0, parse_dates=True).reindex_like(active)
    candidate = active.le(150) & rating.isin(["AAA", "AA+", "AA"]) & (active/floor-1).le(.4)
    if state and not initialize:
        candidate = candidate.loc[candidate.index > pd.Timestamp(state["cutoff"])]
    codes = active.columns[candidate.any()]
    stocks = sorted(basic.loc[basic.ts_code.isin(codes), "stk_cd"].dropna().unique())
    financial_parts, name_parts, failures = [], [], []
    for i, stock in enumerate(stocks):
        try:
            financial_start = state["cutoff"] if state and not initialize else start
            first = f"{pd.Timestamp(financial_start).year-1}0101"
            last = end.replace("-", "")
            for f, fetched in financial_query(reader, root, stock, first, last):
                if not f.empty:
                    if not set(FIN_FIELDS.split(",")).issubset(f):
                        raise SourceError(f"Financial schema: {stock}")
                    f = f.loc[f.ann_date.astype(str).le(end.replace("-", ""))].copy()
                    f["fetched_at"], f["available_at"] = fetched, ""
                    f["vintage"], f["source"] = "provider_history_unverified", "Tushare fina_indicator"
                    financial_parts.append(f)
            names, fetched = cached_query(reader, root, f"names/{stock}/{end}", "namechange",
                "ts_code,name,start_date,end_date,ann_date,change_reason", ts_code=stock)
            if not names.empty:
                names["fetched_at"] = fetched
                name_parts.append(names)
        except SourceError as exc:
            failures.append(str(exc))
            if any(word in str(exc) for word in ("权限", "积分", "token")):
                break
        if i % 20 == 0 or i == len(stocks)-1:
            print(f"Issuer fundamentals {i+1}/{len(stocks)}", flush=True)
    if financial_parts:
        financial = pd.concat(financial_parts, ignore_index=True).drop_duplicates(
            ["ts_code", "ann_date", "end_date", "bps", "eps", "profit_dedt", "ocfps", "debt_to_assets"])
        path = root / "financial_events.csv"
        old = pd.read_csv(path, dtype={"ann_date": str, "end_date": str}) if path.exists() else pd.DataFrame()
        write_csv(path, merge_financial_versions(old, financial))
    if name_parts:
        names_path = root / "name_events.csv"
        if names_path.exists():
            name_parts.insert(0, pd.read_csv(names_path, dtype=str))
        write_csv(names_path, pd.concat(name_parts, ignore_index=True).drop_duplicates(
            ["ts_code", "start_date", "name"], keep="first").sort_values(["ts_code", "start_date"]))
    gaps = []
    try:
        calls, _ = cached_query(reader, root, f"calls/{end}", "cb_call", "",
            start_date=start.replace("-", ""), end_date=end.replace("-", ""))
        if len(calls) >= 2000:
            raise SourceError("cb_call row limit: event coverage incomplete")
        if not calls.empty:
            write_csv(root / "call_events.csv", calls)
    except SourceError as exc:
        gaps.append(str(exc))
    risk_path = root / "risk" / f"{end}.json"
    if not risk_path.exists():
        local = BASE / "logs" / f"jsl_redeem_{end.replace('-', '')}.json"
        if local.exists():
            snapshot = json.loads(local.read_text(encoding="utf-8"))
        else:
            url = "https://www.jisilu.cn/data/cbnew/redeem_list/"
            try:
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
                response.raise_for_status()
                snapshot = {"url": url, "fetched_at": now_local(), "data": response.json()}
            except (requests.RequestException, ValueError):
                snapshot = None
                gaps.append("Jisilu snapshot unavailable")
        if snapshot and (snapshot.get("data") or {}).get("rows"):
            write_json(risk_path, snapshot)
    cash_path = root / "cashflow_events.csv"
    if not cash_path.exists():
        write_csv(cash_path, pd.DataFrame(columns=["event_id", "ts_code", "kind", "record_date",
            "ex_date", "payment_date", "amount_per_bond", "verified", "source"]))
    gaps.append("Coupon rate periods alone do not verify record/ex/payment dates; total return needs verified cashflow_events.csv")
    result = {"start": start, "cutoff": end, "daily_dates": len(dates), "issuers": len(stocks),
        "fetched_at": now_local(), "status": "failed" if failures else "complete",
        "failures": failures, "research_limitations": gaps,
        "financial_vintage": "Initial historical records are not original-release vintages"}
    write_json(state_path, result)
    if failures:
        raise SourceError("Acquisition incomplete; see acquisition.json")
    from strategy_events import update_events
    update_events(root, end)
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initialize-history", action="store_true")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--end")
    parser.add_argument("--output-dir", type=Path, default=INPUTS)
    args = parser.parse_args()
    end = args.end or str(pd.read_csv(BASE / "cb_price_cache.csv", usecols=[0]).iloc[:, 0].max())
    update_inputs(args.output_dir, pd.Timestamp(args.start).date().isoformat(),
                  pd.Timestamp(end).date().isoformat(), args.initialize_history)

if __name__ == "__main__":
    main()

