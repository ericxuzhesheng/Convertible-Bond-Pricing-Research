"""Same-session Eastmoney observations for missing Tushare bond floors."""

from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from market_data_contracts import DataContractError


HISTORY_URL = "https://datacenter-web.eastmoney.com/api/data/get"
QUOTE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
COLUMNS = ["trade_date", "ts_code", "close", "bond_value", "premium"]


def eastmoney_history(date):
    """Read the date-filtered cross section, including its provider trading date."""
    response = requests.get(
        HISTORY_URL,
        params={
            "sty": "DATE,SECUCODE,FCLOSE,PUREBONDVALUE,PUREBONDOR",
            "token": "894050c76af8597a853f5b408b759f5d",  # public web query parameter
            "type": "RPTA_WEB_KZZ_LS", "source": "WEB",
            "filter": f"(date='{date:%Y-%m-%d}')", "st": "date,zcode", "sr": "1,1",
            "p": 1, "ps": 500,
        },
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("result")
    if payload.get("success") is not True or not result:
        raise DataContractError("Eastmoney history has no dated cross section")
    rows = result.get("data") or []
    if result.get("count") != len(rows):
        raise DataContractError("Eastmoney history returned an incomplete cross section")
    fields = dict(zip(
        ["DATE", "SECUCODE", "FCLOSE", "PUREBONDVALUE", "PUREBONDOR"], COLUMNS,
    ))
    return pd.DataFrame(rows).rename(columns=fields)


def eastmoney_close_snapshot(date):
    """Read the spot endpoint only for its own timestamped post-close session."""
    rows = []
    with requests.Session() as session:
        session.mount(QUOTE_URL, requests.adapters.HTTPAdapter(max_retries=1))
        for page in range(1, 7):
            response = session.get(
                QUOTE_URL,
                params={
                    "pn": page, "pz": 100, "po": 0, "np": 1, "fltt": 2, "invt": 2,
                    "fid": "f12", "fs": "b:MK0354", "fields": "f2,f12,f13,f124,f227,f238",
                },
                timeout=12,
            )
            response.raise_for_status()
            result = response.json().get("data")
            if not result or not isinstance(result.get("diff"), list) or not isinstance(result.get("total"), int):
                raise DataContractError("Eastmoney snapshot has no quote page")
            rows.extend(result["diff"])
            if len(rows) >= result["total"]:
                break
        else:
            raise DataContractError("Eastmoney snapshot exceeded six quote pages")
    frame = pd.DataFrame(rows)
    required = {"f2", "f12", "f13", "f124", "f227", "f238"}
    if not required.issubset(frame):
        raise DataContractError("Eastmoney snapshot is missing quote fields")
    timestamps = pd.to_datetime(
        pd.to_numeric(frame["f124"], errors="coerce"), unit="s", utc=True,
    ).dt.tz_convert(ZoneInfo("Asia/Shanghai")).dt.tz_localize(None)
    frame["trade_date"] = timestamps.dt.normalize()
    frame["ts_code"] = frame["f12"] + frame["f13"].map({1: ".SH", 0: ".SZ"})
    return frame.loc[timestamps >= date + pd.Timedelta(hours=15)].rename(
        columns={"f2": "close", "f227": "bond_value", "f238": "premium"},
    )


def verified_floors(rows, date, close):
    """Accept a dated, unique quote matching the primary close and premium."""
    if not set(COLUMNS).issubset(rows):
        raise DataContractError("Eastmoney bond floor rows are missing source fields")
    rows = rows.loc[pd.to_datetime(rows["trade_date"], errors="coerce").eq(date), COLUMNS]
    rows = rows.drop_duplicates().drop_duplicates("ts_code", keep=False).set_index("ts_code")
    rows = rows.reindex(close.index)
    source_close, floor, premium = (
        pd.to_numeric(rows[field], errors="coerce") for field in ("close", "bond_value", "premium")
    )
    valid = (
        np.isfinite(close) & close.gt(0)
        & np.isfinite(source_close) & source_close.gt(0)
        & close.sub(source_close).abs().le(0.001001)
        & np.isfinite(floor) & floor.gt(0)
        & (100 * (source_close / floor - 1)).sub(premium).abs().le(0.01)
    )
    return floor.where(valid)


def fill_bond_floors(floors, market_prices, min_coverage, min_count):
    """Fetch missing observations only on undercovered incremental dates."""
    sources = pd.DataFrame(index=floors.index, columns=floors.columns, dtype=object)
    for date in floors.index:
        close = market_prices.loc[date]
        active = np.isfinite(close) & close.gt(0)
        required = min(active.sum(), max(min_count, int(np.ceil(active.sum() * min_coverage))))
        if floors.loc[date].notna().sum() >= required:
            continue
        providers = [("Eastmoney history", eastmoney_history)]
        if date == floors.index.max():
            providers.append(("Eastmoney close snapshot", eastmoney_close_snapshot))
        for name, fetch in providers:
            try:
                values = verified_floors(fetch(date), date, close)
            except (requests.RequestException, DataContractError) as error:
                print(f"{date:%Y-%m-%d} {name}: {error}")
                continue
            accepted = floors.loc[date].isna() & values.notna()
            floors.loc[date, accepted] = values[accepted]
            sources.loc[date, accepted] = name
            print(f"{date:%Y-%m-%d} {name}: filled {accepted.sum()} missing bond floors")
            if floors.loc[date].notna().sum() >= required:
                break
    return floors, sources
