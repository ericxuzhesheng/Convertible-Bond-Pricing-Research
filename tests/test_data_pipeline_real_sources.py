from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
sys.path.insert(0, str(BACKTEST_DIR))

import data_pipeline  # noqa: E402
from market_data_contracts import DataContractError  # noqa: E402


class FakePro:
    def __init__(self) -> None:
        self.cb_daily_fields = None
        self.cb_daily_calls: list[dict] = []
        self.price_change_calls: list[str] = []
        self.rating_calls: list[str] = []
        self.daily_basic_calls: list[dict] = []

    def cb_daily(self, **kwargs):
        self.cb_daily_fields = kwargs.get("fields")
        self.cb_daily_calls.append(kwargs)
        trade_date = kwargs.get("trade_date", "20240102")
        return pd.DataFrame(
            {
                "ts_code": ["123001.SZ"],
                "trade_date": [trade_date],
                "close": [101.0],
                "amount": [2000.0],
                "cb_value": [88.5],
                "bond_value": [96.2],
            }
        )

    def trade_cal(self, **kwargs):
        return pd.DataFrame(
            {
                "cal_date": ["20240102", "20240103"],
                "is_open": [1, 1],
            }
        )

    def cb_price_chg(self, *, ts_code: str):
        self.price_change_calls.append(ts_code)
        rows = []
        for code in ts_code.split(","):
            rows.append(
                {
                    "ts_code": code,
                    "change_date": "20240101",
                    "convert_price_initial": 10.0,
                    "convertprice_bef": np.nan,
                    "convertprice_aft": np.nan,
                }
            )
        return pd.DataFrame(rows)

    def cb_rating(self, *, ts_code: str):
        self.rating_calls.append(ts_code)
        rows = []
        for code in ts_code.split(","):
            rows.append(
                {
                    "ts_code": code,
                    "ann_date": "20240102",
                    "rating_date": "20231229",
                    "rating": "AA",
                }
            )
        return pd.DataFrame(rows)

    def daily_basic(self, **kwargs):
        self.daily_basic_calls.append(kwargs)
        code = kwargs["ts_code"]
        return pd.DataFrame(
            {
                "ts_code": [code],
                "trade_date": ["20240102"],
                "total_mv": [500_000.0],
            }
        )


def test_cb_daily_downloads_observed_conversion_and_bond_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()

    result = data_pipeline.fetch_cb_daily(pro, "20240101", "20240103")

    assert {"cb_value", "bond_value"}.issubset(set(pro.cb_daily_fields.split(",")))
    assert result["convert_value"].loc[
        pd.Timestamp("2024-01-02"), "123001.SZ"
    ] == pytest.approx(88.5)
    assert result["provider_bond_value"].loc[
        pd.Timestamp("2024-01-02"), "123001.SZ"
    ] == pytest.approx(96.2)


def test_cb_daily_queries_each_open_date_to_avoid_row_limit_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()

    result = data_pipeline.fetch_cb_daily(pro, "20240101", "20240103")

    assert [call.get("trade_date") for call in pro.cb_daily_calls] == [
        "20240102",
        "20240103",
    ]
    assert list(result["price"].index) == list(
        pd.to_datetime(["20240102", "20240103"])
    )


def test_conversion_price_events_are_downloaded_in_bounded_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()

    result = data_pipeline.fetch_conversion_price_events(
        pro,
        ["123001.SZ", "123002.SZ", "123003.SZ"],
        batch_size=2,
    )

    assert pro.price_change_calls == ["123001.SZ,123002.SZ", "123003.SZ"]
    assert set(result["ts_code"]) == {"123001.SZ", "123002.SZ", "123003.SZ"}


def test_rating_download_uses_cb_rating_and_announcement_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()
    dates = pd.to_datetime(["2023-12-29", "2024-01-02"])
    price = pd.DataFrame(
        {"123001.SZ": [100.0, 101.0]},
        index=dates,
    )

    result = data_pipeline.fetch_ratings(pro, price)

    assert pro.rating_calls == ["123001.SZ"]
    assert pd.isna(result.loc[pd.Timestamp("2023-12-29"), "123001.SZ"])
    assert result.loc[pd.Timestamp("2024-01-02"), "123001.SZ"] == "AA"


def test_bond_floor_rejects_missing_contractual_coupon() -> None:
    dates = pd.to_datetime(["2024-01-02"])
    maturity = pd.DataFrame({"123001.SZ": [2.0]}, index=dates)
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ"],
            "par_value": [100.0],
            "interest_freq": [1],
            "rate_clause": [np.nan],
            "value_date": ["20220101"],
        }
    )
    curve = pd.DataFrame({1.0: [0.02], 3.0: [0.025]}, index=dates)

    with pytest.raises(DataContractError, match="coupon"):
        data_pipeline.calc_bond_floor_dcf(basic, maturity, curve)


def test_stock_market_value_queries_each_underlying_security(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()
    basic = pd.DataFrame(
        {
            "ts_code": ["123001.SZ", "123002.SZ"],
            "stk_cd": ["000001.SZ", "000002.SZ"],
        }
    )
    price = pd.DataFrame(
        {"123001.SZ": [100.0], "123002.SZ": [101.0]},
        index=pd.to_datetime(["20240102"]),
    )

    result = data_pipeline.fetch_stock_mv(
        pro, basic, price, "20240101", "20240103"
    )

    assert [call["ts_code"] for call in pro.daily_basic_calls] == [
        "000001.SZ",
        "000002.SZ",
    ]
    assert result.loc[pd.Timestamp("2024-01-02"), "123001.SZ"] == pytest.approx(
        500_000.0
    )
