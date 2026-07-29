from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests


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
        self.fina_indicator_calls: list[dict] = []
        self.cb_basic_fields = None

    def cb_basic(self, **kwargs):
        self.cb_basic_fields = kwargs.get("fields")
        return pd.DataFrame(
            {
                "ts_code": ["123001.SZ"],
                "stk_code": ["000001.SZ"],
                "maturity_date": ["20250102"],
                "par": [100.0],
                "pay_per_year": [1],
                "maturity_call_price": [110.0],
                "coupon_rate": [0.5],
                "remain_size": [500_000_000.0],
            }
        )

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

    def fina_indicator(self, **kwargs):
        self.fina_indicator_calls.append(kwargs)
        return pd.DataFrame(
            {
                "ts_code": [kwargs["ts_code"]],
                "ann_date": ["20231231"],
                "end_date": ["20230930"],
                "bps": [12.5],
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


def test_cb_basic_explicitly_requests_nondefault_maturity_price() -> None:
    pro = FakePro()

    result = data_pipeline.fetch_cb_basic(pro)

    assert "maturity_call_price" in pro.cb_basic_fields.split(",")
    assert result.loc[0, "maturity_call_price"] == pytest.approx(110.0)


def test_exchangeable_bonds_are_excluded_from_research_universe() -> None:
    basic = pd.DataFrame(
        {
            "ts_code": [
                "110001.SH",
                "120001.SZ",
                "132001.SH",
                "123999.SZ",
                "123998.SZ",
                "124017.SZ",
            ],
            "bond_short_name": [
                "普通转债",
                "16以岭EB",
                "14宝钢EB",
                "名称识别债",
                "后缀识别EB",
                "TCL定转2",
            ],
            "bond_full_name": [
                "公开发行可转换公司债券",
                "普通名称",
                "普通名称",
                "某集团公开发行可交换公司债券",
                "普通名称",
                "非公开发行可转换公司债券",
            ],
        }
    )

    result = data_pipeline.filter_exchangeable_bonds(basic)

    assert result["ts_code"].tolist() == [
        "110001.SH",
        "124017.SZ",
    ]


def test_bond_cache_merge_cannot_reintroduce_exchangeable_columns() -> None:
    dates = pd.to_datetime(["2024-01-05", "2024-01-12"])
    existing = pd.DataFrame(
        {
            "110001.SH": [100.0, 101.0],
            "132001.SH": [90.0, 91.0],
        },
        index=dates,
    )
    new = pd.DataFrame(
        {"110001.SH": [102.0]},
        index=pd.to_datetime(["2024-01-12"]),
    )

    result = data_pipeline._merge_bond_wide(
        existing,
        new,
        bond_codes=pd.Index(["110001.SH", "123999.SZ"]),
    )

    # An eligible code with no observations must not inflate every cache with
    # an all-NaN column.
    assert result.columns.tolist() == ["110001.SH"]
    assert result.loc[pd.Timestamp("2024-01-12"), "110001.SH"] == 102.0


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


def test_cb_daily_rejects_empty_open_date_instead_of_checkpointing_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        data_pipeline,
        "CB_DAILY_CHECKPOINT_ROOT",
        str(tmp_path / "cb_daily_checkpoint"),
    )

    class EmptySecondDayPro(FakePro):
        def cb_daily(self, **kwargs):
            if kwargs["trade_date"] == "20240103":
                self.cb_daily_calls.append(kwargs)
                return pd.DataFrame()
            return super().cb_daily(**kwargs)

    with pytest.raises(DataContractError, match="20240103"):
        data_pipeline.fetch_cb_daily(
            EmptySecondDayPro(), "20240101", "20240103"
        )


def test_weekly_rebuild_caps_end_at_last_completed_week() -> None:
    class CurrentWeekPro(FakePro):
        def trade_cal(self, **kwargs):
            return pd.DataFrame(
                {
                    "cal_date": [
                        "20260724",
                        "20260727",
                        "20260728",
                    ],
                    "is_open": [1, 1, 1],
                }
            )

    resolved = data_pipeline.resolve_completed_weekly_end(
        CurrentWeekPro(),
        start="20260724",
        requested_end="20260728",
        as_of=pd.Timestamp("2026-07-28 12:00:00"),
    )

    assert resolved == "20260724"


def test_historical_midweek_end_rolls_back_to_prior_completed_week() -> None:
    class HistoricalMidweekPro(FakePro):
        def trade_cal(self, **kwargs):
            return pd.DataFrame(
                {
                    "cal_date": [
                        "20240105",
                        "20240108",
                        "20240109",
                        "20240110",
                    ],
                    "is_open": [1, 1, 1, 1],
                }
            )

    resolved = data_pipeline.resolve_completed_weekly_end(
        HistoricalMidweekPro(),
        start="20240105",
        requested_end="20240110",
        as_of=pd.Timestamp("2026-07-28 12:00:00"),
    )

    assert resolved == "20240105"


def test_holiday_friday_uses_last_open_date_in_completed_week() -> None:
    class HolidayFridayPro(FakePro):
        def trade_cal(self, **kwargs):
            return pd.DataFrame(
                {
                    "cal_date": [
                        "20260619",
                        "20260622",
                        "20260623",
                        "20260624",
                        "20260625",
                        "20260626",
                    ],
                    "is_open": [1, 1, 1, 1, 1, 0],
                }
            )

    resolved = data_pipeline.resolve_completed_weekly_end(
        HolidayFridayPro(),
        start="20260619",
        requested_end="20260626",
        as_of=pd.Timestamp("2026-07-28 12:00:00"),
    )

    assert resolved == "20260625"


def test_weekly_pipeline_uses_completed_week_end_for_cb_daily(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pro = FakePro()
    observed_end: list[str] = []

    monkeypatch.setattr(data_pipeline, "init_tushare", lambda: pro)
    monkeypatch.setattr(
        data_pipeline,
        "resolve_completed_weekly_end",
        lambda *args, **kwargs: "20260724",
    )
    monkeypatch.setattr(
        data_pipeline,
        "fetch_cb_basic",
        lambda _: pd.DataFrame({"ts_code": ["123001.SZ"]}),
    )
    monkeypatch.setattr(
        data_pipeline,
        "OUT_BASIC",
        str(tmp_path / "cb_basic_info.csv"),
    )

    class PipelineStopped(Exception):
        pass

    def capture_cb_daily(_, start: str, end: str):
        observed_end.append(end)
        raise PipelineStopped

    monkeypatch.setattr(data_pipeline, "fetch_cb_daily", capture_cb_daily)

    with pytest.raises(PipelineStopped):
        data_pipeline.run_pipeline(
            start="20170101",
            end="20260728",
            rebuild_all=True,
            weekly_validation=True,
        )

    assert observed_end == ["20260724"]


def test_cb_daily_treats_nonpositive_close_as_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        data_pipeline,
        "CB_DAILY_CHECKPOINT_ROOT",
        str(tmp_path / "cb_daily_checkpoint"),
    )

    class ZeroClosePro(FakePro):
        def cb_daily(self, **kwargs):
            frame = super().cb_daily(**kwargs)
            frame["close"] = 0.0
            return frame

    result = data_pipeline.fetch_cb_daily(
        ZeroClosePro(), "20240101", "20240103"
    )

    assert result["price"].isna().all().all()


def test_cb_daily_resumes_from_completed_checkpoint_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        data_pipeline,
        "CB_DAILY_CHECKPOINT_ROOT",
        str(tmp_path / "cb_daily_checkpoint"),
    )
    monkeypatch.setattr(data_pipeline, "CB_DAILY_CHECKPOINT_EVERY", 1)

    class InterruptOncePro(FakePro):
        def __init__(self) -> None:
            super().__init__()
            self.fail_once = True

        def cb_daily(self, **kwargs):
            if kwargs["trade_date"] == "20240103" and self.fail_once:
                self.fail_once = False
                self.cb_daily_calls.append(kwargs)
                raise RuntimeError("simulated interruption")
            return super().cb_daily(**kwargs)

    pro = InterruptOncePro()
    with pytest.raises(DataContractError, match="20240103"):
        data_pipeline.fetch_cb_daily(pro, "20240101", "20240103")

    result = data_pipeline.fetch_cb_daily(pro, "20240101", "20240103")

    queried_dates = [call["trade_date"] for call in pro.cb_daily_calls]
    assert queried_dates.count("20240102") == 1
    assert queried_dates.count("20240103") == 2
    assert list(result["price"].index) == list(
        pd.to_datetime(["20240102", "20240103"])
    )
    assert not (tmp_path / "cb_daily_checkpoint").exists()


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


def test_bps_query_uses_announcement_history_not_report_period_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)
    pro = FakePro()
    basic = pd.DataFrame(
        {"ts_code": ["123001.SZ"], "stk_cd": ["000001.SZ"]}
    )
    dates = pd.to_datetime(["2024-01-02", "2024-01-05"])
    price = pd.DataFrame({"123001.SZ": [100.0, 101.0]}, index=dates)

    result = data_pipeline.fetch_bps(
        pro, basic, price, "20240101", "20240105"
    )

    assert pro.fina_indicator_calls == [
        {
            "ts_code": "000001.SZ",
            "start_date": "20220101",
            "end_date": "20231231",
            "fields": "ts_code,ann_date,end_date,bps,update_flag",
        },
        {
            "ts_code": "000001.SZ",
            "start_date": "20240101",
            "end_date": "20241231",
            "fields": "ts_code,ann_date,end_date,bps,update_flag",
        },
    ]
    assert result["123001.SZ"].tolist() == [12.5, 12.5]


def test_bps_query_fails_closed_when_any_underlying_request_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)

    class FailingBpsPro(FakePro):
        def fina_indicator(self, **kwargs):
            raise RuntimeError("source unavailable")

    basic = pd.DataFrame(
        {"ts_code": ["123001.SZ"], "stk_cd": ["000001.SZ"]}
    )
    price = pd.DataFrame(
        {"123001.SZ": [100.0]},
        index=pd.to_datetime(["2024-01-02"]),
    )

    with pytest.raises(DataContractError, match="fina_indicator failed"):
        data_pipeline.fetch_bps(
            FailingBpsPro(), basic, price, "20240101", "20240105"
        )


def test_bps_same_announcement_prefers_latest_report_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)

    class SameAnnouncementPro(FakePro):
        def fina_indicator(self, **kwargs):
            self.fina_indicator_calls.append(kwargs)
            if kwargs["start_date"] != "20240101":
                return pd.DataFrame()
            return pd.DataFrame(
                {
                    "ts_code": [kwargs["ts_code"], kwargs["ts_code"]],
                    "ann_date": ["20240101", "20240101"],
                    "end_date": ["20231231", "20230930"],
                    "bps": [13.0, 11.0],
                }
            )

    basic = pd.DataFrame(
        {"ts_code": ["123001.SZ"], "stk_cd": ["000001.SZ"]}
    )
    price = pd.DataFrame(
        {"123001.SZ": [100.0]},
        index=pd.to_datetime(["2024-01-02"]),
    )

    result = data_pipeline.fetch_bps(
        SameAnnouncementPro(), basic, price, "20240101", "20240105"
    )

    assert result.loc[pd.Timestamp("2024-01-02"), "123001.SZ"] == pytest.approx(
        13.0
    )


def test_observed_bond_floor_is_masked_to_actual_market_cells() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    price = pd.DataFrame({"123001.SZ": [101.0, np.nan]}, index=dates)
    provider_value = pd.DataFrame({"123001.SZ": [96.2, 96.4]}, index=dates)

    floor = data_pipeline.build_observed_bond_floor(
        provider_bond_value=provider_value,
        market_price=price,
    )

    assert floor.loc[pd.Timestamp("2024-01-02"), "123001.SZ"] == pytest.approx(
        96.2
    )
    assert pd.isna(floor.loc[pd.Timestamp("2024-01-03"), "123001.SZ"])


def test_clause_cache_uses_akshare_contract_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_detail(symbol: str):
        assert symbol == "123001"
        return pd.DataFrame(
            {
                "SECURITY_CODE": ["123001"],
                "RESALE_CLAUSE": [
                    "最后两个计息年度，连续三十个交易日低于转股价的70%时回售。"
                ],
                "REDEEM_CLAUSE": [
                    "到期按面值上浮8%赎回；连续三十个交易日中至少十五个交易日"
                    "不低于转股价的130%，或余额不足3,000万元。"
                ],
                "PAR_VALUE": [100.0],
            }
        )

    monkeypatch.setattr(data_pipeline.ak, "bond_zh_cov_info", fake_detail)
    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)

    result = data_pipeline.fetch_clause_terms_akshare(["123001.SZ"])

    row = result.set_index("ts_code").loc["123001.SZ"]
    assert row["source_ok"]
    assert row["put_trigger_ratio"] == pytest.approx(0.70)
    assert row["redeem_trigger_ratio"] == pytest.approx(1.30)
    assert row["redeem_required_days"] == 15
    assert row["maturity_redemption_price"] == pytest.approx(108.0)


def test_load_clause_terms_reuses_valid_retry_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cache = tmp_path / "cb_clause_terms.csv"
    pd.DataFrame(
        {
            "ts_code": ["123001.SZ", "123002.SZ"],
            "source_ok": [True, True],
            "maturity_redemption_price": [110.0, 108.0],
        }
    ).to_csv(cache, index=False)
    monkeypatch.setattr(data_pipeline, "OUT_CLAUSES", str(cache))
    monkeypatch.setattr(
        data_pipeline,
        "fetch_clause_terms_akshare",
        lambda _: pytest.fail("valid retry cache should be reused"),
    )

    result = data_pipeline.load_clause_terms(
        ["123001.SZ", "123002.SZ"],
        reuse_cache=True,
    )

    assert result["ts_code"].tolist() == ["123001.SZ", "123002.SZ"]


def test_yield_curve_download_is_split_into_subannual_requests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_yield(*, start_date: str, end_date: str):
        calls.append((start_date, end_date))
        return pd.DataFrame(
            {
                "曲线名称": ["中债国债收益率曲线"],
                "日期": [pd.Timestamp(start_date)],
                "1年": [2.0],
                "3年": [2.5],
            }
        )

    monkeypatch.setattr(data_pipeline.ak, "bond_china_yield", fake_yield)
    monkeypatch.setattr(
        data_pipeline,
        "RF_CACHE",
        str(tmp_path / "rf_yield_cache.csv"),
    )

    result = data_pipeline.fetch_yield_curve("20170101", "20181231")

    assert calls == [
        ("20170101", "20171231"),
        ("20180101", "20181231"),
    ]
    assert list(result.columns) == [1.0, 3.0]


def test_tushare_retry_recovers_from_transient_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def flaky_call():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise requests.ConnectionError("remote disconnected")
        return "ok"

    monkeypatch.setattr(data_pipeline.time, "sleep", lambda _: None)

    assert data_pipeline.call_tushare_with_retry(flaky_call) == "ok"
    assert attempts == 3
