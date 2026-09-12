from dataclasses import replace
import json
import numpy as np
import pandas as pd
import pytest

from test_weekly_ensemble_strategy import bars, calendar, ranking_data
from weekly_strategy_core import ValueConfig, Config, select_targets, simulate, net_valuation_upside
import weekly_ensemble_strategy as runner


def select(rows, **kwargs):
    return select_targets(rows, transaction_bps=5, safety_margin=.02, balance_legs=True, **kwargs)


def test_three_qualifying_longs_use_thirty_percent_not_full_investment():
    rows = pd.DataFrame({"signal": [.05, .04, .03, .02, .001, -.01] + [-.1]*43,
                         "eligible": True}, index=[f"C{i:02}" for i in range(49)])
    long, ls = select(rows)
    assert len(long) == 3 and list(long.values()) == [.1]*3
    assert sum(long.values()) == pytest.approx(.3)
    shorts = [w for w in ls.values() if w < 0]
    assert len(shorts) == 9 and sum(shorts) == pytest.approx(-.3)
    assert sum(ls.values()) == pytest.approx(0)


def test_cost_adjusted_boundary_and_small_universe():
    threshold = 1.02*1.0005/.9995-1
    rows = pd.DataFrame({"signal": [threshold, threshold-1e-8, .02, 0., -.1], "eligible": True},
                        index=list("ABCDE"))
    assert select(rows)[0] == {"A": .1}
    assert select(rows.iloc[:4]) == ({}, {})
    rows.loc["A", "eligible"] = False
    assert select(rows) == ({}, {})


def test_no_qualifying_long_also_leaves_theoretical_short_leg_empty():
    rows = pd.DataFrame({"signal": [.02, .01, 0., -.01, -.02], "eligible": True}, index=list("ABCDE"))
    assert select(rows) == ({}, {})


def test_equal_signals_have_reproducible_slots_after_risk_filter():
    rows = pd.DataFrame({"signal": .05, "eligible": True}, index=list("JIHGFEDCBA"))
    rows.loc["A", "eligible"] = False
    assert select(rows)[0] == {"B": .1}


@pytest.mark.parametrize("kwargs", [{"transaction_bps": np.nan}, {"transaction_bps": 10000},
                                  {"long_safety_margin": -.01}, {"long_safety_margin": np.inf}])
def test_invalid_cost_or_margin_rejected(kwargs):
    with pytest.raises(ValueError):
        ValueConfig(**kwargs)


def test_open_jump_rejects_buy_and_charges_no_fee():
    d = pd.to_datetime(["2024-01-05", "2024-01-08"])
    b = bars(d, ("A",), [[100], [103]])
    fair = 104.
    limit = fair*.9995/(1.0005*1.02)
    assert net_valuation_upside(fair/100-1, 5) > .02
    nav, trades, held, issues = simulate(b, calendar(), {d[0]: {"A": .1}},
        config=ValueConfig(), mode="long_only", entry_limits={d[0]: {"A": limit}})
    assert trades.empty and held.empty and nav.nav.iloc[-1] == 1
    assert "open_above_margin_buy_limit" in set(issues.reason)


def test_five_bp_only_applies_to_actual_fills_and_cash_stays_idle():
    d = pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"])
    b = bars(d, ("A", "B", "C"))
    nav, trades, held, _ = simulate(b, calendar(), {d[0]: {c:.1 for c in b["open"]}},
        config=ValueConfig(), mode="long_only", entry_limits={d[0]: {c:110 for c in b["open"]}})
    assert len(trades) == 3
    assert np.allclose(trades.cost, trades.notional*.0005)
    assert nav.cost.iloc[-1] == 0
    assert .299 < nav.long_exposure.iloc[0] <= .3
    assert nav.idle_cash_weight.iloc[0] >= .7
    assert held.weight.abs().max() <= .1


def test_rejected_long_reduces_short_target_at_same_open():
    d = pd.to_datetime(["2024-01-05", "2024-01-08"])
    b = bars(d, ("A", "B", "S"))
    nav, trades, _, _ = simulate(b, calendar(), {d[0]: {"A":.1, "B":.1, "S":-.2}},
        config=ValueConfig(borrow_rate=0), mode="long_short",
        entry_limits={d[0]: {"A":99., "B":110.}})
    assert set(trades.code) == {"B", "S"}
    assert nav.net_exposure.iloc[0] == pytest.approx(0)
    assert nav.short_exposure.iloc[0] <= .1


def test_margin_guard_allows_exit_and_blocks_only_increase():
    d = pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-12", "2024-01-15"])
    b = bars(d, ("A",))
    nav, trades, _, _ = simulate(b, calendar(), {d[0]: {"A":.1}, d[2]: {}},
        config=ValueConfig(), mode="long_only", entry_limits={d[0]: {"A":110.}, d[2]: {}})
    assert len(trades) == 2 and trades.quantity.iloc[-1] < 0
    assert nav.long_exposure.iloc[-1] == 0


def test_new_strategy_requires_frozen_buy_limit():
    d = pd.to_datetime(["2024-01-05", "2024-01-08"])
    with pytest.raises(ValueError, match="Missing frozen entry limit"):
        simulate(bars(d, ("A",)), calendar(), {d[0]: {"A":.1}},
                 config=ValueConfig(), mode="long_only")


def test_new_tracking_freeze_and_rerun_preserve_history(tmp_path, monkeypatch):
    data, date, codes = ranking_data()
    dates = pd.to_datetime(["2024-01-26", "2024-01-29", "2024-01-30"])
    data.bars = bars(dates, codes, np.full((3,10),120.))
    data.names = pd.DataFrame([dict(ts_code=c,name="公司",start_date="20200101",
        ann_date="20191231",end_date="") for c in codes])
    snap = pd.DataFrame({"redeem_icon":[""]*10, "delist_dt":[""]*10},index=codes)
    data.risk_snapshot = lambda day: (snap, pd.Timestamp("2024-01-26 23:00"))
    data.signal_dates = lambda start,end,freq: pd.DatetimeIndex([date])
    data.daily_alerts = lambda day,held,fundamentals: {}
    data.metadata = {"cutoff":"2024-01-30", "research_limitations":["fixture"]}
    data.cashflows, data.rf = pd.DataFrame(), pd.DataFrame()
    data.benchmark = pd.DataFrame({"open":[100.]*3, "close":[100.,101.,102.]}, index=dates)
    monkeypatch.setattr(runner, "now_local", lambda: "2024-01-27T10:00:00+08:00")
    monkeypatch.setattr(runner, "observed_average_risk_free_rate", lambda **kwargs: .02)
    config = ValueConfig(risk_mode="tracking")
    runner.run_one(data, config, pd.Timestamp("2024-01-01"), dates[-1], tmp_path)
    state = json.loads((tmp_path/"manifest.json").read_text())
    assert state["specification"]["method"] == runner.VALUE_METHOD
    assert state["specification"]["config"]["transaction_bps"] == 5
    files = [tmp_path/"long_only_trades.csv", tmp_path/"targets.csv", tmp_path/"signals/2024-01-26.json"]
    before = [(p.stat().st_mtime_ns, p.read_bytes()) for p in files]
    monkeypatch.setattr(runner, "now_local", lambda: "2024-01-31T10:00:00+08:00")
    runner.run_one(data, config, pd.Timestamp("2024-01-01"), dates[-1], tmp_path)
    assert before == [(p.stat().st_mtime_ns, p.read_bytes()) for p in files]
    assert len(pd.read_csv(files[0])) == 2
    assert Config().transaction_bps == 10  # legacy research remains unchanged


def test_waiver_announcement_date_is_not_a_redemption():
    from strategy_events import normalize_notices
    raw = pd.DataFrame([
        dict(SECURITY_CODE="113051",SECUCODE="113051.SH",NOTICE_DATE_SH="2026-05-20",
             REDEEM_TYPE=None,RECORD_DATE_SH=None,EXECUTE_PRICE_SH=None),
        dict(SECURITY_CODE="110054",SECUCODE="110054.SH",NOTICE_DATE_SH="2020-03-04",
             REDEEM_TYPE="2",RECORD_DATE_SH="2020-03-16",EXECUTE_PRICE_SH=100.499),
        dict(SECURITY_CODE="UNKNOWN",SECUCODE="UNKNOWN",NOTICE_DATE_SH="2020-03-04",
             REDEEM_TYPE="2",RECORD_DATE_SH=None,EXECUTE_PRICE_SH=None)])
    normalized = normalize_notices(raw,"2026-09-12T10:00:00+08:00")
    assert normalized.ts_code.tolist() == ["110054.SH"]
