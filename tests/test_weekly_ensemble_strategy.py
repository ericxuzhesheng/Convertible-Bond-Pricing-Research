from __future__ import annotations
import sys
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"backtest"))
sys.path.insert(0, str(ROOT/"long-short strategy"))
from strategy_data import SourceError, validate_daily, merge_financial_versions, download_daily
from weekly_strategy_core import (Config, prepare_financial, financial_asof, next_session,
    select_targets, simulate, validate_cashflows, performance, build_ranking)
from weekly_ensemble_strategy import certify_append

def calendar():
    return pd.bdate_range("2024-01-01", "2024-02-29")

def bars(dates, codes=("A", "B"), closes=None):
    c = pd.DataFrame(100., index=pd.DatetimeIndex(dates), columns=list(codes))
    if closes is not None:
        c.loc[:, :] = np.array(closes)
    return {"open": c.copy(), "close": c.copy(), "high": c+1, "low": c-1,
            "vol": c*10, "amount": c*100}

def run(b, targets, **kwargs):
    return simulate(b, calendar(), targets, config=kwargs.pop("config", Config(transaction_bps=0, borrow_rate=0)),
                    mode=kwargs.pop("mode", "long_only"), **kwargs)

def test_filter_then_quantile_without_sign_restriction():
    rows = pd.DataFrame({"signal": [-i for i in range(15)], "eligible": [False]*5+[True]*10},
                        index=[f"C{i:02}" for i in range(15)])
    long, ls = select_targets(rows)
    assert long == {"C05": .1, "C06": .1}
    assert ls["C13"] == -.1 and ls["C14"] == -.1
    assert sum(long.values()) == .2

def test_ties_deterministic_and_no_overlapping_legs():
    r = pd.DataFrame({"signal": [1]*5, "eligible": [True]*5}, index=list("EDCBA"))
    long, ls = select_targets(r)
    assert long == {"A": .1} and ls["E"] == -.1
    assert select_targets(r.iloc[:4]) == ({}, {})

def test_next_session_handles_holidays():
    cal = pd.to_datetime(["2024-02-08", "2024-02-19", "2024-02-20"])
    assert next_session(pd.Timestamp("2024-02-08"), cal) == cal[1]

def test_announcements_lag_and_latest_report_period():
    e = pd.DataFrame([
        dict(ts_code="S", ann_date="20240105", end_date="20231231", bps=2, eps=1, profit_dedt=3, roe=1,debt_to_assets=40,ocfps=2),
        dict(ts_code="S", ann_date="20240109", end_date="20230930", bps=99,eps=1,profit_dedt=3,roe=1,debt_to_assets=40,ocfps=2)])
    p = prepare_financial(e, calendar())
    assert financial_asof(p, pd.Timestamp("2024-01-05")).empty
    assert financial_asof(p, pd.Timestamp("2024-01-08")).loc["S","bps"] == 2
    assert financial_asof(p, pd.Timestamp("2024-01-10")).loc["S","bps"] == 2

def test_revision_never_backdates():
    r = dict(ts_code="S", ann_date="20240105", end_date="20231231", bps=2,eps=1,
             profit_dedt=3,roe=1,debt_to_assets=40,ocfps=2,fetched_at="2024-01-06T12:00:00+08:00",available_at="")
    old = pd.DataFrame([r])
    new = pd.DataFrame([{**r, "bps": 1, "fetched_at": "2024-01-15T12:00:00+08:00"}])
    merged = merge_financial_versions(old,new)
    assert len(merged) == 2
    prepared = prepare_financial(merged,calendar())
    assert financial_asof(prepared,pd.Timestamp("2024-01-12")).loc["S","bps"] == 2
    assert financial_asof(prepared,pd.Timestamp("2024-01-16")).loc["S","bps"] == 1

def test_weekend_signal_executes_next_open_and_daily_marking():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09","2024-01-10"])
    b = bars(d, ("A",), [[90],[100],[120],[90]])
    nav,trades,held,_ = run(b,{d[0]:{"A":.1}})
    assert trades.iloc[0].date == d[1] and trades.iloc[0].price == 100
    assert len(nav) == 3
    assert nav.nav.tolist() == pytest.approx([1,1.02,.99])
    assert len(trades) == 1
    assert held.quantity.nunique() == 1
    assert performance(nav)["max_drawdown"] < -.029

def test_future_signal_only_has_pending_targets():
    d = pd.to_datetime(["2024-01-05"])
    outputs = run(bars(d), {d[0]:{"A":.1}})
    assert all(frame.empty for frame in outputs)

def test_transaction_cost_only_actual_changes():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09"])
    nav,trades,_,_ = run(bars(d),{d[0]:{"A":.1}},config=Config(transaction_bps=10,borrow_rate=0))
    expected_fee = 100/1.001  # fee reserve keeps the holding below 10% of after-cost NAV
    assert trades.cost.sum() == pytest.approx(expected_fee)
    assert nav.cost.tolist() == pytest.approx([expected_fee/1e6,0])
    assert nav.nav.iloc[-1] == pytest.approx(1-expected_fee/1e6)

def test_short_sign_and_calendar_day_borrow():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09","2024-01-12","2024-01-15"])
    b = bars(d,("A",),[[100],[100],[90],[90],[90]])
    nav,trades,_,_ = run(b,{d[0]:{"A":-.1}},mode="long_short",
                         config=Config(transaction_bps=0,borrow_rate=.03))
    assert trades.quantity.iloc[0] < 0
    assert nav.nav.iloc[1] > 1
    assert nav.borrow_cost.iloc[-1] == pytest.approx(90000*.03*3/365/1000000)

def test_missing_exit_keeps_holding_not_fictional_sale():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-12","2024-01-15"])
    b = bars(d,("A",))
    for field in b:
        b[field].loc[d[-1],"A"] = np.nan
    nav,trades,held,issues = run(b,{d[0]:{"A":.1},d[2]:{}})
    assert len(trades) == 1 and held.iloc[-1].quantity > 0
    assert not nav.iloc[-1].marked_prices_verified
    assert held.iloc[-1].quote_age_days == 3
    assert "unverified_close_carry_mark_not_exit" in set(issues.reason)

def test_one_price_bar_is_not_assumed_executable():
    d = pd.to_datetime(["2024-01-05","2024-01-08"])
    b = bars(d,("A",)); b["high"].loc[d[1],"A"]=100; b["low"].loc[d[1],"A"]=100
    nav,trades,_,issues = run(b,{d[0]:{"A":.1}})
    assert trades.empty and nav.nav.iloc[-1] == 1
    assert len(issues) == 1

def test_cash_is_a_real_empty_portfolio_not_missing_data():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09"])
    nav,_,_,_ = run(bars(d),{d[0]:{}})
    assert nav.nav.tolist() == [1,1]

def test_coupon_entitlement_survives_sale_and_no_double_count():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09","2024-01-10"])
    event = pd.DataFrame([dict(event_id="C",ts_code="A",kind="coupon",record_date="20240108",
        ex_date="20240109",payment_date="20240110",amount_per_bond=1,verified=True,source="issuer")])
    nav,trades,_,_ = run(bars(d,("A",)),{d[0]:{"A":.1},d[1]:{}},cashflows=event)
    assert len(trades) == 2
    assert nav.nav.iloc[-1] == pytest.approx(1.001)
    assert nav.receivables.tolist() == pytest.approx([0,.001,0])

def test_short_coupon_is_a_liability():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09","2024-01-10"])
    event = pd.DataFrame([dict(event_id="C",ts_code="A",kind="coupon",record_date="20240108",
        ex_date="20240109",payment_date="20240110",amount_per_bond=1,verified=True,source="issuer")])
    nav,_,_,_ = run(bars(d,("A",)),{d[0]:{"A":-.1}},cashflows=event)
    assert nav.nav.iloc[-1] == pytest.approx(.999)

def test_redemption_extinguishes_quantity_once():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09","2024-01-10"])
    event = pd.DataFrame([dict(event_id="R",ts_code="A",kind="redemption",record_date="20240108",
        ex_date="20240109",payment_date="20240110",amount_per_bond=105,verified=True,source="issuer")])
    b = bars(d,("A",))
    b["close"].loc[d[2]:,"A"]=np.nan
    nav,trades,held,_ = run(b,{d[0]:{"A":.1}},cashflows=event)
    assert nav.nav.iloc[-1] == pytest.approx(1.005)
    assert held.date.max() == d[1]
    assert (trades.reason == "verified_redemption").sum() == 1

def test_invalid_cashflow_rejected():
    event = pd.DataFrame([dict(event_id="R",ts_code="A",kind="coupon",record_date="20240110",
        ex_date="20240109",payment_date="20240110",amount_per_bond=1,verified=True,source="issuer")])
    with pytest.raises(ValueError):
        validate_cashflows(event)

def test_daily_event_exits_only_next_session():
    d = pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09"])
    def alerts(date,held):
        return {"A":"call"} if date==d[1] and "A" in held else {}
    _,trades,_,_ = run(bars(d,("A",)),{d[0]:{"A":.1}},alert_fn=alerts)
    assert trades.iloc[-1].date == d[2] and trades.iloc[-1].reason == "risk_exit"

def test_all_daily_partitions_downloaded_not_just_rebalance_days(tmp_path):
    dates = ["20240108","20240109","20240110","20240111","20240112"]
    class Reader:
        def query(self,api,fields,**kwargs):
            return pd.DataFrame([dict(ts_code="A",trade_date=d,open=100,high=101,low=99,
                close=100,vol=10,amount=1000) for d in dates])
    download_daily(Reader(),tmp_path,dates)
    assert len(list((tmp_path/"daily").glob("*.csv"))) == 5

def test_invalid_daily_high_low_rejected():
    r = pd.DataFrame([dict(ts_code="A",trade_date="20240108",open=100,high=99,low=98,close=100,vol=1,amount=10)])
    with pytest.raises(SourceError):
        validate_daily(r,"20240108")

def test_append_guard_preserves_confirmed_history(tmp_path):
    path = tmp_path/"daily.csv"
    old = pd.DataFrame({"date":pd.to_datetime(["2024-01-08"]),"nav":[1.]})
    old.to_csv(path,index=False)
    extended = pd.DataFrame({"date":pd.to_datetime(["2024-01-08","2024-01-09"]),"nav":[1.,1.1]})
    certify_append(path,extended)
    extended.loc[0,"nav"]=.9
    with pytest.raises(ValueError,match="history changed"):
        certify_append(path,extended)

def ranking_data():
    date = pd.Timestamp("2024-01-26")
    codes = [f"{i:06}.SH" for i in range(10)]
    ix = pd.DatetimeIndex([date])
    px = pd.DataFrame(120.,index=ix,columns=codes)
    features = {k:pd.DataFrame(v,index=ix,columns=codes)
                for k,v in {"rating":"AA","floor":100.,"term":1.,"balance":30000.}.items()}
    basic = pd.DataFrame({"bond_short_name":codes,"stk_cd":codes,"bond_full_name":["公司"]*10,
                          "list_date":["2020-01-01"]*10},index=codes)
    fin = pd.DataFrame([dict(ts_code=c,ann_date="20240101",end_date="20231231",bps=2,eps=1,
        profit_dedt=2,roe=1,debt_to_assets=70,ocfps=1) for c in codes])
    obj = SimpleNamespace(price=px, model={m:px+10 for m in ("BS","ZL","LSM")},
        model_market={m:px.copy() for m in ("BS","ZL","LSM")},features=features,
        bars={"close":px,"amount":px*10},amount_mean=px*10,amount_count=px*0+20,basic=basic,
        financial=prepare_financial(fin,calendar()),names=pd.DataFrame(),calendar=calendar(),
        risk_snapshot=lambda d:(pd.DataFrame(),pd.NaT))
    return obj,date,codes

def test_ranking_mean_units_and_fundamentals():
    data,date,codes=ranking_data()
    rank=build_ranking(data,date,Config())
    assert rank.eligible.all()
    assert rank.signal.iloc[0] == pytest.approx(130/120-1)
    data.features["balance"].loc[date,codes[0]]=29999
    data.financial.loc[data.financial.ts_code.eq(codes[1]),"profit_dedt"]=-1
    data.model["BS"].loc[date,codes[2]]=np.nan
    data.model_market["ZL"].loc[date,codes[3]]=121
    result=build_ranking(data,date,Config())
    assert not result.loc[codes[:4],"eligible"].any()

def test_tracking_unknown_events_are_not_safe():
    data,date,codes=ranking_data()
    assert not build_ranking(data,date,Config(risk_mode="tracking")).eligible.any()

def test_drift_is_not_daily_rebalancing():
    d=pd.to_datetime(["2024-01-05","2024-01-08","2024-01-09"])
    b=bars(d,("A",),[[100],[100],[200]])
    _,trades,holdings,_=run(b,{d[0]:{"A":.1}})
    assert len(trades)==1
    assert holdings.weight.iloc[-1] > .1



def test_provider_close_only_bar_preserved_but_not_executable():
    f=pd.DataFrame([dict(ts_code="124017.SZ",trade_date="20221115",open=0.,high=0.,low=0.,close=104.67,vol=130000.,amount=13607.5)])
    validate_daily(f,"20221115")
    assert f.bar_quality.iloc[0] == "close_only_no_open_quote"
    assert f.open.iloc[0] == 0 and f.close.iloc[0] == 104.67


def test_tracking_roundtrip_is_idempotent(tmp_path, monkeypatch):
    import weekly_ensemble_strategy as runner
    data,date,codes = ranking_data()
    dates = pd.to_datetime(["2024-01-26","2024-01-29","2024-01-30"])
    data.bars = bars(dates, codes, np.full((3,10),120.))
    data.names = pd.DataFrame([dict(ts_code=c,name="公司",start_date="20200101",
        ann_date="20191231",end_date="") for c in codes])
    snap = pd.DataFrame({"redeem_icon":[""]*10,"delist_dt":[""]*10},index=codes)
    data.risk_snapshot = lambda day:(snap,pd.Timestamp("2024-01-26 23:00"))
    data.signal_dates = lambda start,end,freq:pd.DatetimeIndex([date])
    data.daily_alerts = lambda day,held,fundamentals: {}
    data.metadata = {"cutoff":"2024-01-30","research_limitations":["fixture"]}
    data.cashflows = pd.DataFrame()
    data.rf = pd.DataFrame()
    data.benchmark = pd.DataFrame({"open":[100.,100.,100.],"close":[100.,101.,102.]},index=dates)
    monkeypatch.setattr(runner,"now_local",lambda:"2024-01-27T10:00:00+08:00")
    monkeypatch.setattr(runner,"observed_average_risk_free_rate",lambda **kwargs:.02)
    config=Config(risk_mode="tracking")
    runner.run_one(data,config,pd.Timestamp("2024-01-01"),dates[-1],tmp_path)
    path=tmp_path/"long_short_daily_nav.csv"
    original=path.read_bytes()
    modification=path.stat().st_mtime_ns
    trades=pd.read_csv(tmp_path/"long_short_trades.csv")
    assert len(trades)==4
    # Later reruns reuse the earlier frozen signal even after its execution time.
    monkeypatch.setattr(runner,"now_local",lambda:"2024-01-31T10:00:00+08:00")
    runner.run_one(data,config,pd.Timestamp("2024-01-01"),dates[-1],tmp_path)
    assert path.read_bytes()==original
    assert path.stat().st_mtime_ns==modification
    assert len(pd.read_csv(tmp_path/"long_short_trades.csv"))==4

def test_daily_fetch_splits_at_provider_row_limit(tmp_path):
    dates=["20240108","20240109"]
    class Reader:
        calls=0
        def query(self,api,fields,**kwargs):
            self.calls+=1
            rows=[dict(ts_code=f"B{i}",trade_date=d,open=100,high=101,low=99,
                       close=100,vol=10,amount=1000) for d in dates
                  if kwargs["start_date"] <= d <= kwargs["end_date"] for i in range(1001)]
            return pd.DataFrame(rows).head(2000)
    reader=Reader()
    download_daily(reader,tmp_path,dates)
    assert reader.calls==3
    assert all(len(pd.read_csv(tmp_path/"daily"/f"{d}.csv"))==1001 for d in dates)

def test_nonfinite_fundamentals_cannot_pass():
    data,date,codes=ranking_data()
    data.financial["bps"] = data.financial["bps"].astype(float)
    data.financial.loc[data.financial.ts_code.eq(codes[0]),"bps"]=np.inf
    # Public financial preparation sanitizes nonfinite provider fields.
    prepared=prepare_financial(data.financial,calendar())
    assert pd.isna(prepared.loc[prepared.ts_code.eq(codes[0]),"bps"]).all()

def test_full_leg_reserves_fees_and_cannot_borrow_to_buy():
    codes=[f"B{i:02}" for i in range(10)]
    d=pd.to_datetime(["2024-01-05","2024-01-08"])
    nav,_,held,_=run(bars(d,codes),{d[0]:{c:.1 for c in codes}},
                    config=Config(transaction_bps=20,borrow_rate=0))
    assert nav.cash.iloc[-1]>=0
    assert nav.long_exposure.iloc[-1]<=1
    assert held.weight.abs().max()<=.1+1e-12

def test_bank_exemption_does_not_waive_profit_requirement():
    data,date,codes=ranking_data()
    data.basic.loc[codes[0],"bond_full_name"]="某银行股份公司"
    data.financial.loc[data.financial.ts_code.eq(codes[0]),["debt_to_assets","ocfps"]]=[95,-1]
    assert build_ranking(data,date,Config()).loc[codes[0],"eligible"]
    data.financial.loc[data.financial.ts_code.eq(codes[0]),"profit_dedt"]=-1
    assert not build_ranking(data,date,Config()).loc[codes[0],"eligible"]


def test_redemption_notice_is_not_visible_before_publication():
    from weekly_strategy_core import redemption_asof
    events=pd.DataFrame([dict(ts_code='A',ann_date='2020-03-04',is_call='announced_redemption',available_at='')])
    assert not redemption_asof(events,pd.Timestamp('2020-03-04'))
    assert redemption_asof(events,pd.Timestamp('2020-03-05')) == {'A'}
    events['available_at']='2026-09-12T10:00:00+08:00'
    assert not redemption_asof(events,pd.Timestamp('2020-03-05'))


def test_known_historical_redemption_excluded_in_research():
    data,date,codes=ranking_data()
    data.calls=pd.DataFrame([dict(ts_code=codes[0],ann_date='2024-01-20',is_call='announced_redemption',available_at='')])
    ranking=build_ranking(data,date,Config())
    assert not ranking.loc[codes[0],'eligible']
    assert 'known_redemption_clear' in ranking.loc[codes[0],'excluded_by']


def test_late_notice_cannot_be_backfilled():
    from strategy_events import normalize_notices,merge_notices
    old=pd.DataFrame([dict(ts_code='A',ann_date='2020-01-01',available_at='',is_call='announced_redemption')])
    new=pd.DataFrame([dict(ts_code='B',ann_date='2020-01-01',available_at='',is_call='announced_redemption')])
    merged=merge_notices(old,new,'2026-09-12T10:00:00+08:00')
    assert merged.iloc[0].available_at == ''
    assert merged.iloc[1].available_at.startswith('2026-09-12')


def test_unverified_prices_withhold_formal_performance():
    from weekly_strategy_core import performance
    d=pd.to_datetime(['2024-01-05','2024-01-08','2024-01-09'])
    b=bars(d,('A',),[[100],[100],[np.nan]])
    nav,_,_,_=run(b,{d[0]:{'A':.1}})
    result=performance(nav,0)
    assert result['annual_return'] is None
    assert result['max_drawdown'] is None
    assert result['unverified_mark_days']==1

def test_verified_suspension_explains_mark_without_creating_trade():
    d=pd.to_datetime(['2024-01-05','2024-01-08','2024-01-12','2024-01-15'])
    b=bars(d,('A',))
    for field in b:b[field].loc[d[-1],'A']=np.nan
    evidence=pd.DataFrame([dict(ts_code='A',start_date='2024-01-15',end_date='2024-01-15',verified=True,source='issuer_notice')])
    nav,trades,held,issues=run(b,{d[0]:{'A':.1},d[2]:{}},suspensions=evidence)
    assert len(trades)==1 and held.iloc[-1].quantity>0
    assert nav.iloc[-1].marked_prices_verified
    assert held.iloc[-1].quote_age_days==3
    assert performance(nav)['stale_mark_days']==1
    assert 'verified_suspension_stale_mark' in set(issues.reason)


def test_caps_use_after_cost_nav_for_both_legs():
    codes=[f'B{i:02}' for i in range(20)]
    d=pd.to_datetime(['2024-01-05','2024-01-08','2024-01-12','2024-01-15'])
    weights={c:(.1 if i<9 else -.1) for i,c in enumerate(codes[:18])}
    changed={c:-w for c,w in weights.items()}
    nav,_,held,_=run(bars(d,codes),{d[0]:weights,d[2]:changed},mode='long_short',
        config=Config(transaction_bps=20,borrow_rate=0))
    assert held.weight.abs().max()<=.1+1e-12
    assert nav.long_exposure.max()<=1 and nav.short_exposure.max()<=1


def test_daily_risk_exit_never_rebalances_unaffected_bond():
    d=pd.to_datetime(['2024-01-05','2024-01-08','2024-01-09'])
    b=bars(d,('A','B'),[[100,100],[100,100],[100,120]])
    nav,trades,held,_=run(b,{d[0]:{'A':.1,'B':.1}},config=Config(transaction_bps=10,borrow_rate=0),
        alert_fn=lambda day,held: {'A':'risk'} if day==d[1] else {})
    assert len(trades.loc[trades.date.eq(d[-1])])==1
    assert trades.loc[trades.date.eq(d[-1]),'code'].iloc[0]=='A'
    assert held.loc[held.code.eq('B'),'quantity'].nunique()==1


def test_pending_only_crlf_metrics_repeat_without_rewrite(tmp_path):
    from dataclasses import asdict
    import json
    import weekly_ensemble_strategy as runner
    config=Config(risk_mode='tracking')
    data=SimpleNamespace(metadata={'cutoff':'2024-01-26'})
    start,end=pd.Timestamp('2024-01-01'),pd.Timestamp('2024-01-26')
    (tmp_path/'manifest.json').write_text(json.dumps({'engine_revision':runner.ENGINE_REVISION,
        'input_cutoff':'2024-01-26','requested_end':'2024-01-26','input_vintage':None,
        'specification':{'method':runner.METHOD,'config':asdict(config),'start':'2024-01-01','mode':'both'}}))
    p=tmp_path/'metrics.csv';p.write_bytes(b'\r\n');before=p.stat().st_mtime_ns
    assert runner.run_one(data,config,start,end,tmp_path).empty
    assert p.stat().st_mtime_ns==before
