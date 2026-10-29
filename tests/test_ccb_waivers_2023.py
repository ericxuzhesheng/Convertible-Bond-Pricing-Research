from pathlib import Path
import sys
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'experiments/ccb_2023'))
from waiver_terms import parse_term,available_date,protection_at


def test_explicit_future_range_not_trigger_history():
    text='自2023年10月16日至2023年11月16日已触发赎回条款。公司决定不提前赎回。未来三个月内（即2023年11月17日至2024年2月16日），均不行使提前赎回权利。'
    r=parse_term(text,'2023-11-17')
    assert r['status']=='explicit' and r['start']=='2023-11-17' and r['end']=='2024-02-16'


def test_old_promise_does_not_get_projected_forward():
    text='前次未来三个月内（自2023年7月25日至2023年10月24日期间）不提前赎回。此次决定不提前赎回，且未来三个月内均不行使提前赎回权利。'
    assert parse_term(text,'2023-11-15')['status']=='no_explicit_future_period'


def test_no_default_quarter_and_no_silent_noncall_inference():
    assert parse_term('本次不提前赎回。','2020-01-03')['status']=='no_explicit_future_period'
    assert parse_term('已满足赎回条件。','2020-01-03')['status']=='no_explicit_future_period'


def test_explicit_duration_requires_anchor():
    text='自2023年11月15日起三个月内不行使提前赎回权利。'
    r=parse_term(text,'2023-11-15')
    assert r['end']=='2024-02-14'


def test_year_end_is_explicit_and_trading_disclosure_not_new_decision():
    assert parse_term('公司决定本年度内不提前赎回本次转债。','2020-06-01')['end']=='2020-12-31'
    assert parse_term('未来半年不提前赎回','2020-06-01','关于不提前赎回前股东交易情况补充公告')['status']=='informational_notice'


def test_date_only_after_close_and_holiday_availability():
    calendar=pd.DatetimeIndex(pd.to_datetime(['2023-09-28','2023-10-09','2023-10-10']))
    assert available_date('2023-09-28 16:00:00+08:00',calendar)[0]==pd.Timestamp('2023-10-09')
    assert available_date('2023-09-28 00:00:00+08:00',calendar)[0]==pd.Timestamp('2023-10-09')
    assert available_date('2023-09-28 10:00:00+08:00',calendar)[0]==pd.Timestamp('2023-09-28')


def events(rows):
    f=pd.DataFrame(rows)
    for col in ['available_date','start','end']:f[col]=pd.to_datetime(f[col])
    return f


def test_asof_and_expiry_and_no_extra_month_after_commitment():
    f=events([dict(announcement_id='1',available_date='2023-11-20',start='2023-11-17',end='2024-02-16',status='explicit')])
    assert protection_at(f,'2023-11-17',1/12)[0]==1/12
    t,state,source=protection_at(f,'2023-11-24',1/12)
    assert t==(pd.Timestamp('2024-02-17')-pd.Timestamp('2023-11-24')).days/365
    assert state=='explicit_active' and source=='1'
    assert protection_at(f,'2024-02-23',1/12)[0]==1/12


def test_unknown_correction_cannot_reuse_old_promise():
    f=events([dict(announcement_id='1',available_date='2023-11-20',start='2023-11-17',end='2024-02-16',status='explicit'),
              dict(announcement_id='2',available_date='2023-11-21',start=None,end=None,status='correction_review_required')])
    assert protection_at(f,'2023-11-24',1/12)[1]=='unresolved_latest_notice'


def test_informational_notice_does_not_override_decision():
    f=events([dict(announcement_id='1',available_date='2023-11-20',start='2023-11-17',end='2024-02-16',status='explicit'),
              dict(announcement_id='2',available_date='2023-11-21',start=None,end=None,status='informational_notice')])
    assert protection_at(f,'2023-11-24',1/12)[1]=='explicit_active'


def test_contiguous_explicit_extension_not_conflicting_or_backdated():
    text='自2021年8月24日至2021年11月23日不提前赎回。公司决定继续不提前赎回，自2021年11月24日至2022年2月23日均不行使赎回权利。'
    r=parse_term(text,'2021-11-16')
    assert r['status']=='explicit' and r['end']=='2022-02-23'
    f=events([dict(announcement_id='extension',available_date='2021-11-17',**r)])
    assert protection_at(f,'2021-11-12',1/12)[0]==1/12


def test_correction_uses_only_corrected_commitment():
    text='更正前：2024年11月15日至2024年12月14日不提前赎回。更正后：2024年11月15日至2025年2月14日不提前赎回。'
    r=parse_term(text,'2024-11-16','不提前赎回的更正公告')
    assert r['status']=='explicit' and r['end']=='2025-02-14'


def test_correction_without_new_period_requires_review():
    assert parse_term('更正后：触发条件为十五个交易日。其他事项不变。','2024-11-16','不提前赎回的更正公告')['status']=='correction_review_required'


def test_until_maturity_requires_explicit_promise_and_matching_date():
    text='转债将于2026年11月19日到期，自本次董事会审议通过之日起至洪城转债到期日止，均不行使提前赎回权利。'
    r=parse_term(text,'2026-08-01',maturity='2026-11-19')
    assert r['status']=='explicit' and r['end']=='2026-11-19'
    # Explicit PDF date is authoritative even if vendor maturity is payment day.
    assert parse_term(text,'2026-08-01',maturity='2026-11-20')['end']=='2026-11-19'
    assert parse_term('公司将于2026年11月19日到期，届时兑付本金。','2026-08-01',maturity='2026-11-20')['status']=='no_explicit_future_period'


def test_later_shorter_corrected_period_supersedes_old_end():
    f=events([dict(announcement_id='old',available_date='2023-11-20',start='2023-11-17',end='2024-05-16',status='explicit'),
              dict(announcement_id='corrected',available_date='2023-11-21',start='2023-11-17',end='2024-02-16',status='explicit')])
    assert protection_at(f,'2023-11-24',1/12)[0]==85/365
    assert protection_at(f,'2024-03-01',1/12)[0]==1/12
