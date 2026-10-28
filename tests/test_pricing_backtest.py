import json
from pathlib import Path
import sys
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"backtest"))
from pricing_engine import Contract
from pricing_inputs import contract_date, PricingInputs
from pricing_backtest import options, run_model, validate_outputs
from market_data_contracts import DataContractError


class Inputs:
    def __init__(self):
        self.market = pd.DataFrame({"A":[105.,106.]},index=pd.to_datetime(["2024-01-05","2024-01-12"]))
        self.changed = False

    def dates(self,start,end):
        return self.market.index[self.market.index<=pd.Timestamp(end)]

    def fingerprint(self,cutoff,params):
        return str(cutoff.date())+str(self.changed)

    def contract(self,code,date):
        return Contract(parity=80.,maturity=1.,redemption=110.,sigma=0.,spread=0.,curve_rates=np.zeros(2))

    def clause_contract(self,code,date,base,**kwargs):
        return base


def args(model,tmp_path,end,initialize=False):
    argv=["--weekly","--output-dir",str(tmp_path),"--end",end,"--paths","16","--workers","1"]
    if initialize: argv.append("--initialize-history")
    return options(model,argv)


@pytest.mark.parametrize("model",["BS","ZL","LSM"])
def test_independent_backtest_appends_and_unchanged_run_does_not_rewrite(model,tmp_path):
    data=Inputs()
    run_model(model,args(model,tmp_path,"2024-01-05",True),data)
    first=pd.read_csv(tmp_path/f"{model}_Model_Prices.csv",index_col=0)
    run_model(model,args(model,tmp_path,"2024-01-12"),data)
    second=pd.read_csv(tmp_path/f"{model}_Model_Prices.csv",index_col=0)
    pd.testing.assert_frame_equal(first,second.loc[first.index])
    before={p.name:(p.stat().st_mtime_ns,p.read_bytes()) for p in tmp_path.glob('*') if p.is_file()}
    run_model(model,args(model,tmp_path,"2024-01-12"),data)
    assert before=={p.name:(p.stat().st_mtime_ns,p.read_bytes()) for p in tmp_path.glob('*') if p.is_file()}
    assert validate_outputs(model,tmp_path)["input_cutoff"]=="2024-01-12"
    assert not list(tmp_path.glob('ZL*')) if model=="LSM" else True


def test_changed_historical_inputs_and_tampered_prices_fail_closed(tmp_path):
    data=Inputs()
    run_model("LSM",args("LSM",tmp_path,"2024-01-05",True),data)
    data.changed=True
    with pytest.raises(DataContractError,match="verified history changed"):
        validate_outputs('LSM',tmp_path,data)
    with pytest.raises(DataContractError,match="verified history changed"):
        run_model("LSM",args("LSM",tmp_path,"2024-01-12"),data)
    data.changed=False
    path=tmp_path/'LSM_Model_Prices.csv'
    path.write_text(path.read_text().replace('110.0','111.0'))
    with pytest.raises(DataContractError,match="artifact integrity"):
        run_model("LSM",args("LSM",tmp_path,"2024-01-12"),data)


def test_cpu_increment_preserves_a_xoroshiro_cuda_baseline_sampler(tmp_path):
    data=Inputs()
    first=args('LSM',tmp_path,'2024-01-05',True)
    first.sampler='xoroshiro128p'
    run_model('LSM',first,data)
    old=pd.read_csv(tmp_path/'LSM_Model_Prices.csv',index_col=0)
    run_model('LSM',args('LSM',tmp_path,'2024-01-12'),data)
    manifest=validate_outputs('LSM',tmp_path)
    assert 'xoroshiro128p' in manifest['model_parameters']['seed_scheme']
    assert manifest['execution_backend']=='cpu'
    new=pd.read_csv(tmp_path/'LSM_Model_Prices.csv',index_col=0)
    pd.testing.assert_frame_equal(old,new.loc[old.index])


@pytest.mark.parametrize('manifest',[{'contract_version':'weekly-lsm-v1'},
    {'contract_version':'cashflow-pricing-v2','model_parameters':{'implementation':'cashflow-stopping-v2.3'}}])
def test_legacy_contract_is_never_appended_with_corrected_prices(tmp_path,manifest):
    (tmp_path/'LSM_Model_Manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(DataContractError,match="legacy pricing contract"):
        run_model("LSM",args("LSM",tmp_path,"2024-01-12"),Inputs())


def test_encoded_float_dates_keep_the_contractual_calendar_date():
    assert contract_date('20201022.0')==pd.Timestamp('2020-10-22')
    assert contract_date(20201022.)==pd.Timestamp('2020-10-22')
    assert contract_date('2020-10-22')==pd.Timestamp('2020-10-22')


def test_effective_curve_yields_preserve_annual_discount_factors():
    data=PricingInputs.__new__(PricingInputs)
    day=pd.Timestamp('2024-01-05')
    data.curve=pd.DataFrame({1.:[.05],5.:[.06]},index=[day])
    data._curve_cache={}
    tenors,rates=data.observed_curve(day)
    assert np.exp(-rates[0])==pytest.approx(1/1.05)
    assert np.exp(-rates[1]*5)==pytest.approx(1/1.06**5)


def test_explicit_no_price_put_contract_is_not_given_a_default_put():
    data=PricingInputs.__new__(PricingInputs)
    day=pd.Timestamp('2024-01-05')
    data.basic=pd.DataFrame({'maturity_date':[pd.Timestamp('2029-01-05')],
        'conv_start_date':[pd.Timestamp('2023-01-05')]},index=['A'])
    data.frames={'parity':pd.DataFrame({'A':[140.]},index=[day])}
    terms=dict(source_ok=True,put_trigger_ratio=np.nan,put_window_days=np.nan,
        put_eligible_years=np.nan,redeem_trigger_ratio=1.3,redeem_window_days=30,
        redeem_required_days=15,resale_clause='募集资金用途变更可以回售。除此之外，可转债不可由持有人主动回售。')
    data.clauses=pd.DataFrame([terms],index=['A'])
    base=Contract(parity=140.,maturity=5.,redemption=110.,sigma=.2,spread=.01)
    result=data.clause_contract('A',day,base)
    assert result.put_start==np.inf and result.put_ratio==0
    assert result.call_ratio==1.3
    data.clauses.at['A','resale_clause']='仅有不完整文字'
    with pytest.raises(DataContractError,match='incomplete observed clause'):
        data.clause_contract('A',day,base)


@pytest.mark.parametrize('prefix',['自本次可转债','本次发行的可转债'])
def test_ordinal_put_start_and_fixed_full_cash_amount(prefix):
    data=PricingInputs.__new__(PricingInputs);day=pd.Timestamp('2024-01-05')
    data.basic=pd.DataFrame({'value_date':[pd.Timestamp('2020-06-01')],
        'maturity_date':[pd.Timestamp('2026-06-01')],
        'conv_start_date':[pd.Timestamp('2020-12-01')]},index=['A'])
    data.frames={'parity':pd.DataFrame({'A':[60.]},index=[day])}
    data.clauses=pd.DataFrame([dict(source_ok=True,put_trigger_ratio=.7,put_window_days=30,
        put_eligible_years=np.nan,redeem_trigger_ratio=1.3,redeem_window_days=30,
        redeem_required_days=15,resale_clause=prefix+'第三个计息年度起，按103元(含当期应计利息)回售。')],index=['A'])
    c=data.clause_contract('A',day,Contract(parity=60.,maturity=2.,redemption=110.,sigma=.2,spread=0.))
    assert c.put_start==0. and c.put_redemption==103. and c.put_includes_accrued


def test_input_fingerprint_is_stable_across_platform_newlines(monkeypatch):
    import os
    data=PricingInputs.__new__(PricingInputs);day=pd.Timestamp('2024-01-05')
    data.market=pd.DataFrame({'A':[100.]},index=[day]);data.frames={'market':data.market}
    data.curve=pd.DataFrame({1.:[.02]},index=[day])
    fields=['par_value','value_date','maturity_date','maturity_call_price','rate_clause',
        'interest_freq','conv_start_date','conv_end_date','list_date','delist_date']
    data.basic=pd.DataFrame({key:['100'] for key in fields},index=['A'])
    terms=['source_ok','put_trigger_ratio','put_window_days','put_eligible_years',
        'redeem_trigger_ratio','redeem_window_days','redeem_required_days','resale_clause','redeem_clause']
    data.clauses=pd.DataFrame({key:['130%\r\n条款'] if key.endswith('clause') else [1.] for key in terms},index=['A'])
    monkeypatch.setattr(os,'linesep','\r\n');first=data.fingerprint(day,{})
    monkeypatch.setattr(os,'linesep','\n')
    for key in ('resale_clause','redeem_clause'):
        data.clauses[key]=data.clauses[key].str.replace('\r\n','\n',regex=False)
    assert data.fingerprint(day,{})==first
    data.basic.at['A','delist_date']=pd.Timestamp('2024-02-01')
    assert data.fingerprint(day,{})==first
    data.basic.at['A','delist_date']=pd.Timestamp('2024-01-05')
    assert data.fingerprint(day,{})!=first
    data.basic.at['A','delist_date']=pd.NaT
    data.clauses.at['A','redeem_clause']='140%条款'
    assert data.fingerprint(day,{})!=first
