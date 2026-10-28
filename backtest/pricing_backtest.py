"""Independent, versioned, cache-only backtests for corrected BS/ZL/LSM."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import zlib
import numpy as np
import pandas as pd
from pricing_engine import price_bs, price_mc
from pricing_inputs import IMPLEMENTATION, PricingInputs
from market_data_contracts import (DataContractError, validate_pricing_coverage,
    ZL_MIN_PRICING_COVERAGE, ZL_HISTORICAL_MIN_COVERAGE, ZL_MIN_COVERAGE_ENFORCED_FROM)

if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = Path(__file__).resolve().parent
CONTRACT_VERSION = "cashflow-pricing-v2"


def implementation_for(model):
    return 'cashflow-stopping-lsm-v2.4' if model=='LSM' else IMPLEMENTATION


def options(model, argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--weekly", action="store_true", required=True)
    p.add_argument("--initialize-history", "--rebuild-all", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--start", default="2019-01-01")
    p.add_argument("--end")
    p.add_argument("--output-dir", type=Path, default=BASE)
    p.add_argument("--input-dir", type=Path, default=BASE)
    p.add_argument("--paths", type=int, default=8192 if model == "LSM" else 10000)
    p.add_argument("--exercise-steps", type=int, default=192)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--call-notice-days", type=int, default=30)
    p.add_argument("--backend", choices=["cpu","cuda"], default="cpu")
    p.add_argument("--sampler", choices=["mt19937","xoroshiro128p"])
    # Old automation may forward these; observed inputs remain cache-only.
    p.add_argument("--offline-inputs", action="store_true")
    p.add_argument("--incremental-after")
    a = p.parse_args(argv)
    if a.paths < 8 or a.paths%2 or a.workers < 1 or a.exercise_steps < 2 or a.call_notice_days < 0:
        p.error("invalid paths, workers, exercise grid or notice period")
    if a.backend == 'cuda' and a.sampler == 'mt19937':
        p.error('CUDA requires the xoroshiro128p sampler')
    return a


def parameters(model, args):
    gpu_sampler = model != 'BS' and (args.sampler == 'xoroshiro128p' or args.backend == 'cuda')
    return {"model":model,"implementation":implementation_for(model),"credit":"TF-style-cash-only-spread",
        "paths":args.paths if model != "BS" else 0,"exercise_steps":args.exercise_steps if model == "LSM" else 0,
        "call_notice_days":args.call_notice_days if model != "BS" else 0,
        "trading_days_per_year":252,"seed_scheme":("crc32-date-bond; xoroshiro128p-jump-2^64-float64-antithetic-v1" if gpu_sampler else "crc32-date-bond; thread-local-MT19937-antithetic-nonoverlapping-v1"),
        "regression":("training-only-policy-v2; contractual-dominance-screen; " if model=='LSM' else "independent-training-and-valuation; ")+"1,S,S2,call-count,put-count,notice-left",
        "dividend_yield":0.,"reset_policy":"no-unverified-future-reset",
        "call_policy":"deterministic-after-observed-price-trigger",
        "quote_basis":"full-price","coupon_timing":"contractual-dates; next-simulation-observation"}


def paths_for(output, model):
    return {"model":output/f"{model}_Model_Prices.csv",
        "market":output/("Market_Prices.csv" if model == "BS" else f"{model}_Market_Prices.csv"),
        "absolute":output/f"{model}_Model_Deviation_Abs.csv",
        "relative":output/f"{model}_Model_Deviation_Pct.csv",
        "error":output/f"{model}_Monte_Carlo_SE.csv",
        "spread":output/f"{model}_Implied_Credit_Spread.csv",
        "coverage":output/f"{model}_Pricing_Coverage.csv",
        "issues":output/f"{model}_Pricing_Issues.csv",
        "summary":output/f"{model}_Model_Summary.xlsx",
        "manifest":output/f"{model}_Model_Manifest.json"}


def _write_csv(frame, path):
    tmp = path.with_suffix(path.suffix+".tmp")
    frame.to_csv(tmp)
    os.replace(tmp,path)


def validate_outputs(model, output, data=None):
    files = paths_for(Path(output), model)
    m = json.loads(files["manifest"].read_text(encoding="utf-8"))
    if m["contract_version"] != CONTRACT_VERSION or m.get('model_parameters',{}).get('implementation')!=implementation_for(model):
        raise DataContractError(f"{model}: legacy pricing contract; explicit versioned rebuild required")
    if hashlib.sha256(files["summary"].read_bytes()).hexdigest() != m["output_sha256"]:
        raise DataContractError(f"{model}: summary integrity check failed")
    expected_names={p.name for key,p in files.items() if key not in ('manifest','summary')}
    if set(m.get('artifact_sha256',{}))!=expected_names:
        raise DataContractError(f"{model}: incomplete artifact integrity manifest")
    for name, expected in m["artifact_sha256"].items():
        if hashlib.sha256((Path(output)/name).read_bytes()).hexdigest() != expected:
            raise DataContractError(f"{model}: artifact integrity check failed: {name}")
    prices = pd.read_csv(files["model"],index_col=0,parse_dates=True)
    market = pd.read_csv(files["market"],index_col=0,parse_dates=True)
    if not prices.index.equals(market.index) or not prices.columns.equals(market.columns):
        raise DataContractError(f"{model}: market and model axes differ")
    if str(prices.index.max().date()) != m["input_cutoff"]:
        raise DataContractError(f"{model}: cutoff mismatch")
    if data is not None and data.fingerprint(pd.Timestamp(m['input_cutoff']),m['model_parameters'])!=m['input_fingerprint']:
        raise DataContractError(f"{model}: verified history changed")
    return m


def run_model(model, args, data=None):
    data = data or PricingInputs(args.input_dir)
    output = args.output_dir
    output.mkdir(parents=True,exist_ok=True)
    files = paths_for(output, model)
    previous = validate_outputs(model,output) if files['manifest'].exists() else None
    checkpoint = output/f"{model}_rebuild_checkpoint.json"
    saved = json.loads(checkpoint.read_text(encoding='utf-8')) if args.resume and not previous and checkpoint.exists() else None
    existing_params = previous['model_parameters'] if previous else (saved['parameters'] if saved else {})
    if args.sampler is None and 'xoroshiro128p' in existing_params.get('seed_scheme',''):
        args.sampler = 'xoroshiro128p'
    params = parameters(model,args)
    backend = 'cuda' if model != 'BS' and args.backend == 'cuda' else ('cuda-reference' if 'xoroshiro128p' in params['seed_scheme'] else 'cpu')
    dates = data.dates(args.start,args.end)
    if not len(dates):
        raise DataContractError("no completed weekly observations in requested range")
    if previous:
        cutoff = pd.Timestamp(previous["input_cutoff"])
        if previous["model_parameters"] != params or previous["input_fingerprint"] != data.fingerprint(cutoff,params):
            raise DataContractError(f"{model}: verified history changed; refusing full rebuild")
        if args.initialize_history and not args.resume:
            raise DataContractError("initialization requires an unused output directory")
        dates = dates[dates > cutoff]
        if not len(dates):
            print(f"{model}: 0 new dates; history preserved through {cutoff.date()}",flush=True)
            return previous
    elif not args.initialize_history:
        raise DataContractError(f"{model}: corrected manifest required; use reprice_history.py for explicit initialization")
    elif files["model"].exists() and not args.resume:
        raise DataContractError("initialization will not overwrite existing model files")
    frames = {key: pd.DataFrame(index=dates, columns=data.market.columns,dtype=float)
              for key in ["model","error","spread"]}
    coverage, issues = [], []
    fingerprint = data.fingerprint(dates[-1],params)
    if saved:
        if saved["parameters"] != params or saved["input_fingerprint"] != fingerprint:
            raise DataContractError("rebuild checkpoint inputs changed")
        for key in frames:
            frames[key] = pd.read_csv(output/f"{model}_checkpoint_{key}.csv",index_col=0,parse_dates=True,float_precision="round_trip").reindex(index=dates,columns=data.market.columns)
        coverage = saved["coverage"]
        issues = saved["issues"]
        dates_to_run = dates[dates > pd.Timestamp(saved["last_date"])]
    else:
        dates_to_run = dates

    if backend == 'cuda':
        from numba import cuda
        if not cuda.is_available():
            raise RuntimeError('CUDA requested but the device/toolkit is unavailable')
        cuda.select_device(0)

    def calculate(item):
        code, date = item
        try:
            c = data.contract(code,date)
            if model != "BS":
                c = data.clause_contract(code,date,c,call_notice_days=args.call_notice_days)
        except (DataContractError,ValueError,KeyError,OverflowError) as exc:
            return code,np.nan,np.nan,np.nan,str(exc)
        # A numerical/model failure is never relabelled as missing source data.
        seed = zlib.crc32(f"{date.date()}:{code}".encode()) & 0x7fffffff
        if backend == 'cuda':
            from numba import cuda
            cuda.select_device(0)
        value,error = (price_bs(c),0.) if model == "BS" else price_mc(c,method=model,paths=args.paths,
            exercise_steps=args.exercise_steps,seed=seed,backend=backend)
        if not np.isfinite(value) or value <= 0 or not np.isfinite(error):
            raise DataContractError(f"{model} {date.date()} {code}: invalid numerical result")
        return code,value,error,c.spread,None

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for count,date in enumerate(dates_to_run,1):
            active = data.market.columns[data.market.loc[date].gt(0)]
            for code,value,error,spread,problem in pool.map(calculate,[(code,date) for code in active]):
                if problem:
                    issues.append({"date":str(date.date()),"code":code,"reason":problem})
                else:
                    for key,number in [("model",value),("error",error),("spread",spread)]:
                        frames[key].at[date,code] = number
            priced = int(frames["model"].loc[date].notna().sum())
            coverage.append({"date":str(date.date()),"market_count":len(active),"priced_count":priced,"coverage":priced/len(active) if len(active) else 0.})
            print(f"{model} {date.date()}: {priced}/{len(active)}; {count}/{len(dates_to_run)} dates",flush=True)
            if count%10 == 0 or count == len(dates_to_run):
                for key,frame in frames.items():
                    _write_csv(frame,output/f"{model}_checkpoint_{key}.csv")
                checkpoint.write_text(json.dumps({"parameters":params,"input_fingerprint":fingerprint,
                    "last_date":str(date.date()),"coverage":coverage,"issues":issues},ensure_ascii=False),encoding="utf-8")
    model_frame = frames["model"]
    validate_pricing_coverage(market_price=data.market,model_price=model_frame,dates=dates,
        min_coverage=ZL_MIN_PRICING_COVERAGE if model != "BS" else .95,min_count=1,
        label=f"{model} corrected pricing", historical_min_coverage=ZL_HISTORICAL_MIN_COVERAGE if model != "BS" else .95,
        min_coverage_enforced_from=ZL_MIN_COVERAGE_ENFORCED_FROM)
    if previous:
        for key in frames:
            old = pd.read_csv(files[key],index_col=0,parse_dates=True,float_precision="round_trip")
            frames[key] = pd.concat([old.reindex(columns=data.market.columns),frames[key]]).sort_index()
        coverage = pd.read_csv(files["coverage"]).to_dict("records")+coverage
        old_issues = pd.read_csv(files["issues"]).to_dict("records")
        issues = old_issues+issues
    return write_outputs(model,data,output,params,frames,coverage,issues,execution_backend='cuda' if backend=='cuda' else 'cpu')


def write_outputs(model,data,output,params,frames,coverage,issues,execution_backend='cpu'):
    files=paths_for(output,model)
    prices = frames["model"]
    market = data.market.reindex_like(prices)
    absolute = prices-market
    relative = absolute/market.where(market>0)
    for key,frame in {**frames,"market":market,"absolute":absolute,"relative":relative}.items():
        _write_csv(frame,files[key])
    pd.DataFrame(coverage).to_csv(files["coverage"],index=False)
    pd.DataFrame(issues,columns=["date","code","reason"]).to_csv(files["issues"],index=False)
    temp = files["summary"].with_suffix(".tmp.xlsx")
    with pd.ExcelWriter(temp) as writer:
        for sheet,frame in [("理论价格",prices),("市场价格",market),("绝对偏差",absolute),("相对偏差",relative)]:
            frame.to_excel(writer,sheet_name=sheet)
    os.replace(temp,files["summary"])
    manifest = {"contract_version":CONTRACT_VERSION,"model":model,"execution_backend":execution_backend,
        "verified_dates":[str(d.date()) for d in prices.index],"input_cutoff":str(prices.index.max().date()),
        "input_fingerprint":data.fingerprint(prices.index.max(),params),"model_parameters":params,
        "output_sha256":hashlib.sha256(files["summary"].read_bytes()).hexdigest(),
        "artifact_sha256":{path.name:hashlib.sha256(path.read_bytes()).hexdigest()
                           for key,path in files.items() if key not in ("manifest","summary")},
        "limitations":["Zero dividend yield; no unverified future reset or discretionary waiver assumed.",
            "Deterministic issuer policy and fixed notice period are model assumptions, not observed issuer decisions.",
            "TF-style discounting is not an explicit default/recovery model; contractual coupon dates are not verified historical cashflow events."]}
    files["manifest"].write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")
    validate_outputs(model,output)
    return manifest


def main(model, argv=None):
    args = options(model,argv)
    run_model(model,args)
