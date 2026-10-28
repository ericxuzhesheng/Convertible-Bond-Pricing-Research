"""Explicit cache-only three-model rebuild, validated before publication.

Run without --promote for a reviewable staging result. --promote replaces only
tracked pricing artifacts after checking that the old versions are in Git.
It never downloads data, runs factors, or rewrites frozen strategy ledgers.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from pricing_backtest import BASE, options, run_model, validate_outputs, paths_for
from pricing_inputs import PricingInputs


def promote(stage):
    root = BASE.parent
    git = ["git","-c",f"safe.directory={root.as_posix()}","-C",str(root)]
    parent = subprocess.check_output([*git,"rev-parse","HEAD"],text=True).strip()
    artifacts = []
    manifests = {}
    for model in ("BS","ZL","LSM"):
        manifests[model]=validate_outputs(model,stage)
        artifacts.extend(paths_for(stage,model).values())
    # These names alone are owned by pricing; unrelated files are untouched.
    for source in artifacts:
        target = BASE/source.name
        rel = target.relative_to(root).as_posix()
        if target.exists():
            subprocess.run([*git,"cat-file","-e",f"{parent}:{rel}"],check=True,capture_output=True)
            if subprocess.run([*git,"diff","--quiet","HEAD","--",rel]).returncode:
                raise RuntimeError(f"Uncommitted pricing artifact would be overwritten: {rel}")
    proof = {"superseded_git_commit":parent,"models":{
        m:manifests[m]["input_cutoff"]
        for m in ("BS","ZL","LSM")},"research_outputs":"legacy; not recomputed"}
    proof['model_implementations']={m:manifests[m]['model_parameters']['implementation'] for m in manifests}
    receipt=stage/'final_clause_repair_receipt.json'
    if receipt.exists():
        proof['staging_clause_repairs']=json.loads(receipt.read_text(encoding='utf-8'))
    (stage/"Pricing_Migration.json").write_text(json.dumps(proof,indent=2),encoding="utf-8")
    for source in artifacts:
        target = BASE/source.name
        temp = target.with_suffix(target.suffix+".promote.tmp")
        shutil.copyfile(source,temp)
        os.replace(temp,target)
    shutil.copyfile(stage/"Pricing_Migration.json",BASE/"Pricing_Migration.json")
    for model in ("BS","ZL","LSM"):
        validate_outputs(model,BASE)
    print(f"Published corrected pricing. Old artifacts remain in Git commit {parent}.",flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start",default="2019-01-01")
    p.add_argument("--end")
    p.add_argument("--stage",type=Path,default=BASE/"logs"/"pricing_v2_rebuild")
    p.add_argument("--workers",type=int,default=4)
    p.add_argument("--lsm-backend",choices=['cpu','cuda'],default='cpu')
    p.add_argument("--zl-backend",choices=['cpu','cuda'],default='cpu')
    p.add_argument("--lsm-paths",type=int,default=8192)
    p.add_argument("--zl-paths",type=int,default=10000)
    p.add_argument("--exercise-steps",type=int,default=192)
    p.add_argument("--resume",action="store_true")
    p.add_argument("--promote",action="store_true")
    a = p.parse_args()
    if a.stage.resolve() == BASE.resolve():
        p.error("rebuild staging directory must differ from published outputs")
    data = PricingInputs(BASE)
    for model in ("BS","ZL","LSM"):
        argv = ["--weekly","--initialize-history","--output-dir",str(a.stage),"--start",a.start,
                "--workers",str(a.workers),"--paths",str(a.lsm_paths if model=="LSM" else a.zl_paths),
                "--exercise-steps",str(a.exercise_steps),"--backend",
                a.lsm_backend if model=='LSM' else (a.zl_backend if model=='ZL' else 'cpu')]
        if a.end:
            argv.extend(["--end",a.end])
        if a.resume:
            argv.append("--resume")
        run_model(model,options(model,argv),data)
    if a.promote:
        promote(a.stage)


if __name__ == "__main__":
    main()
