"""Fail-closed rebuild of factors, strategies, and README figures."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
import argparse
import json
import os
import shutil
import tempfile


BACKTEST_DIR = Path(__file__).resolve().parent
REPO_ROOT = BACKTEST_DIR.parent
STEPS = [
    BACKTEST_DIR / "build_observed_factors.py",
    REPO_ROOT / "mispricing factor" / "B-S_mispricing_factor.py",
    REPO_ROOT / "mispricing factor" / "Z-L_mispricing_factor.py",
    REPO_ROOT / "mispricing factor" / "LSM_mispricing_factor.py",
    BACKTEST_DIR / "strategy_data.py",
    REPO_ROOT / "long-short strategy" / "weekly_value_strategy.py",
    BACKTEST_DIR / "regenerate_plots.py",
    BACKTEST_DIR / "update_research_readme.py",
]


def run_checked_steps(
    *,
    python_executable: str = sys.executable,
) -> None:
    for script in STEPS:
        print(f"[research rebuild] running {script.name}", flush=True)
        result = subprocess.run(
            [python_executable, str(script)],
            cwd=REPO_ROOT,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{script.stem} failed with exit code "
                f"{result.returncode}"
            )


def rebuild_history(*, promote=False, refresh_inputs=False):
    """Explicit new research baseline; never remove an existing IC guard."""
    from pricing_backtest import validate_outputs
    from pricing_inputs import PricingInputs
    import pandas as pd

    git=["git","-c",f"safe.directory={REPO_ROOT.as_posix()}","-C",str(REPO_ROOT)]
    parent=subprocess.check_output([*git,"rev-parse","HEAD"],text=True).strip()
    inputs=PricingInputs(BACKTEST_DIR)
    manifests={model:validate_outputs(model,BACKTEST_DIR,inputs) for model in ("BS","ZL","LSM")}
    if len({m['input_cutoff'] for m in manifests.values()})!=1:
        raise ValueError('Pricing cutoffs differ')
    logs=BACKTEST_DIR/'logs'
    logs.mkdir(exist_ok=True)
    stage=Path(tempfile.mkdtemp(prefix='research_full_',dir=logs))
    factors=stage/'factors'
    strategy=stage/'weekly_value'
    print(f'Full research staging: {stage}',flush=True)

    def run(script,*args):
        subprocess.run([sys.executable,str(script),*map(str,args)],cwd=REPO_ROOT,check=True)

    if refresh_inputs:
        run(BACKTEST_DIR/'update_etf_benchmark.py')
        run(BACKTEST_DIR/'strategy_data.py')
    run(BACKTEST_DIR/'build_observed_factors.py','--output-dir',factors)
    sys.path.insert(0,str(REPO_ROOT/'mispricing factor'))
    from mispricing_factor_core import main as factor_main
    periods={}
    for model in ('BS','ZL','LSM'):
        backtest=factor_main(model=model,data_dir=factors)
        history,_=backtest._load_factor_ic_history()
        if history.duplicated(['model','factor','rebalance_date','return_date']).any():
            raise ValueError('Duplicate IC observations')
        if not history.groupby(['rebalance_date','return_date']).size().eq(6).all():
            raise ValueError('Incomplete six-factor IC period')
        periods[model]=len(backtest.results_df)

    run(REPO_ROOT/'long-short strategy/weekly_value_strategy.py','--line','research',
        '--compare','--output-dir',strategy)
    tracking=REPO_ROOT/'long-short strategy/weekly_value_results/tracking'
    if tracking.exists():
        shutil.copytree(tracking,strategy/'tracking')
    run(REPO_ROOT/'long-short strategy/weekly_value_strategy.py','--line','tracking','--output-dir',strategy)
    metrics=pd.read_csv(strategy/'research/metrics.csv')
    if set(metrics.portfolio)!= {'long_only','long_short'}:
        raise ValueError('Missing strategy portfolios')
    for portfolio in ('long_only','long_short'):
        nav=pd.read_csv(strategy/f'research/{portfolio}_daily_nav.csv')
        if nav.date.duplicated().any() or not nav.marked_prices_verified.all():
            raise ValueError('Unverified or duplicate strategy daily marks')
    if metrics[['annual_return','sharpe','max_drawdown']].isna().any().any():
        raise ValueError('Strategy performance cannot be certified even as a price-return estimate')

    # Annual reports use the same verified pricing baseline, in a fresh stage.
    # Never overwrite the previous report provenance to force an in-place rerun.
    cutoff = manifests['BS']['input_cutoff']
    report_sources = stage_weekly_reports(stage, cutoff)

    proof=dict(research_contract_version='research-pricing-v2',superseded_git_commit=parent,
        pricing={model:{key:manifest[key] for key in ('contract_version','input_cutoff','output_sha256')} for model,manifest in manifests.items()},
        factor_periods=periods,stage=str(stage),total_return_certified=False,
        frozen_tracking='Original prospective evidence is preserved; recalculated research is retrospective')
    (stage/'Research_Migration.json').write_text(json.dumps(proof,indent=2),encoding='utf-8')
    if not promote:
        print(f'Validated research staging ready: {stage}',flush=True)
        return stage
    for model,manifest in manifests.items():
        current=json.loads((BACKTEST_DIR/f'{model}_Model_Manifest.json').read_text(encoding='utf-8'))
        if current!=manifest:
            raise RuntimeError('Pricing baseline changed during research calculation')
    sources=[(p,REPO_ROOT/'mispricing factor'/p.name) for p in factors.iterdir() if p.is_file()]
    sources += [(p,REPO_ROOT/'long-short strategy/weekly_value_results'/p.relative_to(strategy)) for p in strategy.rglob('*') if p.is_file()]
    sources += report_sources
    # Validate every destination before replacing any published artifact.
    for source,target in sources:
        if target.exists():
            rel=target.relative_to(REPO_ROOT).as_posix()
            subprocess.run([*git,'cat-file','-e',f'{parent}:{rel}'],check=True,capture_output=True)
            if subprocess.run([*git,'diff','--quiet',parent,'--',rel]).returncode:
                raise RuntimeError(f'Uncommitted research output would be overwritten: {rel}')
    for source,target in sources:
        target.parent.mkdir(parents=True,exist_ok=True)
        temporary=target.with_suffix(target.suffix+'.promote.tmp')
        shutil.copyfile(source,temporary)
        os.replace(temporary,target)
    shutil.copyfile(stage/'Research_Migration.json',BACKTEST_DIR/'Research_Migration.json')
    run(BACKTEST_DIR/'regenerate_plots.py')
    run(BACKTEST_DIR/'update_research_readme.py')
    print(f'Full research promoted; previous outputs remain in Git {parent}',flush=True)
    return stage


def stage_weekly_reports(stage, cutoff):
    import pandas as pd

    year = pd.Timestamp(cutoff).year
    reports = Path(stage) / f'weekly_value_results_pricing_v2_{year}'
    subprocess.run([sys.executable,
        str(REPO_ROOT/'long-short strategy/rebuild_weekly_reports_2026.py'),
        '--year', str(year), '--output-dir', str(reports)], cwd=REPO_ROOT, check=True)
    validation = json.loads((reports/'validation.json').read_text(encoding='utf-8'))
    index = pd.read_csv(reports/'report_index.csv')
    expected = pd.date_range(f'{year}-01-01', cutoff, freq='W-FRI').strftime('%Y-%m-%d').tolist()
    if (validation.get('status') != 'passed' or validation.get('end') != cutoff
            or index['week'].tolist() != expected
            or validation.get('report_weeks') != len(expected)
            or sorted(p.stem for p in (reports/'weeks').glob('*.md')) != expected):
        raise ValueError('Weekly reports are incomplete or behind the pricing cutoff')
    destination = REPO_ROOT/'long-short strategy'/reports.name
    return [(p, destination/p.relative_to(reports)) for p in reports.rglob('*') if p.is_file()]


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full-history',action='store_true')
    parser.add_argument('--promote',action='store_true')
    parser.add_argument('--refresh-inputs',action='store_true')
    args=parser.parse_args()
    if args.promote and not args.full_history:
        parser.error('--promote requires --full-history')
    if args.full_history:rebuild_history(promote=args.promote,refresh_inputs=args.refresh_inputs)
    else:run_checked_steps()
