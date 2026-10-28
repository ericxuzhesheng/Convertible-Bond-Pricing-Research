"""Commit only generated weekly outputs, preserving unrelated local/index work."""
from datetime import datetime
from fnmatch import fnmatchcase
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = (
    'README.md', 'backtest/cb_*.csv', 'backtest/rf_yield_cache.csv',
    'backtest/bs_volatility_cache.csv', 'backtest/BS_*.csv',
    'backtest/ZL_*.csv', 'backtest/LSM_*.csv', 'backtest/Market_Prices.csv',
    'backtest/Pricing_Metrics.csv', 'backtest/ETF_511380_Daily.csv',
    'backtest/*_Model_Manifest.json',
    'backtest/*_Model_Summary.xlsx', 'backtest/Research_Migration.json',
    'backtest/ETF_511380_Manifest.json', 'backtest/Fig*.png',
    'backtest/strategy_inputs/*', 'mispricing factor/*.csv',
    'mispricing factor/*.json', 'mispricing factor/*.png',
    'long-short strategy/000832_CSI_close_price.csv',
    'long-short strategy/*_model_performance.png',
    'long-short strategy/weekly_value_results/*',
    'long-short strategy/weekly_results/*',
    'long-short strategy/weekly_value_results_pricing_v2_*/*',
)


def is_output(path):
    # fnmatch * includes slashes; top-level CSV patterns must not admit logs.
    if path.startswith('backtest/') and path.count('/') > 1:
        return path.startswith('backtest/strategy_inputs/') and '/raw/' not in path
    return any(fnmatchcase(path, pattern) for pattern in PATTERNS)


def main():
    git = ['git', '-c', f'safe.directory={ROOT.as_posix()}', '-C', str(ROOT)]
    def read(*args):
        return subprocess.check_output([*git, *args]).decode('utf-8')
    if read('branch', '--show-current').strip() != 'main':
        raise RuntimeError('Weekly publication requires main')
    changed = read('diff', '--name-only', '-z', 'HEAD').split('\0')
    new = read('ls-files', '--others', '--exclude-standard', '-z').split('\0')
    paths = sorted({p for p in changed + new if p and is_output(p)})
    if paths:
        subprocess.run([*git, 'add', '--', *paths], check=True)
        subprocess.run([*git, 'diff', '--cached', '--check', '--', *paths], check=True)
        subprocess.run([*git, 'commit', '--only', '-m',
            f'chore: verified local weekly update {datetime.now():%Y%m%d}', '--', *paths], check=True)
    else:
        print('No weekly output changes to commit')
    # Also retries a previous verified commit whose push was interrupted.
    subprocess.run([*git, 'push', 'origin', 'main'], check=True)


if __name__ == '__main__':
    main()
