from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'backtest'))
import publish_weekly_outputs as publisher


def test_local_task_schedule_and_runtime():
    setup = (ROOT/'backtest/setup_weekly_task.ps1').read_text(encoding='utf-8-sig')
    batch = (ROOT/'backtest/weekly_update.bat').read_text(encoding='utf-8')
    launcher = (ROOT/'backtest/run_scheduled_weekly.ps1').read_text(encoding='utf-8')
    assert "$env:PYTHONNOUSERSITE = '1'" in launcher
    assert 'logs\\plot_runtime' in launcher
    assert '-DaysOfWeek Friday -At "16:30"' in setup
    assert '-WindowStyle Hidden' in setup
    assert '-StartWhenAvailable' in setup
    assert '-MultipleInstances IgnoreNew' in setup
    assert 'set "PYTHON=%CB_PYTHON%"' in batch
    assert 'git add -u' not in batch
    assert batch.index('rebuild_research_outputs.py') < batch.index('publish_weekly_outputs.py')


def test_publisher_keeps_unrelated_work_out_of_commit(monkeypatch):
    reports = 'long-short strategy/weekly_value_results_pricing_v2_2026/weeks/2026-10-30.md'
    def output(command):
        if 'branch' in command: return b'main\n'
        if 'diff' in command: return b'README.md\0AGENTS.md\0report/README.md\0'
        return (reports+'\0report/private.pdf\0backtest/logs/result.csv\0').encode()
    calls = []
    monkeypatch.setattr(publisher.subprocess, 'check_output', output)
    monkeypatch.setattr(publisher.subprocess, 'run', lambda command, **kwargs: calls.append(command))
    publisher.main()
    commit = next(c for c in calls if 'commit' in c)
    assert '--only' in commit
    assert set(commit[commit.index('--')+1:]) == {'README.md', reports}
    check = next(c for c in calls if '--check' in c)
    assert 'core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol' in check
    assert calls[-1][-3:] == ['push', 'origin', 'main']


def test_failed_commit_never_pushes(monkeypatch):
    monkeypatch.setattr(publisher.subprocess, 'check_output', lambda command:
        b'main\n' if 'branch' in command else b'README.md\0')
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if 'commit' in command: raise subprocess.CalledProcessError(1, command)
    monkeypatch.setattr(publisher.subprocess, 'run', run)
    with pytest.raises(subprocess.CalledProcessError): publisher.main()
    assert all('push' not in c for c in calls)


@pytest.mark.parametrize('path', ['AGENTS.md', 'report/README.md',
    'experiments/new.csv', 'backtest/logs/BS_results.csv',
    'backtest/BS_checkpoint_model.csv', 'backtest/ZL_checkpoint_error.csv',
    'backtest/LSM_checkpoint_spread.csv',
    'backtest/strategy_inputs/raw/private.csv',
    'long-short strategy/weekly_value_results_event_v1_invalid/results.csv'])
def test_non_output_paths_are_excluded(path):
    assert not publisher.is_output(path)
