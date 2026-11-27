import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backtest'))
sys.path.insert(0, str(ROOT / 'long-short strategy'))
import rebuild_research_outputs as rebuild
from publish_weekly_outputs import is_output
from ranking_archive import read_rankings, write_rankings
from rebuild_weekly_reports_2026 import pricing_coverage_line
from update_research_readme import header


def test_rankings_are_partitioned_by_signal_year_and_publishable(tmp_path):
    rankings = [
        pd.DataFrame({'signal_date': [date], 'code': [code], 'signal': [score]})
        for date, code, score in [('2025-12-31', '113001.SH', 0.1), ('2026-01-09', '123001.SZ', 0.2)]
    ]
    write_rankings(tmp_path, rankings)
    archive = tmp_path / 'rankings'
    assert sorted(path.name for path in archive.iterdir()) == ['2025.csv.gz', '2026.csv.gz']
    assert not (tmp_path / 'rankings.csv').exists()
    assert all(is_output(f'long-short strategy/weekly_value_results/research/rankings/{path.name}')
               for path in archive.iterdir())
    assert read_rankings(tmp_path, pd.to_datetime(['2026-01-09'])).code.tolist() == ['123001.SZ']
    dates = pd.to_datetime(['2025-12-31', '2026-01-09'])
    pd.testing.assert_frame_equal(read_rankings(tmp_path, dates), pd.concat(rankings, ignore_index=True))
    old = (archive / '2025.csv.gz').stat().st_mtime_ns
    write_rankings(tmp_path, rankings)
    assert (archive / '2025.csv.gz').stat().st_mtime_ns == old


def test_reports_can_read_older_unpartitioned_rankings(tmp_path):
    frame = pd.DataFrame({'signal_date': ['2025-12-31'], 'code': ['113001.SH']})
    frame.to_csv(tmp_path / 'rankings.csv', index=False)
    pd.testing.assert_frame_equal(read_rankings(tmp_path, pd.to_datetime(frame.signal_date)), frame)


def fake_reports(command, **kwargs):
    assert kwargs['check'] is True
    assert command[command.index('--year') + 1] == '2027'
    output = Path(command[command.index('--output-dir') + 1])
    assert not output.exists()  # No reuse of the old manifest or frozen signals.
    (output / 'weeks').mkdir(parents=True)
    weeks = ['2027-01-01', '2027-01-08']
    for week in weeks:
        (output / 'weeks' / f'{week}.md').write_text(week)
    pd.DataFrame({'week': weeks}).to_csv(output / 'report_index.csv', index=False)
    (output / 'validation.json').write_text(json.dumps(
        dict(status='passed', end='2027-01-08', report_weeks=2)))


def test_report_stage_uses_cutoff_year_and_complete_publication_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(rebuild.subprocess, 'run', fake_reports)
    sources = rebuild.stage_weekly_reports(tmp_path, '2027-01-08', tmp_path/'research')
    assert len(sources) == 4
    assert all(target.is_relative_to(ROOT / 'long-short strategy' /
        'weekly_value_results_pricing_four_2027') for _, target in sources)


def test_year_end_holiday_keeps_the_completed_week_in_its_friday_year(monkeypatch, tmp_path):
    def report(command, **kwargs):
        assert command[command.index('--year') + 1] == '2021'
        output = Path(command[command.index('--output-dir') + 1])
        (output/'weeks').mkdir(parents=True)
        (output/'weeks/2021-01-01.md').write_text('2020-12-31 signal')
        pd.DataFrame({'week':['2021-01-01']}).to_csv(output/'report_index.csv', index=False)
        (output/'validation.json').write_text(json.dumps(
            dict(status='passed',end='2020-12-31',report_weeks=1)))

    monkeypatch.setattr(rebuild.subprocess, 'run', report)
    sources = rebuild.stage_weekly_reports(tmp_path, '2020-12-31', tmp_path/'research')
    assert any(target.name == '2021-01-01.md' for _, target in sources)


@pytest.mark.parametrize('failure', ['missing_week', 'stale_cutoff', 'process'])
def test_incomplete_reports_stop_publication(monkeypatch, tmp_path, failure):
    def run(command, **kwargs):
        if failure == 'process':
            raise subprocess.CalledProcessError(1, command)
        fake_reports(command, **kwargs)
        output = Path(command[command.index('--output-dir') + 1])
        if failure == 'missing_week':
            (output / 'weeks/2027-01-08.md').unlink()
        else:
            validation = output / 'validation.json'
            value = json.loads(validation.read_text())
            value['end'] = '2027-01-01'
            validation.write_text(json.dumps(value))
    monkeypatch.setattr(rebuild.subprocess, 'run', run)
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        rebuild.stage_weekly_reports(tmp_path, '2027-01-08', tmp_path/'research')


def test_weekly_entrypoints_publish_new_report_files():
    for path in ('.github/workflows/weekly-incremental-cpu.yml', 'backtest/weekly_update.bat'):
        source = (ROOT / path).read_text(encoding='utf-8')
        assert '--full-history --refresh-inputs --promote' in source
        if path.endswith('.yml'):
            assert ':(glob)long-short strategy/weekly_value_results_pricing_four_*/**' in source
            assert source.index('rebuild_research_outputs.py') < source.index('git push')
        else:
            assert source.index('rebuild_research_outputs.py') < source.index('publish_weekly_outputs.py')
            publisher = (ROOT / 'backtest/publish_weekly_outputs.py').read_text(encoding='utf-8')
            assert 'long-short strategy/weekly_value_results_pricing_four_*/*' in publisher


def test_readme_header_retains_buttons_and_advances_year():
    text = header('2027-01-08')
    assert 'href="#简体中文"' in text
    assert 'href="#english-version"' in text
    assert 'weekly_value_results_pricing_four_2027/README.md' in text
    assert 'BS + ZL + LSM + CCB' in text
    assert '2027--01--08' in text
    assert text.count('style=for-the-badge') == 9


def test_weekly_coverage_distinguishes_zero_volume_and_missing_volume():
    part = pd.DataFrame({'code': ['A', 'B', 'C']})
    volume = pd.Series({'B': 0., 'outside_universe': 10., 'A': 25.})
    assert pricing_coverage_line(part, volume, 2) == (
        '有收盘价记录 3 只，正成交量 1 只，零成交 1 只。'
        '成交量缺项 1 只。四模型共同覆盖 2 只，估值缺项 1 只。'
    )
