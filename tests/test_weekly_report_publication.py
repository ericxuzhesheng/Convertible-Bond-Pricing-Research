import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backtest'))
import rebuild_research_outputs as rebuild
from update_research_readme import header


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
    sources = rebuild.stage_weekly_reports(tmp_path, '2027-01-08')
    assert len(sources) == 4
    assert all(target.is_relative_to(ROOT / 'long-short strategy' /
        'weekly_value_results_pricing_v2_2027') for _, target in sources)


@pytest.mark.parametrize('failure', ['missing_week', 'stale_cutoff', 'process'])
def test_incomplete_reports_stop_publication(monkeypatch, tmp_path, failure):
    def run(command, **kwargs):
        if failure == 'process':
            raise subprocess.CalledProcessError(1, command)
        fake_reports(command, **kwargs)
        output = Path(command[-1])
        if failure == 'missing_week':
            (output / 'weeks/2027-01-08.md').unlink()
        else:
            validation = output / 'validation.json'
            value = json.loads(validation.read_text())
            value['end'] = '2027-01-01'
            validation.write_text(json.dumps(value))
    monkeypatch.setattr(rebuild.subprocess, 'run', run)
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        rebuild.stage_weekly_reports(tmp_path, '2027-01-08')


def test_weekly_entrypoints_publish_new_report_files():
    for path in ('.github/workflows/weekly-incremental-cpu.yml', 'backtest/weekly_update.bat'):
        source = (ROOT / path).read_text(encoding='utf-8')
        assert '--full-history --refresh-inputs --promote' in source
        assert ':(glob)long-short strategy/weekly_value_results_pricing_v2_*/**' in source
        assert source.index('rebuild_research_outputs.py') < source.index('git push')


def test_readme_header_retains_buttons_and_advances_year():
    text = header('2027-01-08')
    assert 'href="#简体中文"' in text
    assert 'href="#english-version"' in text
    assert 'weekly_value_results_pricing_v2_2027/README.md' in text
    assert '2027--01--08' in text
    assert text.count('style=for-the-badge') == 9
