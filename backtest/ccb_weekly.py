"""Publish the observed-curve CCB with explicit non-call notices, one week at a time."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import sys
import tempfile

import numpy as np
import pandas as pd

from market_data_contracts import DataContractError
from pricing_backtest import BASE, LEGACY_MODELS, implementation_for, paths_for, validate_outputs, write_outputs
from pricing_inputs import PricingInputs

EXPERIMENT = BASE.parent / 'experiments' / 'ccb_2023'
sys.path.insert(0, str(EXPERIMENT))
from curve_pde import price_curve_ccb
from run_history import barrier_for
from waiver_data import ROOT as NOTICES, download, fetch_index
from waiver_terms import build_terms, protection_at

PARAMETERS = {'model': 'CCB', 'implementation': implementation_for('CCB'),
              'credit': 'observed-floor-flat-nonnegative-spread', 'dx': .02,
              'max_time_step': 1/96, 'minimum_protection_years': 1/12,
              'call_policy': 'continuous-price-barrier-after-explicit-protection',
              'quote_basis': 'full-price', 'dividend_yield': 0.}
LIMITATIONS = [
    'Continuous call barrier after verified non-call protection, with a one-month minimum.',
    'Unresolved latest notices are excluded. Keyword search does not certify absence.',
    'Zero dividends, no holder puts or discretionary future conversion-price resets.',
    'Observed bond floor calibrates a flat spread; stock volatility and curve are observed inputs.',
]
TERM_FIELDS = ['announcement_id', 'code', 'available_date', 'start', 'end', 'status', 'superseded_by']


def read_terms():
    terms = pd.read_csv(NOTICES/'terms.csv', dtype={'announcement_id': str, 'superseded_by': str})
    for field in ('available_date', 'start', 'end'):
        terms[field] = pd.to_datetime(terms[field])
    return terms


class CCBInputs(PricingInputs):
    def __init__(self, directory):
        super().__init__(directory)
        self.terms = read_terms()
        self.by_code = {code: events for code, events in self.terms.dropna(subset=['code']).groupby('code')}

    def fingerprint(self, cutoff, parameters):
        digest = hashlib.sha256(super().fingerprint(cutoff, parameters).encode())
        observed = self.terms.loc[self.terms.available_date.le(cutoff), TERM_FIELDS].copy()
        observed['superseded_by'] = observed.superseded_by.where(
            observed.superseded_by.isin(observed.announcement_id))
        digest.update(observed.sort_values('announcement_id').to_csv(index=False, lineterminator='\n').encode())
        return digest.hexdigest()

    def value(self, date, code):
        try:
            contract = self.contract(code, date)
            if self.basic.at[code, 'cb_type'] != 'CB':
                raise DataContractError('nonstandard convertible type outside model')
            barrier = barrier_for(self, code)
            protection = max(contract.conversion_start, PARAMETERS['minimum_protection_years'])
            if code in self.by_code:
                protection, status, notice = protection_at(self.by_code[code], date, protection)
                if status.startswith('unresolved'):
                    raise DataContractError(f'{status}: {notice}')
        except DataContractError as exc:
            return code, None, str(exc)
        price = price_curve_ccb(contract, protection=protection, barrier=barrier)
        if not np.isfinite(price) or price <= 0:
            raise ArithmeticError(f'CCB invalid output: {date.date()} {code}')
        return code, (price, contract, protection, barrier), None


def refresh_notices(end):
    search = pd.read_csv(NOTICES/'search_coverage.csv')
    if set(search['query']) != {'不赎回', '不提前赎回'} or search.groupby('query').end_date.max().nunique() != 1:
        raise DataContractError('CCB notice search has inconsistent cutoffs')
    after = search.end_date.max()
    if after < end:
        index = fetch_index(end=end, after=after)
    else:
        index = pd.read_csv(NOTICES/'announcement_index.csv', dtype={'announcementId': str, 'secCode': str})
    download(index, end)
    status = pd.read_csv(NOTICES/'downloads.csv')
    if not status.status.isin(['ok', 'empty_text']).all():
        raise DataContractError('CCB notice download or extraction failed; see downloads.csv')
    build_terms()


def seed(data, dates, columns):
    source = EXPERIMENT/'results_waivers_2023'
    original = pd.read_csv(source/'CCB_observations.csv', parse_dates=['date'], float_precision='round_trip',
                           usecols=['date', 'code', 'floor_source', 'CCB', 'waiver_state', 'market', 'spread'])
    issues = pd.read_csv(EXPERIMENT/'results/pricing_issues.csv', parse_dates=['date'])
    original = original.loc[original.date.isin(dates)].set_index(['date', 'code'], verify_integrity=True)
    if not pd.Index(original.index.get_level_values('date').unique()).equals(dates):
        raise DataContractError('CCB seed does not span the production weeks')
    approved = original.floor_source.eq('production Tushare') & original.CCB.gt(0)
    records = original.loc[approved]
    for date, quoted in records.reset_index().groupby('date'):
        close = data.market.loc[date, quoted.code].to_numpy()
        if not np.allclose(close, quoted.market, rtol=0, atol=1e-8):
            raise DataContractError(f'CCB seed quote mismatch: {date.date()}')
    blocked = original.loc[~approved, ['waiver_state', 'floor_source']].reset_index()
    blocked['reason'] = blocked.waiver_state.where(blocked.floor_source.eq('production Tushare'),
                                                   'non-production bond floor source')
    issues = pd.concat([issues.loc[issues.date.isin(dates)], blocked[['date', 'code', 'reason']]])
    issues['date'] = issues.date.dt.strftime('%Y-%m-%d')
    frames = {key: records[field].unstack().reindex(index=dates, columns=columns)
              for key, field in [('model', 'CCB'), ('spread', 'spread')]}
    frames['error'] = frames['model'].where(frames['model'].isna(), 0.)
    return frames, issues.to_dict('records')


def update(*, initialize=False, workers=4, output=BASE):
    peers = {model: validate_outputs(model, BASE) for model in LEGACY_MODELS}
    cutoff = min(m['input_cutoff'] for m in peers.values())
    dates = pd.DatetimeIndex([date for date in peers['BS']['verified_dates'] if date <= cutoff])
    if any([date for date in peer['verified_dates'] if date <= cutoff] != list(dates.strftime('%Y-%m-%d'))
           for peer in peers.values()):
        raise DataContractError('Production pricing calendars differ')
    files = paths_for(output, 'CCB')
    columns = pd.read_csv(BASE/'Market_Prices.csv', nrows=0).columns[1:]
    output.mkdir(parents=True, exist_ok=True)
    if initialize:
        if files['manifest'].exists():
            raise DataContractError('CCB already has a production baseline')
        data = CCBInputs(BASE)
        frames, issues = seed(data, dates, columns)
    else:
        data = CCBInputs(BASE)
        previous = validate_outputs('CCB', output, data)
        after = pd.Timestamp(previous['input_cutoff'])
        if list(dates[dates <= after].strftime('%Y-%m-%d')) != previous['verified_dates']:
            raise DataContractError('CCB verified calendar differs from production peers')
        pending = dates[dates > after]
        if not len(pending):
            print(f'CCB: no new weekly dates through {cutoff}', flush=True)
            return previous
        refresh_notices(cutoff)
        data = CCBInputs(BASE)
        validate_outputs('CCB', output, data)
        frames = {key: pd.read_csv(files[key], index_col=0, parse_dates=True, float_precision='round_trip')
                  .reindex(index=dates, columns=columns) for key in ('model','spread','error')}
        issues = pd.read_csv(files['issues']).to_dict('records')
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for date in pending:
                quoted = data.market.loc[date]
                jobs = quoted.index[quoted.gt(0)]
                checks = []
                for code, result, reason in pool.map(lambda code: data.value(date, code), jobs):
                    if reason:
                        issues.append({'date': str(date.date()), 'code': code, 'reason': reason})
                        continue
                    price, contract, protection, barrier = result
                    frames['model'].at[date, code] = price
                    frames['spread'].at[date, code] = contract.spread
                    frames['error'].at[date, code] = 0.
                    checks.append(result)
                if not checks:
                    raise DataContractError(f'CCB has no valid prices for {date.date()}')
                checks.sort(key=lambda result: result[1].parity)
                for i in np.linspace(0, len(checks)-1, min(len(checks), 9), dtype=int):
                    price, contract, protection, barrier = checks[i]
                    fine = price_curve_ccb(contract, protection=protection, barrier=barrier, dx=.01, max_dt=1/192)
                    if not np.isfinite(fine) or abs(fine-price) > .05:
                        raise ArithmeticError(f'CCB grid tolerance exceeded for {date.date()}')
    priced = frames['model'].notna().sum(axis=1)
    quoted = data.market.reindex_like(frames['model']).gt(0).sum(axis=1)
    dropped = pd.DataFrame(issues, columns=['date','code','reason']).groupby('date').size().reindex(
        dates.strftime('%Y-%m-%d'), fill_value=0)
    if (priced.le(0).any() or list(quoted-priced) != dropped.tolist()):
        raise DataContractError('CCB prices, quotes and exclusions do not reconcile')
    coverage = [{'date': str(day.date()), 'market_count': int(quoted[day]), 'priced_count': int(priced[day]),
                 'coverage': float(priced[day]/quoted[day])} for day in dates]
    logs = BASE/'logs'
    logs.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='ccb_publish_', dir=logs) as folder:
        stage = Path(folder)
        manifest = write_outputs('CCB', data, stage, PARAMETERS, frames, coverage, issues, limitations=LIMITATIONS)
        for key, source in paths_for(stage, 'CCB').items():
            os.replace(source, files[key])
    print(f'CCB verified through {cutoff}: {int(priced.sum()):,} prices across {len(dates)} weeks', flush=True)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--initialize', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output-dir', type=Path, default=BASE)
    args = parser.parse_args()
    if args.check:
        manifest = validate_outputs('CCB', args.output_dir, CCBInputs(BASE))
        print(f"CCB baseline verified through {manifest['input_cutoff']}")
    else:
        update(initialize=args.initialize, workers=args.workers, output=args.output_dir)
