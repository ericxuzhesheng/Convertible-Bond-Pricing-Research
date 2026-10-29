"""Read production inputs; save bounded, source-backed CCB repairs separately."""
import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1]/'backtest'
sys.path.insert(0, str(BASE))
from pricing_inputs import PricingInputs
from strategy_data import TushareReader, now_local, SourceError
from market_data_contracts import build_observed_volatility


def eligible_cells(data):
    for day in data.dates():
        for code in data.market.columns[data.market.loc[day].gt(0)]:
            row = data.basic.loc[code]
            if pd.isna(row.list_date) or day < row.list_date or (pd.notna(row.delist_date) and day >= row.delist_date):
                continue
            yield day, code


def main(fetch=False):
    out = HERE/'inputs'; out.mkdir(parents=True, exist_ok=True)
    raw = out/'raw'; raw.mkdir(exist_ok=True)
    d = PricingInputs(BASE)
    targets = []
    for day, code in eligible_cells(d):
        for field, frame in d.frames.items():
            value = frame.at[day, code] if day in frame.index and code in frame else np.nan
            if not np.isfinite(value) or (value < 0 if field == 'volatility' else value <= 0):
                targets.append(dict(date=str(day.date()), code=code, field=field))
    missing = pd.DataFrame(targets)
    missing.to_csv(out/'missing_before.csv', index=False)
    reader = TushareReader() if fetch else None
    old = BASE/'logs/pricing_input_repairs'
    records, attempts = [], []

    def query(name, api, fields, params, refresh=False):
        saved = raw/name
        if saved.exists():
            item = json.loads(saved.read_text(encoding='utf-8'))
            return pd.DataFrame(item['records']), str(saved.relative_to(HERE)), item['fetched_at']
        cached = old/name
        if cached.exists() and not refresh:
            return pd.read_json(cached, dtype=False), str(cached.relative_to(HERE.parents[1])), 'existing_source_snapshot'
        if reader is None:
            return pd.DataFrame(), '', ''
        try:
            frame = reader.query(api, fields, **params)
            if len(frame) >= 2000:
                raise SourceError('query may be truncated')
            stamp = now_local()
            saved.write_text(json.dumps(dict(api=api, params=params, fields=fields, fetched_at=stamp,
                records=json.loads(frame.to_json(orient='records'))), ensure_ascii=False, indent=2), encoding='utf-8')
            attempts.append(dict(api=api, key=name, status='returned', rows=len(frame)))
            return frame, str(saved.relative_to(HERE)), stamp
        except SourceError as exc:
            attempts.append(dict(api=api, key=name, status=str(exc), rows=0))
            return pd.DataFrame(), '', ''

    for (code, year), group in missing.loc[missing.field.eq('floor')].groupby(['code', missing.date.str[:4]]):
        params = dict(ts_code=code, start_date=group.date.min().replace('-', ''), end_date=group.date.max().replace('-', ''))
        name = f'floor_{code}_{year}.json'
        for refresh in [False, True] if fetch else [False]:
            frame, source, stamp = query(name, 'cb_daily', 'ts_code,trade_date,close,bond_value', params, refresh)
            if not frame.empty:
                frame = frame.set_index(frame.trade_date.astype(str))
                for target in group.itertuples():
                    key = target.date.replace('-', ''); day = pd.Timestamp(target.date)
                    if pd.notna(d.frames['floor'].at[day, code]) or key not in frame.index:
                        continue
                    row = frame.loc[key]
                    if isinstance(row, pd.DataFrame):
                        raise ValueError('duplicate source observation')
                    if (pd.notna(row.bond_value) and row.bond_value > 0
                            and np.isclose(float(row.close), d.market.at[day, code], rtol=0, atol=.001)):
                        value = float(row.bond_value)
                        d.frames['floor'].at[day, code] = value
                        records.append(dict(date=target.date, code=code, field='floor', value=value,
                            source=source, fetched_at=stamp, source_asof=target.date, age_days=0))
            if group.apply(lambda x: pd.notna(d.frames['floor'].at[pd.Timestamp(x.date), code]), axis=1).all():
                break
        print(f'Floor source checked: {code} {year}', flush=True)

    vols = missing.loc[missing.field.eq('volatility')].copy()
    vols['stock'] = vols.code.map(d.basic.stk_cd)
    for (stock, year), group in vols.groupby(['stock', vols.date.str[:4]]):
        params = dict(ts_code=stock, start_date=(pd.Timestamp(group.date.min())-pd.Timedelta(days=600)).strftime('%Y%m%d'),
                      end_date=group.date.max().replace('-', ''))
        # Reuse complete historical source snapshots. Fetch only when absent;
        # a known >7-day stale quote is not repaired with a made-up volatility.
        daily, ds, stamp = query(f'daily_{stock}_{year}.json', 'daily', 'ts_code,trade_date,close', params)
        adj, ads, _ = query(f'adj_{stock}_{year}.json', 'adj_factor', 'ts_code,trade_date,adj_factor', params)
        if daily.empty or adj.empty:
            continue
        daily.trade_date = daily.trade_date.astype(str); adj.trade_date = adj.trade_date.astype(str)
        merged = daily.merge(adj, on=['ts_code', 'trade_date'], validate='one_to_one')
        close = pd.Series(merged.close.to_numpy()*merged.adj_factor.to_numpy(),
            index=pd.to_datetime(merged.trade_date, format='%Y%m%d')).sort_index()
        dates = pd.DatetimeIndex(pd.to_datetime(group.date.unique()))
        vol = build_observed_volatility(adjusted_close=close, target_dates=dates, window=250,
            min_observations=60, max_staleness_days=7)
        for target in group.itertuples():
            day = pd.Timestamp(target.date)
            past = close.loc[:day]
            age = (day-past.index[-1]).days if len(past) else None
            if pd.notna(vol.at[day]):
                d.frames['volatility'].at[day, target.code] = vol.at[day]
                records.append(dict(date=target.date, code=target.code, field='volatility', value=vol.at[day],
                    source=ds+' + '+ads, fetched_at=stamp, source_asof=str(past.index[-1].date()), age_days=age))
            else:
                attempts.append(dict(api='volatility', key=f'{target.code}/{target.date}',
                    status=f'last_observed_quote_age_days={age}; no valid volatility', rows=len(past)))
        print(f'Volatility source checked: {stock} {year}', flush=True)
    repaired = pd.DataFrame(records, columns=['date','code','field','value','source','fetched_at','source_asof','age_days'])
    repaired.to_csv(out/'repairs.csv', index=False)
    pd.DataFrame(attempts).to_csv(out/'source_attempts.csv', index=False)
    keys = set(zip(repaired.date, repaired.code, repaired.field))
    remaining = missing.loc[[tuple(x) not in keys for x in missing[['date','code','field']].itertuples(index=False, name=None)]]
    remaining.to_csv(out/'missing_after.csv', index=False)
    print(json.dumps(dict(before=len(missing), repaired=len(repaired), remaining=len(remaining)), indent=2), flush=True)


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--fetch',action='store_true');main(p.parse_args().fetch)
