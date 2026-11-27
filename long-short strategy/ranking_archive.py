"""Store strategy rankings in independent, reproducible annual CSV archives."""

import gzip
from io import BytesIO
from itertools import groupby

import pandas as pd


def write_rankings(output, rankings):
    for year, frames in groupby(rankings, key=lambda frame: pd.Timestamp(frame.signal_date.iloc[0]).year):
        part = pd.concat(list(frames), ignore_index=True)
        target = output / 'rankings' / f'{year}.csv.gz'
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb', mtime=0, compresslevel=6) as compressed:
            compressed.write(part.to_csv(index=False, lineterminator='\n').encode('utf-8'))
        payload = buffer.getvalue()
        if target.is_file() and target.read_bytes() == payload:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix('.tmp')
        temporary.write_bytes(payload)
        temporary.replace(target)


def read_rankings(output, dates):
    years = sorted(set(pd.DatetimeIndex(dates).year))
    if not years:
        return pd.DataFrame()
    archive = output / 'rankings'
    if not archive.is_dir():
        return pd.read_csv(output / 'rankings.csv', low_memory=False)
    return pd.concat(
        [pd.read_csv(archive / f'{year}.csv.gz', low_memory=False) for year in years],
        ignore_index=True,
    )
