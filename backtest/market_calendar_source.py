"""Independent, date-bounded SSE calendar for pricing and strategy inputs."""

from functools import lru_cache
import re

import pandas as pd
import requests

from market_data_contracts import DataContractError


@lru_cache(maxsize=1)
def fetch_sina_calendar() -> pd.DatetimeIndex:
    """Read Sina's SSE calendar with the decoder used by AkShare."""
    from akshare.stock.cons import hk_js_decode
    from py_mini_racer import MiniRacer

    response = requests.get(
        'https://finance.sina.com.cn/realstock/company/klc_td_sh.txt', timeout=12,
    )
    response.raise_for_status()
    match = re.search(r'=\s*"([A-Za-z0-9+/=]+)"', response.text)
    if not match:
        raise DataContractError('Sina SSE calendar is missing a date payload')
    decoder = MiniRacer()
    decoder.eval(hk_js_decode)
    dates = pd.DatetimeIndex(pd.to_datetime(decoder.call('d', match[1])))
    dates = dates.tz_localize(None).normalize().sort_values()
    if dates.empty or dates.hasnans or not dates.is_unique:
        raise DataContractError('Sina SSE calendar returned invalid dates')
    return dates


def sina_open_dates(start: str, end: str) -> list[str]:
    dates = fetch_sina_calendar()
    first, last = pd.Timestamp(start), pd.Timestamp(end)
    if first < dates.min() or last > dates.max():
        raise DataContractError(f'Sina SSE calendar does not cover {start}..{end}')
    return dates[(dates >= first) & (dates <= last)].strftime('%Y%m%d').tolist()
