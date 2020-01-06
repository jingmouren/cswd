"""
trading_date

存储对象
1. 所有交易日历
2. 最新交易的股票代码列表

工作日每天 9：31分执行
"""
import re

import pandas as pd
import requests
from numpy.random import shuffle

from ..data import HDFData
from ..reader import stock_list
from ..setting.constants import MARKET_START, TZ
from ..utils import data_root, ensure_dt_localize
from ..websource.tencent import get_recent_trading_stocks
from ..websource.wy import fetch_history

DATE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')


def get_all_stock_codes():
    try:
        codes = stock_list()['股票代码'].values.tolist()
    except Exception:
        codes = get_recent_trading_stocks()
    return codes


def need_refresh(fp):
    """是否需要刷新"""
    if not fp.exists():
        raise FileNotFoundError(f'不存在：{fp}')
    h = HDFData(fp, 'a')
    need = False
    now = pd.Timestamp.now()
    refresh_time = now.replace(hour=9, minute=30)
    completed_time = h.record['completed_time']
    if completed_time.date() < now.date():
        need = True
    else:
        # 仅当已过更新时间而没有更新时执行
        if now >= refresh_time and completed_time < refresh_time:
            need = True
    return need


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _is_today_trading(codes):
    today = pd.Timestamp.today()
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    r = requests.get(url)
    dts = re.findall(DATE_PATTERN, r.text)
    return today.strftime(r"%Y-%m-%d") in dts


def add_info(dates):
    fp = data_root('trading_calendar.h5')
    now = pd.Timestamp.now()
    status = {
        'completed': True,
        'codes': get_all_stock_codes(),
        'index_col': 'trading_date',
        'max_index': 'trading_date',
        'completed_time': now,
        'next_time': now + pd.Timedelta(days=1),
        'last_date': now.normalize(),
        'memo': '-'
    }
    h = HDFData(fp, 'w')
    df = pd.DataFrame({'trading_date': dates})
    h.add(df, status)


def handle_today():
    today = pd.Timestamp.today()
    codes = get_all_stock_codes()
    shuffle(codes)
    codes = codes[:10]
    if _is_today_trading(codes):
        return today.normalize()


def refresh_trading_calendar():
    """刷新交易日历"""
    today = pd.Timestamp.today()
    yesterday = today - pd.Timedelta(days=1)
    dts = pd.date_range(MARKET_START.tz_localize(None), yesterday, freq='B')
    history = fetch_history('000001', None, None, True)
    dates = []
    for d in dts:
        if d in history.index:
            dates.append(d)
    if handle_today():
        dates.append(handle_today())
    add_info(dates)


def is_trading_day(dt):
    """是否为交易日历"""
    assert isinstance(dt, pd.Timestamp)
    dt = ensure_dt_localize(dt).tz_localize(None).normalize()
    fp = data_root('trading_calendar.h5')
    h = HDFData(fp, 'w')
    return dt.to_datetime64() in h.data['trading_date'].values
