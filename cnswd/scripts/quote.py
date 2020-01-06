import asyncio
import re

import aiohttp
import logbook
import numpy as np
import pandas as pd

from ..data import HDFData
from ..setting.constants import QUOTE_COLS
from ..utils import data_root, loop_codes
from .trading_calendar import is_trading_day

logger = logbook.Logger('实时报价')

QUOTE_PATTERN = re.compile('"(.*)"')
CODE_PATTERN = re.compile(r'hq_str_s[zh](\d{6})')


def _convert_to_numeric(s, exclude=()):
    if pd.api.types.is_string_dtype(s):
        if exclude:
            if s.name not in exclude:
                return pd.to_numeric(s, errors='coerce')
    return s


def _to_dataframe(content):
    """解析网页数据，返回DataFrame对象"""
    res = [x.split(',') for x in re.findall(QUOTE_PATTERN, content)]
    codes = [x for x in re.findall(CODE_PATTERN, content)]
    df = pd.DataFrame(res).iloc[:, :32]
    df.columns = QUOTE_COLS[1:]
    df.insert(0, '股票代码', codes)
    df.dropna(inplace=True)
    return df


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


async def fetch(codes):
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    async with aiohttp.request('GET', url) as r:
        data = await r.text()
    return data


async def to_dataframe(codes):
    """解析网页数据，返回DataFrame对象"""
    content = await fetch(codes)
    df = _to_dataframe(content)
    df = df.apply(_convert_to_numeric, exclude=('股票代码', '股票简称', '日期', '时间'))
    df = df[df.成交额 > 0]
    if len(df) > 0:
        df['时间'] = pd.to_datetime(df.日期 + ' ' + df.时间)
        del df['日期']
        return df
    return pd.DataFrame()


async def fetch_all(fp, batch_num=800):
    """获取所有股票实时报价原始数据"""
    t_fp = data_root('trading_calendar.h5')
    h = HDFData(t_fp, 'a')
    stock_codes = h.record['codes']
    b_codes = loop_codes(stock_codes, batch_num)
    tasks = [to_dataframe(codes) for codes in b_codes]
    dfs = await asyncio.gather(*tasks)
    return pd.concat(dfs)


async def refresh_live_quote():
    """刷新实时报价"""
    today = pd.Timestamp('today')
    fp = data_root(f"live_quotes/{today.strftime(r'%Y%m%d')}.h5")
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        return
    df = await fetch_all(fp)
    if len(df) > 0:
        df = df.loc[df['时间'] >= today.normalize(), :]
        df.to_hdf(fp, 'data', append=True)
        logger.info('添加{}行'.format(df.shape[0]))
