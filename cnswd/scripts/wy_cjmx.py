import math
import time
from functools import lru_cache, partial
from multiprocessing import Pool

import pandas as pd
from numpy.random import shuffle

from ..data import HDFData
from ..setting.constants import MAX_WORKER
from ..utils import data_root, ensure_dtypes, loop_codes, make_logger
from ..websource.wy import fetch_cjmx
from .trading_calendar import is_trading_day
from ..reader import daily_history


logger = make_logger('网易股票成交明细')
DATE_FMT = r'%Y-%m-%d'


def _last_5():
    """最近的5个交易日"""
    fp = data_root('trading_calendar.h5')
    h = HDFData(fp, 'a')
    dates = h.data['trading_date'].values[-5:]
    return dates


def _wy_fix_data(df):
    dts = df.日期.dt.strftime(DATE_FMT) + ' ' + df.时间
    df['成交时间'] = pd.to_datetime(dts)
    del df['时间']
    del df['日期']
    df = df.rename(columns={'价格': '成交价', '涨跌额': '价格变动', '方向': '性质'})
    df = ensure_dtypes(df,
                       d_cols=['成交时间'],
                       s_cols=['股票代码', '性质'],
                       i_cols=['成交量'],
                       f_cols=['成交价', '成交额'])
    # 保留2位小数
    df = df.round({'价格变动': 2, '成交额': 2, '成交价': 2})
    return df


def data_path(code, date):
    return data_root(f"wy_cjmx/{code}/{date.strftime(r'%Y%m%d')}.h5")


def write_cjmx(codes, date):
    status = {}
    date_str = date.strftime(DATE_FMT)
    for _ in range(3):
        for code in codes:
            fp = data_path(code, date)
            if fp.exists():
                status[code] = True
            if status.get(code, False):
                continue
            try:
                df = fetch_cjmx(code, date_str)
                status[code] = True
            except Exception as e:
                logger.info(f'股票：{code} {date_str} {e!r}')
                status[code] = False
                continue
            df = _wy_fix_data(df)
            df.to_hdf(fp, 'data', append=False)
            logger.info(f'股票：{code} {date_str} 共{len(df):>3}行')
        time.sleep(0.5)
    failed = [k for k, v in status.items() if not v]
    if len(failed):
        print(f'{date_str} 以下股票成交明细提取失败')
        print(failed)


def stock_is_trading(code, date):
    """股票当天是否交易"""   
    try:
        df = daily_history(code, date, date)
        cond = df.loc[df['日期'] == date, '成交量'].values[0]
        if cond > 0:
            return code
        else:
            return None
    except Exception:
        return None


@lru_cache(None)
def get_traded_codes(date):
    """当天交易的股票代码列表"""
    fp = data_root('wy_stock')
    fps = fp.glob('*.h5')
    codes = [fp.name.split('.')[0] for fp in fps]
    func = partial(stock_is_trading, date=date)
    with Pool(MAX_WORKER) as pool:
        res = pool.map(func, codes)
    return [x for x in res if x is not None]


def _refresh_wy_cjmx(date):
    """刷新指定日期成交明细数据"""
    date = pd.Timestamp(date)
    codes = get_traded_codes(date)
    shuffle(codes)
    print(f'{date.strftime(DATE_FMT)} 共{len(codes)}只股票交易')
    batch_num = math.ceil(len(codes) / MAX_WORKER)
    batchs = loop_codes(codes, batch_num)
    func = partial(write_cjmx, date=date)
    with Pool(MAX_WORKER) as pool:
        pool.map(func, batchs)


def refresh_wy_cjmx():
    for d in _last_5():
        _refresh_wy_cjmx(d)
