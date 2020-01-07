import logbook
import pandas as pd

from cnswd.utils import data_root

from ..websource.tencent import fetch_minutely_prices
from .trading_calendar import is_trading_day

logger = logbook.Logger('分钟交易数据')


def refresh_minutely_prices():
    """刷新分钟交易数据"""
    today = pd.Timestamp('today')
    fp = data_root(f"live_quotes/{today.strftime(r'%Y%m%d')}.h5")
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        return
    df = fetch_minutely_prices()
    if len(df) > 0:
        dt = pd.Timestamp.now().floor('min').timestamp()
        fp = data_root(f"TCT/{today.strftime('%Y%m%d')}/{dt}.pkl")
        df.to_pickle(fp)
        logger.info('添加{}行'.format(df.shape[0]))
