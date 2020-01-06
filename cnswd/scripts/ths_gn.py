"""
覆盖式更新

同花顺网站禁止多进程提取数据
"""
import math
import os
import random
import time
import warnings

import pandas as pd
# from tenacity import retry, retry_if_exception_type, stop_after_attempt

from cnswd.utils import (data_root, is_trading_time, kill_firefox, loop_codes,
                         make_logger)
from cnswd.websource.ths import THS

logger = make_logger('同花顺')


def _update_gn_list(urls):
    api = THS()
    codes = [x[0][-7:-1] for x in urls]
    d = {x[0][-7:-1]: x[1] for x in urls}
    dfs = []
    status = {}
    # 测试
    # codes = ['301636', '300337', '300023']
    for gn in codes:
        for _ in range(3):
            if status.get(gn, False):
                break
            try:
                df = api.get_gn_detail(gn)
                df['概念'] = d[gn]
                dfs.append(df)
                status[gn] = True
                logger.info('提取 {} {}行'.format(d[gn], df.shape[0]))
            except Exception as e:
                status[gn] = False
                logger.error(f'{e!r}')
        time.sleep(0.1)
    api.browser.quit()
    fp = data_root('THS/thsgns.pkl')
    data = pd.concat(dfs, sort=True)
    data.to_pickle(fp)
    failed = [k for k, v in status.items() if not v]
    if len(failed):
        print('失败：', ' '.join(failed))


def update_gn_list():
    """
    更新股票概念列表

    非交易时段更新有效
    """
    if is_trading_time():
        warnings.warn('建议非交易时段更新股票概念。交易时段内涨跌幅经常变动，容易产生重复值！！！')
        return
    try:
        api = THS()
        urls = api.gn_urls
        api.browser.quit()
        _update_gn_list(urls)
    except Exception as e:
        logger.error(e)


def update_gn_time():
    """
    更新股票概念概述列表
    """
    data_fp = data_root('THS/gn_time.pkl')
    try:
        with THS() as api:
            df = api.gn_times
            df.to_pickle(data_fp)
    except Exception:
        pass
