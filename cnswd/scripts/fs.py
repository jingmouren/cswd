"""
快速搜索
项目下
    按股票代码循环
"""

import math
from itertools import product
from multiprocessing import Pool

from cnswd.cninfo import FastSearcher
from cnswd.scripts.cninfo_cols import CNINFO_COLS
from cnswd.scripts.refresh import FSRefresher
from cnswd.setting.constants import MAX_WORKER
from cnswd.utils import loop_codes
from cnswd.websource.tencent import get_recent_trading_stocks
from numpy.random import shuffle


def refresh_batch(batch):
    """分批刷新快速刷新项目"""
    r = FSRefresher()
    kwargs = {}
    with FastSearcher() as api:
        fetch_data_func = api.get_data
        for one in batch:
            try:
                r.refresh_one(fetch_data_func, one, kwargs)
            except Exception:
                api.reset()
                r.refresh_one(fetch_data_func, one, kwargs)


def fs_refresh_all():
    """快速搜索数据刷新"""
    levels = CNINFO_COLS.keys()
    codes = get_recent_trading_stocks()
    # 随机化代码，以便均匀分布任务
    shuffle(codes)
    items = list(product(levels, codes))
    batch_num = math.ceil(len(items) / MAX_WORKER)
    batchs = loop_codes(items, batch_num)
    with Pool(MAX_WORKER) as pool:
        pool.map_async(refresh_batch, batchs).get()


if __name__ == "__main__":
    fs_refresh_all()
