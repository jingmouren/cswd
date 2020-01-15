"""可迭代的数据项目刷新模块（多进程）

h5 中文词 = 2 * len

Notes:
    并行数量: 取当前CPU数量的一半
    循环项目：
        1.  代码字符串
        2. （项目,代码）二元组

    分批格式：
        日期循环
        [['20191126', '20191127'],
        ......
        ]
        代码循环
        [['399001', '399004'],
         ['399005', '399006'],
         ['399313', '399550'],
         ['399321', '399322'],
         ['000010', '000016'],
         ['000300', '000903'],
         ['000905']]

        [[('7.4.1', '603967'),
          ('7.4.1', '603968'),
          ('7.4.1', '603969'),
          ('7.4.1', '603970'),
          ('7.4.1', '603976'),
          ('7.4.1', '603977'),
          ('7.4.1', '603978'),
          ('7.4.1', '603979'),
          ('7.4.1', '603980'),
          ('7.4.1', '603982'),
          ('7.4.1', '603983'),
          ('7.4.1', '603985'),
          ('7.4.1', '603986'),
         ...
        ]
    刷新记录：记载刷新完成情况
        1. 以可迭代对象作为键，一般为代码
        2. 记载数据最后时间及下次更新开始时间
        3. 备注记录刷新过程中触发的异常及尝试次数

# 唯一性
# 合并行为 
"""

import asyncio
import json
import math
import os
import random
import sys
import time
import warnings
from functools import partial
from itertools import product
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd
from numpy.random import shuffle

from ..cninfo import (AdvanceSearcher, ClassifyTree, FastSearcher,
                      ThematicStatistics)
from ..cninfo.utils import get_field_type, get_min_itemsize
from ..data import HDFData, default_status
from ..setting.config import DB_CONFIG, TS_CONFIG
from ..setting.constants import MAIN_INDEX, MARKET_START, MAX_WORKER, TZ
from ..utils import (data_root, ensure_dt_localize, ensure_dtypes, loop_codes,
                     loop_period_by, make_logger, time_for_next_update)
from ..websource.disclosures import fetch_one_day
from ..websource.sina_news import TOPIC_MAPS, Sina247News
from ..websource.tencent import get_recent_trading_stocks
from ..websource.treasuries import (EARLIEST_POSSIBLE_DATE, download_last_year,
                                    fetch_treasury_data_from)
from ..websource.wy import fetch_history
from ..query_utils import query, query_stmt, Ops

warnings.filterwarnings("ignore")


def _path_helper(x, ext):
    """路径辅助变换"""
    if isinstance(x, (int, str)):
        return f"{x}.{ext}"
    if isinstance(x, tuple):
        f, c = x[0], x[1]
        return f"{f}/{c}.{ext}"
    raise ValueError(f"期望参数x类型为str、int、tuple，实际为{type(x)}")


def _one_parse_helper(arg):
    """解析one参数"""
    x = arg[0]
    if isinstance(x, (int, str)):
        return (x, *arg[1:])
    if isinstance(x, tuple):
        assert len(x) == 2, "元组元素数量必须为2"
        return (*x, *arg[1:])
    raise ValueError(f"期望参数x类型为str、int、tuple，实际为{type(x)}")


class RefresherBase(object):
    def __init__(self, retry_times=3):
        self.retry_times = retry_times

    @property
    def logger(self):
        name = self.__class__.__name__.lower()
        return make_logger(name)

    @property
    def iterables(self):
        """循环列表"""
        raise NotImplementedError('子类中完成')

    def get_min_date(self, one):
        """项目最早有效日期"""
        raise NotImplementedError('子类中完成')

    def get_data_columns(self, one):
        """查询数据列"""
        raise NotImplementedError('子类中完成')

    def get_col_dtypes(self, one):
        """
        列数据类型

        返回：
        {
            'd_cols':[],
            's_cols':[],
            'i_cols':[],
            'f_cols':[],
        }
        """
        raise NotImplementedError('子类中完成')

    def get_index_col(self, one):
        """索引列名称（保持唯一性）"""
        raise NotImplementedError('子类中完成')

    def _is_index_col_dt_dtype(self, index_col):
        """确保索引列为datetime64[ns]类型"""
        dt_keys = ('日期', '时间')
        for key in dt_keys:
            if key in index_col:
                return True
        return False

    def _ensure_index_col_dt_dtype(self, df, index_col):
        """确保索引列为datetime64[ns]类型"""
        assert not df.empty, '数据非空'
        assert index_col is not None, '索引列非空'
        is_dt_dtype = self._is_index_col_dt_dtype(index_col)
        if is_dt_dtype:
            df[index_col] = pd.to_datetime(df[index_col])
        return df

    def get_mode(self, one):
        """刷新模式"""
        raise NotImplementedError('子类中完成')

    def get_freq(self, one):
        """刷新频率"""
        raise NotImplementedError('子类中完成')

    def get_refresh_num(self, one):
        """可进行项目刷新的小时（整数）"""
        raise NotImplementedError('子类中完成')

    def get_data_path(self, one):
        """数据文件路径(扩展名为h5)"""
        sub_dir = self.__class__.__name__.lower()
        p_str = f"{sub_dir}/{_path_helper(one,'h5')}"
        return data_root(p_str)

    def get_hdfdata(self, one):
        """h5数据对象"""
        fp = self.get_data_path(one)
        mode = self.get_mode(one)
        return HDFData(fp, mode)

    def get_table_data(self, one):
        """获取表数据
        
        Arguments:
            one {str} -- 项目层级
        
        Returns:
            DataFrame -- 整表数据
        """
        hdf = self.get_hdfdata(one)
        return hdf.data

    def get_record(self, one):
        """刷新状态"""
        hdf = self.get_hdfdata(one)
        return hdf.record

    def get_start(self, one, use_last_date):
        """开始刷新时间"""
        record = self.get_record(one)
        if use_last_date:
            start = record['last_date']
        else:
            start = record['next_time']
        # 使用项目实际初始日期，简化期间循环
        # 最小日期非常重要
        # 如设置为None代表start为None
        min_date = self.get_min_date(one)
        if min_date is None:
            return None
        if ensure_dt_localize(min_date) > ensure_dt_localize(start):
            start = min_date
        return start

    def get_fetch_func_arg(self, one, start, end):
        """构造网页提取函数的参数(部分子类需要重写)"""
        return (one, start, end)

    def _normalize_data(self, one, web_data, record, use_last_date):
        """规范数据和刷新状态"""
        index_col = self.get_index_col(one)
        record['index_col'] = index_col

        freq = self.get_freq(one)
        num = self.get_refresh_num(one)
        now = pd.Timestamp.now(tz=TZ)
        if record['completed']:
            next_t = time_for_next_update(now, freq, num)
            record['next_time'] = next_t

        # 规范
        if web_data is None:
            # 保存空数据框
            web_data = pd.DataFrame()
        if not web_data.empty:
            # 规范
            if web_data.index.name == index_col:
                web_data.reset_index(inplace=True)

            col_dict = self.get_col_dtypes(one)
            # 转换数据类型
            web_data = ensure_dtypes(web_data, **col_dict)

            if index_col is not None:
                web_data = self._ensure_index_col_dt_dtype(web_data, index_col)
                web_data.sort_values(index_col, inplace=True)
                # 当使用最后日期作为下一次刷新的开始日期时，更新记录
                if use_last_date:
                    max_date = max(web_data[index_col])
                    record['last_date'] = max_date
        return record, web_data

    def get_min_itemsize(self, one):
        """定义列字符串最小长度"""
        return {}

    def refresh_one(self, fetch_data_func, one, kwargs):
        """单项刷新"""
        data_columns = self.get_data_columns(one)
        kwargs.update({'data_columns': data_columns})
        # 由于初始化时一次性写入数据，忽略了min_itemsize问题
        kwargs.update({'min_itemsize': self.get_min_itemsize(one)})

        logger = self.logger
        hdf = self.get_hdfdata(one)
        record = self.get_record(one)

        try:
            use_last_date = kwargs.pop('use_last_date')
        except KeyError:
            use_last_date = False

        start = self.get_start(one, use_last_date)
        end = pd.Timestamp.now(tz=TZ)
        # 初始化时使用分段 ■
        # end = pd.Timestamp('2010-12-31', tz=TZ)
        if start:  # start 可为None
            if ensure_dt_localize(start) > end:
                logger.info(f'{one} 数据已经刷新')
                return
        # 进入刷新
        # 重置完成状态
        record['completed'] = False
        arg = self.get_fetch_func_arg(one, start, end)

        for i in range(1, self.retry_times + 1):
            if record['completed']:
                break
            try:
                # 只捕获网络数据提取部分的异常
                web_data = fetch_data_func(*_one_parse_helper(arg))
                record['completed'] = True
                record['memo'] = '-'  # 清除此前可能遗留的备注
            except Exception as e:
                web_data = pd.DataFrame()
                logger.exception(f"第{i}次尝试提取网络数据，{one}出现异常\n")
                record['completed'] = False
                record['memo'] = f"第{i}次尝试中出现异常{e}"
            finally:
                record['completed_time'] = pd.Timestamp.now(tz=TZ)
                record['retry_times'] = i
                record, web_data = self._normalize_data(
                    one, web_data, record, use_last_date)
        hdf.add(web_data, record, kwargs)

    def refresh_batch(self, batch):
        """分批刷新"""
        raise NotImplementedError('子类中完成')

    def _get_batchs(self):
        batch_num = math.ceil(len(self.iterables) / MAX_WORKER)
        batchs = loop_codes(self.iterables, batch_num)
        return batchs

    def refresh_all(self, item=None):
        """刷新所有项目"""
        if item is None:
            # 形式 [[1],[2,3]]
            batchs = self._get_batchs()
        else:
            # 单个项目
            batchs = [[item]]
        with Pool(MAX_WORKER) as pool:
            pool.map_async(self.refresh_batch, batchs).get()


# region 深证信


class FSRefresher(RefresherBase):
    """
    快速搜索刷新器

    Notes:
    -----
    不再用于刷新。
    """
    def get_data_path(self, one):
        """数据文件路径(扩展名为h5)"""
        level, code = one[0], one[1]
        p_str = f"cninfo/fs/{level}/{code}.h5"
        return data_root(p_str)

    def get_mode(self, one):
        """刷新模式"""
        if one in ('6.5', ):
            return 'w'
        return 'a'

    def get_hdfdata(self, one):
        """h5数据对象"""
        fp = self.get_data_path(one)
        mode = self.get_mode(one)
        return HDFData(fp, mode)

    def get_min_date(self, one):
        """项目最初日期"""
        level, code = one[0], one[1]
        dt = DB_CONFIG[level]['date_field'][1]
        if dt is None:
            raise ValueError(f'必须指定{code}最初日期')
        return pd.Timestamp(dt, tz=TZ)

    def get_index_col(self, one):
        """索引列名称"""
        level = one[0]
        return DB_CONFIG[level]['date_field'][0]

    def get_freq(self, one):
        """刷新频率"""
        level = one[0]
        freq_str = DB_CONFIG[level]['date_freq'][0]
        if freq_str is None:
            raise ValueError('快速搜索刷新器不接受freq为空')
        else:
            # 第二项为刷新频率
            return freq_str[1]

    def get_refresh_num(self, level):
        """可进行项目刷新的小时（整数）"""
        return 20

    def get_col_dtypes(self, one):
        """列数据类型"""
        level = one[0]
        return get_field_type('db', level)

    def get_data_columns(self, one):
        """查询数据列"""
        level, _ = one[0], one[1]
        return DB_CONFIG[level]['data_columns']

    def refresh_all(self):
        raise NotImplementedError('不支持')


class ASRefresher(RefresherBase):
    """高级搜索刷新器"""
    _fs = None

    @property
    def iterables(self):
        """循环列表"""
        res = list(DB_CONFIG.keys())
        shuffle(res)
        return res

    def get_data_path(self, one):
        """分日期存储数据路径"""
        item = '_'.join(one.split('.'))
        return data_root(f'cninfo/as/{item}.h5')

    def get_mode(self, one):
        """刷新模式"""
        if one in ('6.5', ):
            return 'w'
        return 'a'

    def get_min_date(self, one):
        """项目最初日期"""
        min_start = DB_CONFIG[one]['date_field'][1]
        if min_start is None:
            return None
        min_start = pd.Timestamp(min_start, tz=TZ)
        return min_start

    def get_index_col(self, one):
        """索引列名称"""
        return DB_CONFIG[one]['date_field'][0]

    def get_freq(self, one):
        """刷新频率"""
        # 按天刷新
        return 'D'

    def get_refresh_num(self, one):
        """可进行项目刷新的小时数"""
        return 8

    def get_col_dtypes(self, one):
        """列数据类型"""
        return get_field_type('db', one)

    def get_data_columns(self, one):
        """查询数据列"""
        return DB_CONFIG[one]['data_columns']

    def get_min_itemsize(self, one):
        """定义列字符串最小长度"""
        if one == '1':
            return get_min_itemsize('db', '1')
        try:
            return DB_CONFIG[one]['min_itemsize']
        except KeyError:
            return {}

    def _get_one(self, fetch_data_func, one, kwargs, codes, start, end):
        """单项刷新"""
        kwargs = kwargs.copy()
        data_columns = self.get_data_columns(one)
        kwargs.update({'data_columns': data_columns})
        # 由于初始化时一次性写入数据，忽略了min_itemsize问题
        kwargs.update({'min_itemsize': self.get_min_itemsize(one)})

        logger = self.logger
        record = self.get_record(one)

        record['level'] = one
        record['name'] = DB_CONFIG[one]['name']
        # 使用最后日期的意义在于
        # 日线数据每日递增，股票总体可在同一天同步
        # 而财务报告发布日期并不同步
        # 如年初~3月期间，各上市公司发布去年年报
        # 在此期间刷新数据，刷新期间为 （X-1）-12-31 ~ today
        # 提取的网络数据与本地同期数据合并，不保留重复部分，即为新增
        use_last_date = True

        # 进入刷新
        # 重置完成状态
        record['completed'] = False

        for i in range(1, self.retry_times + 1):
            if record['completed']:
                break
            try:
                if codes is None:
                    # 只捕获网络数据提取部分的异常
                    # 高级搜索
                    web_data = fetch_data_func(one, start, end, codes)
                else:
                    # 快速搜索
                    web_data = fetch_data_func(one, codes, start, end)
                record['completed'] = True
                record['memo'] = '-'  # 清除此前可能遗留的备注
            except Exception as e:
                web_data = pd.DataFrame()
                logger.exception(f"第{i}次尝试提取网络数据，{one}出现异常\n")
                record['completed'] = False
                record['memo'] = f"第{i}次尝试中出现异常{e}"
            finally:
                record['completed_time'] = pd.Timestamp.now(tz=TZ)
                record['retry_times'] = i
                record, web_data = self._normalize_data(
                    one, web_data, record, use_last_date)
        return web_data, record, kwargs

    def _get_ipo_by_code(self, code):
        """查询股票上市日期"""
        fp = self.get_data_path('1')
        args = [
            ('股票代码', Ops.eq, code),
        ]
        stmt = query_stmt(*args)
        df = query(fp, stmt)
        return df['上市日期'].values[0]

    def _one_by_one(self, one, code, kwargs):
        """使用快速搜索
        
        避免开始日期设置为市场开始日期，无效循环，导致引发异常
        """
        start = self._get_ipo_by_code(code)
        end = pd.Timestamp.now()
        # 未上市或在未来日期上市的股票，暂不刷新
        # 但股票概况或IPO则需要
        if one not in ('1', '6.5'):
            if pd.isnull(start) or start >= end:
                return
        hdf = self.get_hdfdata(one)
        fetch_data_func = self._fs.get_data
        web_data, record, kwargs = self._get_one(fetch_data_func, one, kwargs,
                                                 code, start, end)
        if not web_data.empty:
            hdf.insert_by(web_data, record, kwargs, '股票代码', code)

    def refresh_one(self, fetch_data_func, one, kwargs, web_codes):
        """单项刷新"""
        local_codes = self.get_hdfdata(one).get_codes()
        hdf = self.get_hdfdata(one)
        logger = self.logger
        record = self.get_record(one)
        use_last_date = True
        if len(local_codes) == 0:
            new_codes = []
        else:
            # 差集
            new_codes = list(set(web_codes).difference(set(local_codes)))

        end = pd.Timestamp.now(tz=TZ)
        # 初始化时使用分段 ■
        # end = pd.Timestamp('2010-12-31', tz=TZ)
        start = self.get_start(one, use_last_date)

        completed = False
        completed_time = ensure_dt_localize(record['completed_time'])
        delta = end - completed_time

        # 最近12小时之内已经完成，不再刷新
        if record['completed'] and delta < pd.Timedelta(hours=12):
            completed = True
        if completed:
            logger.info(f"在最近12小时内，{DB_CONFIG[one]['name']}({one}) 数据已经刷新")
            return
        # 此时codes务必设置为None
        web_data, record, kwargs = self._get_one(fetch_data_func, one, kwargs,
                                                 None, start, end)
        hdf.add(web_data, record, kwargs)

        # 然后完成附加代码
        for code in new_codes:
            self._one_by_one(one, code, kwargs)

    def refresh_batch(self, batch):
        """分批刷新"""
        kwargs = {
            'min_itemsize': {
                '股票简称': 20,
            },
        }
        api = AdvanceSearcher()
        if self._fs is None:
            self._fs = FastSearcher()
        codes = api.codes
        fetch_data_func = api.get_data
        for one in batch:
            try:
                self.refresh_one(fetch_data_func, one, kwargs, codes)
            except Exception as e:
                print(f"{e!r}")
        api.driver.quit()
        if self._fs:
            self._fs.driver.quit()


class MarginDataRefresher(RefresherBase):
    """融资融券刷新器"""
    @property
    def iterables(self):
        """循环列表"""
        min_start = self.get_min_date(None)
        try:
            record = self.get_record(None)
            start = record['last_date'] + pd.Timedelta(days=1)
            if start < min_start:
                start = min_start
        except Exception:
            start = min_start
        end = pd.Timestamp('now', tz=TZ)
        # 以时点判断结束日期，昨日或前日
        if end.hour >= 9:
            end = end - pd.Timedelta(days=1)
        else:
            end = end - pd.Timedelta(days=2)
        dates = pd.date_range(start, end, freq='B')
        dates = [d.strftime(r'%Y-%m-%d') for d in dates]
        return dates

    def get_min_date(self, one):
        """项目最初日期"""
        min_start = TS_CONFIG['8.2']['date_field'][1]
        min_start = pd.Timestamp(min_start, tz=TZ)
        return min_start

    def get_data_path(self, one):
        """分日期存储数据路径"""
        return data_root('margin.h5')

    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_refresh_num(self, one):
        """刷新时点"""
        return 9

    def get_freq(self, one):
        """刷新频率"""
        return 'B'

    def get_data_columns(self, one):
        """查询数据列"""
        return TS_CONFIG['8.2']['data_columns']

    def get_col_dtypes(self, one):
        """列数据类型"""
        return get_field_type('ts', '8.2')

    def get_fetch_func_arg(self, one, start, end):
        """构造网页提取函数的参数(部分子类需要重写)"""
        return (one, )

    def get_index_col(self, one):
        """索引列名称"""
        return TS_CONFIG['8.2']['date_field'][0]

    def refresh_one(self, one, web_data):
        # 列名称长度限定
        kwargs = {
            'data_columns': self.get_data_columns(None),
            'min_itemsize': {
                '股票简称': 20,
            },
        }
        hdf = self.get_hdfdata(one)
        record = self.get_record(one)
        record['completed'] = True
        record['index_col'] = self.get_index_col(None)
        record, web_data = self._normalize_data(one, web_data, record, True)
        record['completed_time'] = pd.Timestamp.now(tz=TZ)
        record['subset'] = self.get_data_columns(None)
        # 异步导致数据重复
        web_data = web_data.drop_duplicates(record['subset'])
        record['retry_times'] = 1
        record['last_date'] = pd.Timestamp(one, tz=TZ)
        hdf.add(web_data, record, kwargs)

    def refresh_all(self):
        """刷新"""
        with ThematicStatistics() as api:
            # 自最后日期起至昨日
            for d in self.iterables:
                web_data = api.get_data('8.2', d, d)
                if not web_data.empty:
                    # 需要限定日期，此时为字符串
                    web_data = web_data[web_data['交易日期'] == d]
                self.refresh_one(d, web_data)
                time.sleep(random.randint(1, 3) / 10)


class ClassifyTreeRefresher(RefresherBase):
    """股票分类树刷新器"""
    @property
    def iterables(self):
        """循环列表"""
        return list(range(1, 7))

    def get_data_path(self, one):
        """分日期存储数据路径"""
        return data_root(f'cninfo/classify_tree/{one}.h5')

    def get_table_data(self):
        """获取表数据"""
        dfs = []
        for one in self.iterables:
            dfs.append(self.get_hdfdata(one).data)
        return pd.concat(dfs, sort=True)

    def get_mode(self, one):
        """刷新模式"""
        # 每周刷新
        return 'w'

    def get_fetch_func_arg(self, one, start, end):
        """构造网页提取函数的参数(部分子类需要重写)"""
        return (one, )

    def get_min_date(self, one):
        """项目最初日期"""
        return MARKET_START

    def get_index_col(self, one):
        """索引列名称"""
        return None

    def get_freq(self, one):
        """刷新频率"""
        # 按天刷新
        return 'D'

    def get_refresh_num(self, one):
        """可进行项目刷新的小时数"""
        return 8

    def get_col_dtypes(self, one):
        """列数据类型"""
        return {
            'd_cols': [],
            's_cols': ['股票代码', '股票简称', '分类层级', '分类名称', '分类编码', '平台类别'],
            'i_cols': [],
        }

    def get_data_columns(self, one):
        """查询数据列"""
        return ['股票代码', '平台类别']

    def refresh_all(self):
        kwargs = {'min_itemsize': {'分类名称': 30, '平台类别': 20}}
        data_columns = self.get_data_columns(None)
        kwargs.update({'data_columns': data_columns})
        with ClassifyTree(False) as api:
            fetch_data_func = api.get_classify_tree
            for level in self.iterables:
                self.refresh_one(fetch_data_func, level, kwargs)


class ClassifyBomRefresher(RefresherBase):
    """股票分类bom"""
    @property
    def iterables(self):
        """循环列表"""
        return [1]

    def get_data_path(self, one):
        """分日期存储数据路径"""
        return data_root('cninfo/classify_bom.h5')

    def get_mode(self, one):
        """刷新模式"""
        return 'w'

    def get_fetch_func_arg(self, one, start, end):
        """构造网页提取函数的参数(部分子类需要重写)"""
        return (one, )

    def get_min_date(self, one):
        """项目最初日期"""
        return MARKET_START

    def get_index_col(self, one):
        """索引列名称"""
        return None

    def get_freq(self, one):
        """刷新频率"""
        # 按周刷新
        return 'W'

    def get_refresh_num(self, one):
        """可进行项目刷新的小时数"""
        return 8

    def get_col_dtypes(self, one):
        """列数据类型"""
        return {
            'd_cols': [],
            's_cols': ['分类编码', '分类名称'],
            'i_cols': [],
        }

    def get_data_columns(self, one):
        """查询数据列"""
        return ['分类编码', '分类名称']

    def refresh_batch(self, batch):
        """分批刷新"""
        kwargs = {'min_itemsize': {'分类名称': 30}}
        with ClassifyTree(False) as api:
            # 属性 -> func
            def f(one):
                return api.classify_bom

            for one in batch:
                self.refresh_one(f, one, kwargs)


# endregion


# region 网易数据
class WYSRefresher(RefresherBase):
    """网易股票日线行情刷新器"""
    @property
    def iterables(self):
        """循环列表"""
        return get_recent_trading_stocks()

    def get_data_path(self, one):
        return data_root(f'wy_stock/{one}.h5')

    def get_col_dtypes(self, one):
        """列数据类型"""
        return {
            'd_cols': ['日期'],
            's_cols': ['股票代码', '名称'],
            'i_cols': ['成交量', '成交笔数'],
        }

    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_min_date(self, one):
        """项目最初日期"""
        return MARKET_START

    def get_index_col(self, one):
        """索引列名称"""
        return '日期'

    def get_freq(self, one):
        """刷新频率"""
        return 'B'

    def get_refresh_num(self, one):
        """可进行项目刷新的小时数"""
        return 17

    def get_data_columns(self, one):
        """查询数据列"""
        return ['股票代码', '日期']

    def refresh_batch(self, batch):
        """分批刷新"""
        kwargs = {
            'min_itemsize': {
                '名称': 20,
            },
        }
        for one in batch:
            self.refresh_one(fetch_history, one, kwargs)


class WYIRefresher(RefresherBase):
    """网易股指日线行情刷新器"""
    @property
    def iterables(self):
        """循环列表"""
        return list(MAIN_INDEX.keys())

    def get_data_path(self, one):
        return data_root(f'wy_index/{one}.h5')

    def get_col_dtypes(self, one):
        """列数据类型"""
        return {
            'd_cols': ['日期'],
            's_cols': ['股票代码', '名称'],
            'i_cols': ['成交量', '成交笔数'],
        }

    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_min_date(self, one):
        """项目最初日期"""
        return MARKET_START

    def get_index_col(self, one):
        """索引列名称"""
        return '日期'

    def get_freq(self, one):
        """刷新频率"""
        return 'B'

    def get_refresh_num(self, one):
        """可进行项目刷新的小时数"""
        return 17

    def get_data_columns(self, one):
        """查询数据列"""
        return ['股票代码', '日期']

    def refresh_batch(self, batch):
        """分批刷新"""
        kwargs = {'data_columns': ['日期']}
        fetch_data_func = partial(fetch_history, is_index=True)
        for one in batch:
            self.refresh_one(fetch_data_func, one, kwargs)


# endregion


# region 其他数据
class DisclosureRefresher(RefresherBase):
    """公司公告刷新器"""
    @property
    def iterables(self):
        """循环列表"""
        min_start = self.get_min_date(None)
        try:
            record = self.get_record(None)
            start = record['last_date'] - pd.Timedelta(days=1)
            if start < min_start:
                start = min_start
        except Exception:
            start = min_start
        end = pd.Timestamp('now', tz=TZ)
        if end.hour >= 16:
            end = end + pd.Timedelta(days=1)
        dates = pd.date_range(start, end)
        dates = [d.strftime(r'%Y%m%d') for d in dates]
        return dates

    def get_data_path(self, one):
        """分日期存储数据路径"""
        return data_root('disclosure.h5')

    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_col_dtypes(self, one):
        """列数据类型"""
        return {
            'd_cols': ['公告时间'],
            's_cols': ['下载网址', '公告标题', '股票代码', '股票简称'],
            'i_cols': ['序号'],
        }

    def get_fetch_func_arg(self, one, start, end):
        """构造网页提取函数的参数(部分子类需要重写)"""
        return (one, )

    def get_min_date(self, one):
        """项目最初日期"""
        return pd.Timestamp('2010-01-01', tz=TZ)

    def get_index_col(self, one):
        """索引列名称"""
        return '公告时间'

    def get_freq(self, one):
        """刷新频率"""
        return 'H'

    def get_refresh_num(self, one):
        """每6小时刷新一次"""
        return 6

    def get_data_columns(self, one):
        """查询数据列"""
        return ['股票代码', '公告时间', '序号']

    def refresh_one(self, one, web_data):
        # 标题长度限定
        kwargs = {
            'min_itemsize': {
                '公告标题': 600,
                '下载网址': 120,
                '股票简称': 20,
            },
            'data_columns': ['股票代码', '公告时间', '序号'],
        }
        hdf = self.get_hdfdata(one)
        record = self.get_record(one)
        record['completed'] = True
        record['index_col'] = '公告时间'
        record, web_data = self._normalize_data(one, web_data, record, True)
        record['completed_time'] = pd.Timestamp.now(tz=TZ)
        record['next_time'] = pd.Timestamp.now(tz=TZ) + pd.Timedelta(
            minutes=30)
        record['retry_times'] = 1
        record['last_date'] = pd.Timestamp(one, tz=TZ)
        record['subset'] = ['股票代码', '公告时间', '序号']
        hdf.add(web_data, record, kwargs)

    async def refresh_all(self):
        """刷新"""
        # 自最后日期起，至当日（或明日）至
        # 初始化时，如果单次时段超过半年，会导致远程主机强迫关闭了一个现有的连接
        # 休眠一段，再次运行即可。
        for d in self.iterables:
            web_data = await fetch_one_day(d)
            self.refresh_one(d, web_data)
            delay = random.randint(100, 500) / 100
            # asyncio.sleep(delay)
            time.sleep(delay)


class SinaNewsRefresher(RefresherBase):
    """财经消息"""
    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_data_path(self, one):
        """分日期存储数据路径"""
        return data_root('news.h5')

    def get_table_data(self):
        """分日期存储数据"""
        hdf = HDFData(self.get_data_path(None), self.get_mode(None))
        return hdf.data

    def get_min_date(self, one):
        """项目最初日期"""
        return pd.Timestamp('2010-01-01', tz=TZ)

    def get_index_col(self, one):
        """索引列名称"""
        return '时间'

    def get_freq(self, one):
        """刷新频率"""
        return 'D'

    def get_refresh_num(self, one):
        """频次（每30分钟）"""
        return 30

    def get_data_columns(self, one):
        """查询数据列"""
        return ['证券代码', '交易日期']

    def refresh_all(self, times=3):
        """刷新"""
        # 不可按项目循环。
        record = {
            'index_col': '时间',
        }
        kwargs = {'data_columns': ['时间', '分类'], 'min_itemsize': {'概要': 2000}}
        with Sina247News() as api:
            history = api.history_news(times)
        history.drop(columns='序号', inplace=True)
        history.sort_values('时间', inplace=True)
        record['completed'] = True
        now = pd.Timestamp.now(tz=TZ)
        record['completed_time'] = now
        record['next_time'] = time_for_next_update(now, 'H', 30)
        fp = self.get_data_path(None)
        hdf = HDFData(fp, self.get_mode(None))
        hdf.add(history, record, kwargs)


class TreasuryRefresher(RefresherBase):
    """国库券利率"""
    def get_mode(self, one):
        """刷新模式"""
        return 'a'

    def get_data_path(self):
        """存储数据路径"""
        return data_root('treasury/treasury.h5')

    def get_table_data(self):
        """表数据"""
        fp = self.get_data_path()
        return pd.read_hdf(fp, 'data')

    def get_min_date(self, one):
        """项目最初日期"""
        return EARLIEST_POSSIBLE_DATE.tz_convert(TZ)

    def get_index_col(self, one):
        """索引列名称"""
        return '日期'

    def get_freq(self, one):
        """刷新频率"""
        return 'D'

    def get_refresh_num(self, one):
        """晚上6点"""
        return 18

    def refresh_all(self, times=3):
        """刷新"""
        download_last_year()
        df = fetch_treasury_data_from()
        fp = self.get_data_path()
        df.to_hdf(fp, 'data', mode='w')


# endregion
