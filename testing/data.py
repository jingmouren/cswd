"""h5格式数据

Notes:
    1. 模式：  `a` 添加数据： 数据追加到原文件
              `w`  覆盖更新：只保留最近添加的数据
    2. 可添加模式通过记录`max_index`的值限定`index_col`列，防止重复
    3. 数据保存在`data`
    4. 记录保持在'record'
    5. `last_date`专门用于计算start
    6. `next_time`指示下一次可更新时间
"""

import pandas as pd
from cnswd.utils import make_logger
from cnswd.setting.constants import MARKET_START

default_status = {
    'completed': False,             # 网络提取状态，如途中发生异常为False
    'retry_times': 0,               # 记录网络提取尝试次数
    'index_col': None,              # **单**个索引列名称
    'max_index': None,              # 索引列的最大值
    'completed_time': MARKET_START, # 完成时间
    'subset': None,                 # 数据唯一性索引，可为列名称或者列名称列表
    'freq': None,                   # 刷新频率
    'next_time': MARKET_START,      # 下次可刷新数据的时间
    'last_date': MARKET_START,      # 刷新的结束日期
    'memo': '-'
}

# 参数模板
to_hdf_kwargs = {
    # 'append': True,
    'format': 'table',
    'complevel': 9,
    'complib': 'blosc:blosclz',
    # 索引列
    # 'data_columns': ['id'],
    'index': False,
    # 字符串最小长度
    # 'min_itemsize': {
    #     '概要': 2000
    # }
}


class HDFData(object):
    """h5格式存储的数据及刷新记录"""
    def __init__(self, fp, mode):
        """初始数据对象
        
        Arguments:
            object {Path} -- 数据存放路径（带扩展名.h5）
            fp {Path} -- 数据存放路径（带扩展名.h5）
            mode {str} -- 数据对象写入方式。只支持添加模式`a`和`w`覆盖模式。
        """
        assert fp.name.endswith('.h5'), '扩展名必须为`.h5`'
        self._fp = fp
        assert mode in ('a', 'w'), '只支持添加和写入模式'
        self._mode = mode
        self._record = default_status.copy()
        self.logger = make_logger('HDFData')

    @property
    def file_path(self):
        """文件路径"""
        return self._fp

    @property
    def mode(self):
        """读写模式"""
        return self._mode

    @property
    def data(self):
        """数据框对象"""
        return pd.read_hdf(self._fp, 'data')

    @property
    def record(self):
        """刷新记录"""
        try:
            self._record = pd.read_hdf(self._fp, 'record').to_dict()
        except Exception:
            pass
        return self._record

    @property
    def has_data(self):
        """是否有数据（空数据视同无数据）"""
        try:
            return not self.data.empty
        except (KeyError, FileNotFoundError):
            return False

    @property
    def appendable(self):
        """可添加"""
        return self._mode == 'a'

    def get_next_time(self, use_last_date):
        if use_last_date:
            return self.record['last_date'] + pd.Timedelta(days=1)
        else:
            return self.record['next_time']

    def get_max_index(self, index_col):
        """当前索引最大值"""
        if self._mode == 'w':
            return None
        record = self.record
        max_index = record['max_index']
        if self.has_data:
            if max_index is None:
                return max(self.data[index_col])
            else:
                return max_index
        return None

    def _check_valid(self, record):
        if self.appendable:
            col = record.get('index_col', None)
            assert col, '添加模式必须指定索引列名称'

    def _set_record(self, record):
        """设置刷新记录(仅当存在数据对象时有效)"""
        self._check_valid(record)
        self._record.update(record)
        s = pd.Series(self._record)
        # 刷新方式写入记录
        s.to_hdf(self._fp, 'record', append=False)

    def _rewrite(self, df, record, kwargs):
        """重写"""
        if not df.empty:
            df.to_hdf(self._fp, 'data', append=False, index=False)
        self._set_record(record)

    def _get_to_add(self, df, record):
        """截取添加数据"""
        if self.has_data:
            index_col = record['index_col']
            subset = record.get('subset', index_col)
            max_index = self.get_max_index(index_col)
            old_cond = self.data[index_col] == max_index
            old = self.data[old_cond]
            to_add_cond = df[index_col] >= max_index
            to_add = df[to_add_cond]
            merged = pd.concat([old, to_add], sort=False)
            # 不保留重复部分
            to_add = merged.drop_duplicates(subset, keep=False)
            return to_add
        else:
            return df

    def _append(self, df, record, kwargs):
        kwargs['append'] = True
        index_col = record['index_col']
        max_index = self.record['max_index']
        # 强制添加标志
        add_flag = record.get('add_flag', None)
        if not df.empty:
            # 更新最大值
            max_index = max(df[index_col])
            if not self.has_data:
                df.to_hdf(self._fp, 'data', **kwargs)
                rows = len(df)
            else:
                if add_flag:
                    df.to_hdf(self._fp, 'data', **kwargs)
                    rows = len(df)
                else:
                    to_add = self._get_to_add(df, record)
                    if len(to_add) >= 1:
                        to_add.to_hdf(self._fp, 'data', **kwargs)
                        max_index = max(to_add[index_col])
                        rows = len(to_add)
                    else:
                        rows = 0
            self.logger.info(f"添加{rows}行 -> {self._fp}")
            record['max_index'] = max_index
        self._set_record(record)

    def add(self, df, record, kwargs={}):
        """添加或重写数据及记录"""
        default = to_hdf_kwargs.copy()
        default.update(kwargs)
        if self.appendable:
            self._append(df, record, default)
        else:
            self._rewrite(df, record, default)
        store = pd.HDFStore(self._fp)
        if store.is_open:
            store.close()

    def create_table_index(self, index_col):
        """创建索引"""
        if index_col is None:
            return
        columns = [index_col]
        store = pd.HDFStore(self._fp)
        store.create_table_index('data',
                                 columns=columns,
                                 optlevel=9,
                                 kind='full')
        store.close()


pd.to_datetime
import pandas as pd
from cnswd.utils import data_root
from cnswd.setting.constants import TZ

fp = data_root('test.h5')

df = pd.DataFrame({
    'close': [1.0, 2.0, 3.0],
    '日期': pd.date_range('2019-01-01', '2019-01-03')
})

record = {
    'completed': True,
    'retry_times': 1,
    'index_col': '日期',
    'max_index': None,
    'completed_time': MARKET_START,
    'freq': None,
    'next_time': MARKET_START,
    'last_date': MARKET_START,
    'memo': '-'
}
web_data = pd.DataFrame({
    'close': [31.0, 41.0, 51.0],
    '日期': pd.date_range('2019-01-03', '2019-01-05')
})

# 对象

# 覆盖模式

h = HDFData(fp, 'w')
kwargs = {
    'format': 'table',
    'complevel': 9,
    'complib': 'blosc:blosclz',
    # 索引列
    # 'data_columns': ['id'],
    'index': False,
    # 字符串最小长度
    # 'min_itemsize': {
    #     '概要': 2000
    # }
}
h.add(df, record, kwargs)
h.add(web_data, record, kwargs)
h.data

# 只保留最后记录
h.record
record = {
    'completed': True,
    'retry_times': 2,
    'index_col': '日期',
    'max_index': None,
    'completed_time': MARKET_START,
    'freq': None,
    'next_time': MARKET_START,
    'last_date': MARKET_START,
    'memo': '测试'
}
h._set_record(record)
h.record

h = HDFData(fp, 'w')
kwargs = {
    'format': 'table',
    'complevel': 9,
    'complib': 'blosc:blosclz',
    # 索引列
    # 'data_columns': ['id'],
    'index': False,
    # 字符串最小长度
    # 'min_itemsize': {
    #     '概要': 2000
    # }
}

# 仅刷新状态
record = {
    'completed': True,
    'retry_times': 1,
    'index_col': '日期',
    'max_index': None,
    'completed_time': pd.Timestamp.now(tz=TZ),
    'freq': 'D',
    'next_time': MARKET_START,
    'last_date': MARKET_START,
    'memo': '-'
}
h.add(pd.DataFrame(), record, {})

# 重写覆盖
kwargs = {
    'append': False,
    'format': 'table',
    'complevel': 9,
    'complib': 'blosc:blosclz',
    # 索引列
    # 'data_columns': ['id'],
    'index': False,
    # 字符串最小长度
    # 'min_itemsize': {
    #     '概要': 2000
    # }
}
h.add(web_data, record, kwargs)

# 添加
# 1. 最后日期
# 2. NOW

#
h.data
h.record