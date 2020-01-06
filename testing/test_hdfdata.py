"""
并行测试下，文件名称务必唯一
"""
import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cnswd.data import HDFData, default_status
from cnswd.utils import data_root

df1 = pd.DataFrame({
    'price': [1., 2., 3.],
    'date': pd.date_range('2019-01-01', periods=3)
})
df2 = pd.DataFrame({
    'price': [1., 2., 3.],
    'date': pd.date_range('2019-02-01', periods=3)
})
df3 = pd.DataFrame({
    'price': [1., 2., 3.],
    'date': pd.date_range('2019-02-03', periods=3)
})
df4 = pd.DataFrame({
    'price': [1., 2., 3.],
    'date': pd.date_range('2019-02-03', periods=3)
})
record = default_status


# @pytest.mark.skip
def test_mode_w():
    fp = data_root('TEST/store_w.h5')
    hdf = HDFData(fp, 'w')
    hdf.add(df1, record)
    hdf.add(df2, record)
    actual = hdf.data
    assert_frame_equal(actual, df2)
    fp.unlink()


# @pytest.mark.skip
def test_mode_a_all():
    fp = data_root('TEST/store_all.h5')
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    hdf.add(df1, record)
    hdf.add(df2, record)
    actual = hdf.data
    expected = pd.concat([df1, df2])
    assert_frame_equal(actual, expected)

    df = pd.DataFrame({
        'price': [1., 2., 3.],
        'date': pd.date_range('2019-03-03', periods=3)
    })
    hdf.add(df, record)
    assert hdf.data.shape == (9, 2)

    fp.unlink()


# @pytest.mark.skip
def test_mode_a_part():
    """添加部分(添加不重复的数据)"""
    fp = data_root('TEST/store_part.h5')
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    hdf.add(df1, record)
    hdf.add(df2, record)
    hdf.add(df3, record)
    actual = hdf.data
    assert actual.shape == (8, 2)
    fp.unlink()


# @pytest.mark.skip
def test_str_len():
    record = default_status.copy()
    record['index_col'] = 'date'
    df5 = pd.DataFrame({
        'price': np.random.random(3),
        'date': pd.date_range('2019-02-03', periods=3),
        'str': ['a' * 100, 'b' * 20, 'c' * 10],
    })
    df6 = pd.DataFrame({
        'price': np.random.random(3),
        'date': pd.date_range('2019-03-03', periods=3),
        'str': ['a' * 200, 'b' * 20, 'c' * 10],
    })
    fp = data_root('TEST/store_str.h5')
    hdf = HDFData(fp, 'a')

    # 长度不够
    hdf.add(df5, record)
    with pytest.raises(ValueError):
        hdf.add(df6, record)

    fp.unlink()
    del hdf
    fp = data_root('TEST/store_str.h5')
    hdf = HDFData(fp, 'a')
    # 注意写法
    kwargs = {'min_itemsize': {'str': 300}}
    hdf.add(df5, record, kwargs)
    hdf.add(df6, record, kwargs)
    fp.unlink()
    del hdf

    # 中文标题
    df5 = pd.DataFrame({
        'price': np.random.random(3),
        'date': pd.date_range('2019-02-03', periods=3),
        '中文标题': ['a' * 100, 'b' * 20, 'c' * 10],
    })
    df6 = pd.DataFrame({
        'price': np.random.random(3),
        'date': pd.date_range('2019-03-03', periods=3),
        '中文标题': ['a' * 200, 'b' * 20, 'c' * 10],
    })

    fp = data_root('TEST/store_str.h5')
    hdf = HDFData(fp, 'a')    
    kwargs = {'min_itemsize': {'中文标题': 300}}
    hdf.add(df5, record, kwargs)
    hdf.add(df6, record, kwargs)
    del hdf   
    fp.unlink()


# @pytest.mark.skip
def test_record():
    fp = data_root('TEST/store_record.h5')
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    index_col = 'date'
    record['index_col'] = index_col
    hdf.add(df1, record)
    assert hdf.get_max_index(index_col) == max(df1[index_col])
    hdf.add(df2, record)
    assert hdf.get_max_index(index_col) == max(df2[index_col])
    hdf.add(df3, record)
    assert hdf.get_max_index(index_col) == max(df3[index_col])
    # 没有添加，其值不变
    old = hdf.get_max_index(index_col)
    hdf.add(df4, record)
    assert hdf.get_max_index(index_col) == old
    fp.unlink()


# @pytest.mark.skip
def test_cn_columns():
    record = default_status.copy()
    record['index_col'] = '日期'
    df5 = pd.DataFrame({
        'price': np.random.random(3),
        '日期': pd.date_range('2019-02-03', periods=3),
        '中文标题': ['a' * 100, 'b' * 20, 'c' * 10],
    })
    fp = data_root('TEST/store_cn.h5')
    hdf = HDFData(fp, 'a')
    # 注意写法
    kwargs = {'min_itemsize': {'中文标题': 300}}
    hdf.add(df5, record, kwargs)
    assert '中文标题' in hdf.data.columns
    fp.unlink()