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
    'code': ['000001'] * 3,
    'date': pd.date_range('2019-01-01', periods=3)
})
df2 = pd.DataFrame({
    'price': [1., 2., 3.],
    'code': ['000001'] * 3,
    'date': pd.date_range('2019-02-01', periods=3)
})
df3 = pd.DataFrame({
    'price': [2., 3., 4.],
    'code': ['000001'] * 3,
    'date': pd.date_range('2019-02-02', periods=3)
})
df4 = pd.DataFrame({
    'price': [1., 2., 3.],
    'code': ['000001'] * 3,
    'date': pd.date_range('2019-02-03', periods=3)
})

df5 = pd.DataFrame({
    'code': ['000001', '000002', '000003'],
    'info': ['a', 'b', 'c']
})
df6 = pd.DataFrame({
    'code': ['000001', '000002', '000003', '000004'],
    'info': ['a', 'b', 'c', 'd']
})

df7 = pd.DataFrame({
    'price': [1., 2., 3.] * 2,
    'code': ['000001'] * 3 + ['000002'] * 3,
    'date': list(pd.date_range('2019-02-03', periods=3)) * 2
})

df8 = pd.DataFrame({
    'price': [1., 2.] * 2,
    'code': ['000003'] * 4,
    'date': pd.date_range('2019-02-04', periods=4),
})

df9 = pd.DataFrame({
    'price': [1., 2.] * 2,
    'code': ['000003'] * 4,
    'date': pd.date_range('2019-02-05', periods=4),
})

record = default_status
record['completed'] = True


# @pytest.mark.skip
def test_completed():
    """测试完成状态

    如果未完成，不执行添加
    """
    fp = data_root('TEST/store_completed.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'w')
    record['completed'] = False
    hdf.add(df1, record)
    with pytest.raises(FileNotFoundError):
        hdf.data


# @pytest.mark.skip
def test_mode_w():
    """测试覆盖式写入

    期望最终查询结果仅为最后写入的数据
    """
    fp = data_root('TEST/store_w.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'w')
    hdf.add(df1, record)
    hdf.add(df2, record)
    actual = hdf.data
    assert_frame_equal(actual, df2)
    fp.unlink()


# @pytest.mark.skip
def test_append_without_data_columns():
    """添加模式不指定数据列触发KeyError"""
    fp = data_root('TEST/store_without_data_columns.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    with pytest.raises(KeyError):
        hdf.add(df1, record)


# @pytest.mark.skip
def test_append_without_overlap():
    """测试不重叠添加"""
    fp = data_root('TEST/store_without_overlap.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    kwargs = {}
    kwargs['data_columns'] = ['code', 'date']
    hdf.add(df1, record, kwargs)
    hdf.add(df2, record, kwargs)
    actual = hdf.data
    expected = pd.concat([df1, df2])
    assert_frame_equal(actual, expected)
    fp.unlink()


# @pytest.mark.skip
def test_get_to_add():
    fp = data_root('TEST/store_get_to_add.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    # 重写
    record['index_col'] = index_col = None
    kwargs = {}
    data_columns = ['code', 'date']
    kwargs['data_columns'] = data_columns
    subset = data_columns
    hdf.add(df2, record, kwargs)
    to_add, action = hdf._get_to_add(df4, record, subset)
    assert action == 'rewrite'
    expected = pd.concat([df2, df4]).drop_duplicates(subset, keep='first')
    assert_frame_equal(to_add, expected)

    # 添加
    record['index_col'] = index_col = 'date'
    to_add, action = hdf._get_to_add(df4, record, subset)
    old_max_index = hdf.get_max_index(index_col)
    assert action == 'append'
    expected = df4[df4[index_col] > old_max_index]
    assert_frame_equal(to_add, expected)
    fp.unlink()


# @pytest.mark.skip
def test_append_with_overlap():
    """测试重叠添加"""
    fp = data_root('TEST/store_with_overlap.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    kwargs = {}
    data_columns = ['code', 'date']
    kwargs['data_columns'] = data_columns
    hdf.add(df2, record, kwargs)
    hdf.add(df4, record, kwargs)
    actual = hdf.data
    # 不指定子集时，使用data_columns
    subset = data_columns
    expected = pd.concat([df2, df4]).drop_duplicates(subset, keep='first')
    assert_frame_equal(actual, expected)
    fp.unlink()


# @pytest.mark.skip
def test_append_with_overlap_but_no_index_col():
    """测试重叠添加"""
    fp = data_root('TEST/store_with_overlap_but_no_index_col.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = None
    kwargs = {}
    data_columns = ['code']
    kwargs['data_columns'] = data_columns
    hdf.add(df5, record, kwargs)
    hdf.add(df6, record, kwargs)
    actual = hdf.data
    # 不指定子集时，使用data_columns
    subset = data_columns
    expected = pd.concat([df5, df6]).drop_duplicates(subset, keep='first')
    assert_frame_equal(actual, expected)
    fp.unlink()


# @pytest.mark.skip
def test_add_report_data():
    df1 = pd.DataFrame({
        'item': [1., 2.],
        'code': ['000001', '000002'],
        'date': pd.to_datetime(['2019-03-31'] * 2),
    })
    df2 = pd.DataFrame({
        'item': [1., 2., 3.],
        'code': ['000001', '000002', '000003'],
        'date': pd.to_datetime(['2019-03-31'] * 3),
    })
    fp = data_root('TEST/store_add_report_data.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    kwargs = {}
    data_columns = subset = ['code', 'date']
    kwargs['data_columns'] = data_columns
    # 第一次添加
    hdf.add(df1, record, kwargs)
    assert_frame_equal(hdf.data, df1)
    # 重复添加不会增加数据
    hdf.add(df1, record, kwargs)
    assert_frame_equal(hdf.data, df1)
    # 第二次添加
    hdf.add(df2, record, kwargs)
    expected = pd.concat([df1, df2]).drop_duplicates(subset, keep='first')
    assert_frame_equal(hdf.data, expected)
    fp.unlink()


# @pytest.mark.skip
def test_warning():
    fp = data_root('TEST/store_warning.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    index_col = 'date'
    kwargs = {}
    kwargs['data_columns'] = ['code', 'date']
    kwargs['subset'] = ['code', 'date']
    record['index_col'] = index_col
    hdf.add(df2, record, kwargs)
    with pytest.warns(UserWarning):
        hdf.add(df3, record, kwargs)
    print('2019-02-02重复')
    print(hdf.data)


# @pytest.mark.skip
def test_insert_by():
    """测试按代码添加

    深证信高级搜索默认全部代码为当前交易的股票代码
    而当前停牌的股票未包含
    所以当添加个股财务报表时，历史数据无法插入
    
    为解决此类问题，新增`insert_by`
    即以`by_col`为条件插入新增部分数据
    """
    fp = data_root('TEST/store_insert_by.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    record['index_col'] = 'date'
    kwargs = {}
    data_columns = ['code', 'date']
    kwargs['data_columns'] = data_columns
    hdf.add(df7, record, kwargs)
    hdf.insert_by(df8, record, kwargs, 'code', '000003')
    actual = hdf.data
    expected = pd.concat([df7, df8])
    # 只能添加非重复部分
    hdf.insert_by(df9, record, kwargs, 'code', '000003')
    actual = hdf.data
    subset = ('code', 'date')
    expected = pd.concat([df7, df8, df9]).drop_duplicates(subset, keep='first')
    assert_frame_equal(actual, expected)
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
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()

    hdf = HDFData(fp, 'a')
    kwargs = {}
    data_columns = ['date']
    kwargs['data_columns'] = data_columns
    # 长度不够
    hdf.add(df5, record, kwargs)
    with pytest.raises(ValueError):
        hdf.add(df6, record, kwargs)

    fp.unlink()
    del hdf
    fp = data_root('TEST/store_str.h5')
    hdf = HDFData(fp, 'a')
    # 注意写法
    kwargs.update({'min_itemsize': {'str': 300}})
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
    kwargs['subset'] = ['date']
    kwargs.update({'min_itemsize': {'中文标题': 300}})
    hdf.add(df5, record, kwargs)
    hdf.add(df6, record, kwargs)
    del hdf
    fp.unlink()


# @pytest.mark.skip
def test_record():
    fp = data_root('TEST/store_record.h5')
    # 确保干净测试环境
    if fp.exists():
        fp.unlink()
    hdf = HDFData(fp, 'a')
    record = default_status.copy()
    index_col = 'date'
    kwargs = {}
    kwargs['data_columns'] = ['code', 'date']
    kwargs['subset'] = ['code', 'date']
    record['index_col'] = index_col
    hdf.add(df1, record, kwargs)
    assert hdf.get_max_index(index_col) == max(df1[index_col])
    hdf.add(df2, record, kwargs)
    assert hdf.get_max_index(index_col) == max(df2[index_col])
    hdf.add(df4, record, kwargs)
    assert hdf.get_max_index(index_col) == max(df4[index_col])
    print(hdf.data)
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
    kwargs = {}
    kwargs['data_columns'] = ['日期']
    kwargs['subset'] = ['日期']
    # 注意写法
    kwargs.update({'min_itemsize': {'中文标题': 300}})
    hdf.add(df5, record, kwargs)
    assert '中文标题' in hdf.data.columns
    fp.unlink()