import tempfile
# from itertools import product
from os.path import join
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cnswd.cninfo import FastSearcher
from cnswd.setting.config import DB_CONFIG
from cnswd.utils.path_utils import data_root


@pytest.fixture
def pageapi():
    with FastSearcher(False) as api:
        yield api


def get_expected_data(level, code):
    item = '_'.join(level.split('.'))
    fp = data_root(item, './testing/data/fs') / f"{code}.csv"
    na_values = ['-', '无', ';']
    dtype = {'股票代码': str, '证券代码': str}
    try:
        return pd.read_csv(fp, dtype=dtype, na_values=na_values)
    except Exception:
        return pd.DataFrame()


def get_actual_data(df):
    temp = join(tempfile.mkdtemp(), 'x.csv')
    df.to_csv(temp, index=False)
    na_values = ['-', '无', ';']
    dtype = {'股票代码': str, '证券代码': str}
    try:
        return pd.read_csv(temp, dtype=dtype, na_values=na_values)
    except Exception:
        return pd.DataFrame()


@pytest.mark.parametrize("level", list(DB_CONFIG.keys()))
@pytest.mark.parametrize("code", ['000001', '300033'])
def test_level(pageapi, level, code):
    """测试读取所有项目数据"""
    t1 = '2014-01-01'
    t2 = '2016-03-31'
    origin = pageapi.get_data(level, code, t1, t2)
    actual = get_actual_data(origin)
    expected = get_expected_data(level, code)
    assert_frame_equal(actual, expected, check_dtype=False,
                       check_index_type=False)
