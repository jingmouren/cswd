"""测试市场行情数据

重点:
    1. 测试期间所有
    2. 当代码改变后的响应是否正确
"""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cnswd.cninfo import MarketIndexData
from cnswd.utils import data_root, kill_firefox

START_DATE = '1995-01-23'
END_DATE = '2019-11-24'


@pytest.fixture
def pageapi():
    # 确保干净的测试环境
    kill_firefox()
    with MarketIndexData() as api:
        yield api


def get_expected_data(code):
    fp = data_root(f"{code}.csv", './testing/data/md/index')
    na_values = ['-', '无', 'NaN']
    dtype = {'指数代码': str}
    try:
        # 无序号列
        df = pd.read_csv(fp, index_col=False, dtype=dtype, na_values=na_values)
        return df.sort_values('交易日期').query(f"交易日期>='{START_DATE}'").query(f"交易日期<='{END_DATE}'")
    except Exception:
        return pd.DataFrame()


# @pytest.mark.skip
@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('399001', None, END_DATE, (6024, 9)),
    ('399005',  None, END_DATE,  (3363, 9)),
])
def test_with_enddate(pageapi, code, t1, t2, expected_shape):
    """测试期间数据"""
    actual = pageapi.get_data(code, t1, t2)
    actual = actual.sort_values('交易日期').reset_index(drop=True)
    assert actual.shape == expected_shape
    expected = get_expected_data(code).reset_index(drop=True)
    assert_frame_equal(actual, expected)


# @pytest.mark.skip
@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('399005', '2006-01-25', '2019-11-21', (3361, 9)),
    ('399001', '1995-01-24', '2019-11-21', (6022, 9)),
])
def test_with_start_and_end(pageapi, code, t1, t2, expected_shape):
    """测试定义区间的数据"""
    actual = pageapi.get_data(code, t1, t2)
    actual = actual.sort_values('交易日期').reset_index(drop=True)
    assert actual.shape == expected_shape
    expected = get_expected_data(code)
    expected = expected.query(f"交易日期>='{t1}'").query(
        f"交易日期<='{t2}'").reset_index(drop=True)
    assert_frame_equal(actual, expected)

# @pytest.mark.skip
@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('399005', '2019-11-21', '2019-11-21', (1, 9)),
    ('399001', '2019-11-21', '2019-11-21', (1, 9)),
])
def test_one_day(pageapi, code, t1, t2, expected_shape):
    """测试定义区间的数据"""
    actual = pageapi.get_data(code, t1, t2)
    assert actual.shape == expected_shape
    expected = get_expected_data(code)
    expected = expected.query(f"交易日期>='{t1}'").query(
        f"交易日期<='{t2}'").reset_index(drop=True)
    assert_frame_equal(actual, expected)