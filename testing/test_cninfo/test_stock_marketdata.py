"""测试股票市场行情数据
"""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cnswd.cninfo import MarketStockData
from cnswd.utils import data_root, kill_firefox

START_DATE = '2018-12-31'
END_DATE = '2019-11-24'


@pytest.fixture
def pageapi():
    # 确保干净的测试环境
    kill_firefox()
    with MarketStockData() as api:
        yield api


def get_expected_data(code):
    fp = data_root(f"{code}.csv", './testing/data/md/stock')
    na_values = ['-', '无', 'NaN']
    dtype = {'证券代码': str}
    try:
        # 无序号列
        df = pd.read_csv(fp, index_col=False, dtype=dtype, na_values=na_values)
        return df.sort_values('交易日期').query(f"交易日期<='{END_DATE}'")
    except Exception:
        return pd.DataFrame()


# @pytest.mark.skip
@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('000002', None, END_DATE, (7094, 8)),
    ('000001', None, END_DATE, (7083, 8)),
    ('000333', None, END_DATE, (1505, 8)),
])
def test_with_enddate(pageapi, code, t1, t2, expected_shape):
    """测试期间数据"""
    actual = pageapi.get_data(code, t1, t2)
    actual = actual.sort_values('交易日期').reset_index(drop=True)
    assert actual.shape == expected_shape
    expected = get_expected_data(code).reset_index(drop=True)
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('000002', START_DATE, END_DATE, (217, 8)),
    ('000001', START_DATE, END_DATE, (217, 8)),
    ('000333', START_DATE, END_DATE, (217, 8)),
])
def test_with_start_and_end(pageapi, code, t1, t2, expected_shape):
    """测试期间数据"""
    actual = pageapi.get_data(code, t1, t2)
    actual = actual.sort_values('交易日期').reset_index(drop=True)
    assert actual.shape == expected_shape
    expected = get_expected_data(code)
    expected = expected.query(f"交易日期>='{t1}'").reset_index(drop=True)
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize("code, t1, t2, expected_shape", [
    ('000001', '2019-11-21', '2019-11-21', (1, 8)),
    ('600000', '2019-11-21', '2019-11-21', (1, 8)),
])
def test_one_day(pageapi, code, t1, t2, expected_shape):
    """测试定义区间的数据"""
    actual = pageapi.get_data(code, t1, t2)
    assert actual.shape == expected_shape
    expected = get_expected_data(code)
    expected = expected.query(f"交易日期>='{t1}'").query(
        f"交易日期<='{t2}'").reset_index(drop=True)
    assert_frame_equal(actual, expected)
