"""

针对提取的数据进行测试验证

前提条件：必须提取网络数据到本地
"""

import pandas as pd
import pytest
import numpy as np
from cnswd.reader import (cjmx, classify_tree, news, stock_list, calendar,
                          daily_history, treasury)
from cnswd.scripts.wy_cjmx import _wy_fix_data
from cnswd.websource.wy import fetch_cjmx
from pandas.util.testing import assert_almost_equal
from numpy.testing import assert_array_equal


def test_read_news():
    """测试读取财经新闻"""
    start = pd.Timestamp('2019-12-18 14:35:00')
    end = pd.Timestamp('2019-12-18 15:00:00')
    df = news(None, start, end)
    assert len(df) == 26  # 26 行


def test_read_treasury():
    """"测试读取国库券利率"""
    start = None
    end = pd.Timestamp('2019-12-18 15:00:00')
    df = treasury(start, end)
    assert df.shape == (4216, 16)


def test_read_classify_tree():
    """测试读取股票分类树"""
    df = classify_tree()
    assert df[df['分类名称'] == '国证2000'].shape == (2000, 7)
    assert df[df['分类名称'] == '沪市A'].shape[0] > 1492
    # 当前数据应该有9行
    assert df[df['分类层级'] == '3.1.1.1.1'].shape[0] > 7


def test_read_stock_list():
    """测试读取股票列表"""
    df = stock_list()
    assert len(df) > 3800
    assert df.shape[1] == 41


def test_szx_daily_history():
    """测试读取深证信股票日线行情"""
    df = daily_history('000001', '2019-12-01', '2019-12-19')
    assert len(df['日期'].unique()) == 14
    assert len(df) == 14
    assert df.shape == (14, 16)


def test_trading_dates():
    """测试交易日历"""
    start = pd.Timestamp('2000-01-01')
    end = pd.Timestamp('2019-12-23')
    tds = calendar()
    actual = [x for x in tds if x <= end and x >= start]
    actual = np.array(actual)
    expected = daily_history('399001', start, end, True)['日期'].values
    assert_array_equal(actual, expected)


@pytest.mark.skip
@pytest.mark.parametrize("code, date", [
    ('300635', '2019-12-16'),
    ('600390', '2019-12-16'),
])
def test_cjmx(code, date):
    """测试读取成交明细

    只能提取最近一周的成交明细
    """
    actual = cjmx(code, date)
    expected = fetch_cjmx(code, date)
    expected = _wy_fix_data(expected)
    assert_almost_equal(actual, expected, check_less_precise=2)
