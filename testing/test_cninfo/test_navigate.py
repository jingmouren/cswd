"""测试导航菜单

判断标准:
    如菜单属性`class`的值为`active`，意味着正确处理。
"""

import pytest
from bs4 import BeautifulSoup

from cnswd.cninfo import (FastSearch, MarketIndexData, MarketStockData,
                          ThematicStatistics)
from cnswd.cninfo.ops import _normalize_level_num, navigate


def get_tag_class(driver, level):
    num = [_normalize_level_num(x) for x in level.split('.')]
    data_pos = ''.join(num)
    soup = BeautifulSoup(driver.page_source, features="lxml")
    tag = soup.find("li", attrs={"data-pos": data_pos})
    # 属性返回列表?
    return tag['class'][0]


# 单变量，其值为列表
# 多变量以`,`分割，其值以元组表示
@pytest.mark.parametrize("class_", [
    FastSearch,
    ThematicStatistics,
    MarketStockData,
    MarketIndexData,
])
def test_navigate(class_):
    """测试菜单导航"""
    with class_() as api:
        for key in api.config.keys():
            navigate(api.driver, key)
            assert get_tag_class(api.driver, key) == 'active'
