import pytest
from cnswd.cninfo.databrowser import FastSearcher
from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal


def get_expected_data(level, code):
    item = '_'.join(level.split('.'))
    fp = Path(f"testing/data/fs/{item}/{code}.csv")
    na_values = ['-', '无', ';']
    dtype = {'股票代码': str, '证券代码': str}
    try:
        return pd.read_csv(fp, dtype=dtype, na_values=na_values)
    except Exception:
        return pd.DataFrame()


@pytest.fixture
def pageapi():
    with FastSearcher(False) as api:
        yield api


@pytest.mark.parametrize("level,code,start,end", [
    ('1', '000001', None, None),
    ('1', '300033', None, None),
])
def test_fetch_data(pageapi, level, code, start, end):
    expected = get_expected_data(level, code)
    actual = pageapi.get_data(level, code, start, end)
    assert_frame_equal(expected, actual)