from enum import Enum, unique
import pandas as pd
from cnswd.utils import ensure_dt_localize


@unique
class Ops(Enum):
    eq = 1  # ==
    gte = 2  # >=
    lse = 3  # <=


def _to_op_symbol(e):
    if e == Ops.eq:
        return '=='
    elif e == Ops.gte:
        return '>='
    else:
        return '<='


def force_freq_to_none(v):
    # 查询时间不得带tz,freq信息
    if isinstance(v, pd.Timestamp):
        if v.tz is not None or v.freq is not None:
            v = ensure_dt_localize(v)
            return pd.Timestamp(v.to_pydatetime()).tz_localize(None)
    return v


def query_stmt(*args):
    """生成查询表达式
    
    Notes:
    ------
        如值表为`None`代表全部
    """
    stmt = []
    for arg in args:
        assert len(arg) == 3, '子查询必须是（列名称、比较符、限定值）三元组'
        key, e, value = arg
        value = force_freq_to_none(value)
        if key is None or value is None or pd.isnull(value):
            continue
        stmt.append(f"{key} {_to_op_symbol(e)} {value!r}")
    return stmt


def query(fp, stmt):
    try:
        store = pd.HDFStore(fp, mode='r')
        df = store.select('data', stmt, auto_close=True)
        return df
    except OSError as e:
        raise FileNotFoundError(f"{e!r}")
    finally:
        store.close()