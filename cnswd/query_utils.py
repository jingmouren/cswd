from enum import Enum, unique
import pandas as pd
from cnswd.utils import ensure_dt_localize
import warnings


@unique
class Ops(Enum):
    eq = 1  # ==
    gte = 2  # >=
    lse = 3  # <=
    gt = 4  # >
    ls = 5  # <


def _to_op_symbol(e):
    if e == Ops.eq:
        return '=='
    elif e == Ops.gte:
        return '>='
    elif e == Ops.lse:
        return '<='
    elif e == Ops.gt:
        return '>'
    elif e == Ops.ls:
        return '<'
    raise ValueError(f'不支持比较操作符号{e}')


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
    if not fp.exists():
        raise FileNotFoundError(f"找不到文件：{fp}")
    try:
        store = pd.HDFStore(fp, mode='r')
        df = store.select('data', stmt, auto_close=True)
    except KeyError:
        # 当h5文件不存在data节点时触发
        raise ValueError('数据内容为空，请刷新项目数据。')
    except Exception as e:
        warnings.warn(f"{e!r}")
        df = pd.DataFrame()
    finally:
        store.close()
    return df