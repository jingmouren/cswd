import pandas as pd

from .cninfo.classify_tree import PLATE_LEVELS, PLATE_MAPS
from .data import HDFData
from .query_utils import Ops, query, query_stmt
from .scripts.fs import refresh_batch as fs_batch_refresh
from .scripts.refresh import (ASRefresher, ClassifyBomRefresher,
                              ClassifyTreeRefresher, DisclosureRefresher,
                              FSRefresher, MarginDataRefresher,
                              SinaNewsRefresher, TreasuryRefresher,
                              WYIRefresher, WYSRefresher)
from .setting.config import DB_CONFIG
from .utils import data_root, sanitize_dates


def _minutely_history(fp):
    df = pd.read_pickle(fp)
    dt = fp.name.split('.')[0]
    dt = pd.Timestamp(int(dt), unit='s')
    df['时间'] = dt
    df.reset_index(inplace=True)
    return df


# region 交易数据
def minutely_history(code=None, start=None, end=None):
    """分钟级别成交数据
    
    Arguments:
        code {str} -- 股票代码。默认`None`代表全部股票
        start {datetime_like}} -- 开始时间
        end {datetime_like} -- 结束时间
    
    Returns:
        DataFrame -- 成交数据

    Usage:
    >>> code = '000333'
    >>> start = '2020-01-07 09:30'
    >>> end = '2020-01-07 15:00'
    >>> df = minutely_history(code, start, end)
    >>> df[['时间','代码','今开','最高','最低','最新价']].tail()

        时间	代码	今开	最高	最低	最新价
    2748	2020-01-07 14:56:00	000333	57.400002	58.200001	57.200001	58.150002
    2748	2020-01-07 14:57:00	000333	57.400002	58.200001	57.200001	58.150002
    2748	2020-01-07 14:58:00	000333	57.400002	58.200001	57.200001	58.150002
    2748	2020-01-07 14:59:00	000333	57.400002	58.200001	57.200001	58.150002
    2748	2020-01-07 15:00:00	000333	57.400002	58.200001	57.200001	58.150002
    """
    start, end = sanitize_dates(start, end)
    if start == end:
        # 如果查询一天的数据，需要将日期更改为
        end = end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
    dates = pd.date_range(start, end)
    dfs = []
    for d in dates:
        dp_dir = data_root(f"TCT/{d.strftime(r'%Y%m%d')}")
        fps = dp_dir.glob('*.pkl')
        for fp in fps:
            dt = fp.name.split('.')[0]
            dt = pd.Timestamp(int(dt), unit='s')
            if start <= dt <= end:
                df = _minutely_history(fp)
                dfs.append(df)
    ret = pd.concat(dfs)
    cond = (ret['时间'] >= start) & (ret['时间'] <= end)
    ret = ret[cond]
    ret['代码'] = ret['代码'].map(lambda x: x[2:])
    if code:
        return ret[ret['代码'] == code]
    else:
        return ret


def daily_history(code, start, end, is_index=False):
    """网易日线行情（股票/指数）
    
    Arguments:
        code {str} -- 代码 
        start {date_like} -- 开始时间
        end {date_like} -- 结束时间
        is_index {bool} -- 是否为指数，默认为股票。
    Returns:
        DataFrame -- 日线数据框
    """
    start, end = sanitize_dates(start, end)
    if not is_index:
        r = WYSRefresher()
    else:
        r = WYIRefresher()
    fp = r.get_data_path(code)
    if not fp.exists():
        # 使用当前交易股票刷新时，如股票长期停牌，历史数据没有保存在本地
        batch = [code]
        r.refresh_batch(batch)
    date_col = r.get_index_col(code)
    args = [
        # 原始数据中股票代码有前缀`'`
        ('股票代码', Ops.eq, f"'{code}"),
        (date_col, Ops.gte, start),
        (date_col, Ops.lse, end),
    ]
    stmt = query_stmt(*args)
    try:
        df = query(fp, stmt)
        # 原始代码表达为 "'600710" -> "600710"
        df['股票代码'] = df['股票代码'].map(lambda x: x[1:])
        return df
    except KeyError:
        # 新股尚未有历史成交数据
        return pd.DataFrame()


def margin(code, start, end):
    """深证信期间融资融券数据"""
    start, end = sanitize_dates(start, end)
    r = MarginDataRefresher()
    fp = r.get_data_path(None)
    stmt = query_stmt(*[
        ('股票代码', Ops.eq, code),
        ('交易日期', Ops.gte, start),
        ('交易日期', Ops.lse, end),
    ])
    return query(fp, stmt)


def quotes(date):
    """股票实时报价
    
    Arguments:
        date {date_like} -- 日期

    Returns:
        DataFrame -- 日线数据框
    """
    date = pd.Timestamp(date)
    fp = data_root(f"live_quotes/{date.strftime(r'%Y%m%d')}.h5")
    h = HDFData(fp, 'a')
    return h.data


def cjmx(code, date):
    """股票成交明细
    
    Arguments:
        code {str} -- 代码 
        date {date_like} -- 日期

    Returns:
        DataFrame -- 成交明细数据框
    """
    date = pd.Timestamp(date)
    fp = data_root(f"wy_cjmx/{code}/{date.strftime(r'%Y%m%d')}.h5")
    h = HDFData(fp, 'a')
    return h.data


# endregion


# region 信息
def news(cate, start, end):
    """期间新浪财经消息
    
    Arguments:
        cate {str} -- 类别，如None代表全部
        start {date_like} -- 开始时间
        end {date_like} -- 结束时间
    
    Returns:
        DataFrame -- 消息数据
    """
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    args = [
        ('分类', Ops.eq, cate),
        ('时间', Ops.gte, start),
        ('时间', Ops.lse, end),
    ]
    stmt = query_stmt(*args)
    r = SinaNewsRefresher()
    fp = r.get_data_path(None)
    return query(fp, stmt)


def disclosure(code, start, end):
    """公司公告"""
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    r = DisclosureRefresher()
    fp = r.get_data_path(None)
    args = [
        ('股票代码', Ops.eq, code),
        ('公告时间', Ops.gte, start),
        ('公告时间', Ops.lse, end),
    ]
    stmt = query_stmt(*args)
    return query(fp, stmt)


def ths_gn():
    """同花顺股票概念"""
    fp = data_root('THS/thsgns.pkl')
    return pd.read_pickle(fp)


def ths_gn_time():
    """同花顺股票概念简介"""
    fp = data_root('THS/gn_time.pkl')
    return pd.read_pickle(fp)


def tct_gn():
    """腾讯股票概念"""
    fp = data_root('TCT/gn.h5')
    return pd.read_hdf(fp, 'data')


def classify_bom():
    """分类BOM"""
    r = ClassifyBomRefresher()
    return r.get_table_data(None)


def classify_tree(plate=None, code=None):
    """股票分类树或股票行业映射"""
    r = ClassifyTreeRefresher()
    if plate is None:
        df = r.get_table_data()
        return df
    # 申万、国证、证监会、地区分类
    valid_plate = [
        v[:3] if k == '137002' else v[:2] for k, v in PLATE_MAPS.items()
    ][:4]
    assert plate in valid_plate, f"仅支持'{valid_plate}'分类查询"
    plate_code = [k for k, v in PLATE_MAPS.items() if plate in v][0]
    one = [k for k, v in PLATE_LEVELS.items() if plate_code == v][0]
    fp = r.get_data_path(one)
    args = [
        ('平台类别', Ops.eq, plate_code),
        ('股票代码', Ops.eq, code),
    ]
    stmt = query_stmt(*args)
    df = query(fp, stmt)
    if code:
        return {code: df['分类名称'].values[0]}
    else:
        part = df[['股票代码', '分类名称']]
        return part.set_index('股票代码').to_dict()['分类名称']


def stock_list():
    """股票列表"""
    r = ASRefresher()
    df = r.get_table_data('1')
    return df


def calendar():
    """交易日历
    
    Returns:
        array -- 交易日历 datetime64[ns]
    """
    fp = data_root('trading_calendar.h5')
    h = HDFData(fp, 'a')
    return h.data['trading_date'].values


def treasury(start, end):
    """国库券期间利率"""
    start, end = sanitize_dates(start, end)
    r = TreasuryRefresher()
    df = r.get_table_data()
    cond = df.index.to_series().between(start, end)
    df = df[cond]
    return df


# endregion


# region 深证信高级搜索
def asr_data(level, code=None, start=None, end=None):
    """深证信高级搜索项目数据
    
    Arguments:
        level {str} -- 项目层级
        code {str} -- 股票代码
    
    Keyword Arguments:
        start {date-like} -- 开始日期 (default: {None})
        end {date-like} -- 结束日期 (default: {None})
    
    Returns:
        DataFrame -- 项目数据
    """
    assert level in DB_CONFIG.keys(), f"有效项目层级为{list(DB_CONFIG.keys())}"
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    r = ASRefresher()
    fp = r.get_data_path(level)
    code_str = '股票代码'
    date_str = DB_CONFIG[level]['date_field'][0]
    args = [
        (code_str, Ops.eq, code),
        (date_str, Ops.gte, start),
        (date_str, Ops.lse, end),
    ]
    stmt = query_stmt(*args)
    df = query(fp, stmt)
    if not df.empty:
        # 少数项目股票名称解析错误，在此修正
        df[code_str] = df[code_str].map(lambda x: str(int(x)).zfill(6))
    return df


# endregion
