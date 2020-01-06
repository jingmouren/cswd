"""
命令行

用法
# 查看使用方法
$ stock --help
# 刷新<深证信股指日线>数据
$ stock szxs

使用后台任务计划，自动刷新数据

"""
from __future__ import absolute_import, division, print_function

import asyncio

import click
import pandas as pd

from ..setting.config import DB_CONFIG
from ..utils import kill_firefox, remove_temp_files
from .fs import fs_refresh_all
from .quote import refresh_live_quote
from .refresh import (ASRefresher, ClassifyBomRefresher, ClassifyTreeRefresher,
                      DisclosureRefresher, MarginDataRefresher,
                      SinaNewsRefresher, TreasuryRefresher, WYIRefresher,
                      WYSRefresher)
from .tct_gn import refresh as tct_gn_refresh
from .ths_gn import update_gn_list, update_gn_time
from .trading_calendar import refresh_trading_calendar
from .wy_cjmx import refresh_wy_cjmx
from .yahoo import refresh_all as refresh_yahoo_data


@click.group()
def stock():
    """
    刷新股票数据\n
    \t1. 基本维护\n
    \t2. 基础信息\n
    \t3. 交易数据\n
    """
    pass


# region 新浪
@stock.command()
@click.option('--times', default=3, help='刷新日期')
def news(times):
    """新浪财经消息"""
    r = SinaNewsRefresher()
    r.refresh_all(times)


@stock.command()
def quote():
    """股票实时报价"""
    asyncio.run(refresh_live_quote())


# endregion


# region 巨潮
@stock.command()
def disclosure():
    """刷新公司公告"""
    r = DisclosureRefresher()
    asyncio.run(r.refresh_all())


@stock.command()
def classify():
    """数据浏览器股票分类树及BOM表"""
    r = ClassifyBomRefresher()
    r.refresh_all()
    r = ClassifyTreeRefresher(6)
    r.refresh_all()


@stock.command()
def margin():
    """刷新<深证信>融资融券"""
    r = MarginDataRefresher()
    r.refresh_all()


@stock.command()
def fsr():
    """数据浏览器快速搜索（按代码循环刷新）"""
    fs_refresh_all()


@stock.command()
@click.option(
    '--item',
    required=False,
    default=None,
    type=click.Choice(list(DB_CONFIG.keys())),
    help='刷新股票项目数据。如果项目未指定，刷新全部项目',
)
def asr(item):
    """数据浏览器高级搜索（刷新全部代码数据）"""
    r = ASRefresher()
    if item is None:
        batch = list(DB_CONFIG.keys())
    else:
        batch = [item]
    r.refresh_batch(batch)
    # r.refresh_all(item)


# endregion

# region 网易


@stock.command()
def wys():
    """刷新<网易股票日线>数据"""
    r = WYSRefresher()
    r.refresh_all()


@stock.command()
def wyi():
    """刷新<网易股指日线>数据"""
    r = WYIRefresher()
    r.refresh_all()


@stock.command()
def calendar():
    """交易日历"""
    refresh_trading_calendar()


@stock.command()
@click.option('--date', default=None, help='刷新日期')
def cjmx(date):
    """刷新近期成交明细"""
    if date is None:
        date = pd.Timestamp('today').normalize()
    # 多次刷新。可能存在漏网之鱼
    for i in range(1, 4):
        print(f'第{i}次尝试')
        refresh_wy_cjmx(date)


# endregion


# region 同花顺
@stock.command()
def thsgn():
    """刷新同花顺概念股票列表"""
    update_gn_list()


@stock.command()
def gntime():
    """刷新同花顺概念概述"""
    update_gn_time()


# endregion

# region yahoo


@stock.command()
def yahoo():
    """刷新雅虎数据"""
    refresh_yahoo_data()


# endregion


# region 腾讯
@stock.command()
def tctgn():
    """刷新腾讯概念股票列表"""
    tct_gn_refresh()


# endregion


# region 其他网站
@stock.command()
def treasury():
    """刷新国库券利率数据"""
    r = TreasuryRefresher()
    r.refresh_all()


# endregion


# region 其他辅助命令
@stock.command()
def clean():
    """每天清理可能残余的firefox，注意避免与日常任务时间重叠
    如每日凌晨在没有后台抓取数据任务时，执行此任务
    """
    remove_temp_files()
    kill_firefox()


# endregion
