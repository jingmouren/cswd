"""
腾讯概念股票列表(覆盖式更新)
"""
from cnswd.utils import data_root, make_logger
from cnswd.websource.tencent import fetch_concept_stocks
import pandas as pd


logger = make_logger('腾讯概念')


def refresh():
    """采用覆盖式更新腾讯股票概念列表"""
    df = fetch_concept_stocks()
    fp = data_root('TCT/gn.h5')
    df.rename(columns={'item_id': '概念id',
                       'item_name': '概念简称', 'code': '股票代码'}, inplace=True)
    df.to_hdf(fp, 'data', format='table')
    logger.notice(f"写入{len(df)}行")
