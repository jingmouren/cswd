import pandas as pd
from collections import OrderedDict
from os import path

here = path.abspath(path.dirname(__file__))


def get_field_map(item, level, to_dict=True):
    """获取字段信息
    
    Arguments:
        item {str} -- 项目名称 db, ts
        level {str} -- 项目层级
    
    Keyword Arguments:
        to_dict {bool} -- 是否以字典形式输出 (default: {True})
    
    Returns:
        {dict or DataFrame} -- 项目字段信息
    """
    fp = path.join(here, 'api_doc', item, f"{level}.csv")
    df = pd.read_csv(fp, '\t')
    df.columns = df.columns.str.strip()
    if to_dict:
        return OrderedDict(
            {row['英文名称']: row['中文名称']
             for _, row in df.iterrows()})
    else:
        return df


def get_field_type(item, level):
    """获取列类型"""
    fp = path.join(here, 'api_doc', item, f"{level}.csv")
    df = pd.read_csv(fp, '\t', dtype={'类型': str})
    df.columns = df.columns.str.strip()
    d_cols, s_cols, i_cols, f_cols = [], [], [], []
    for _, row in df.iterrows():
        type_ = row['类型'][:3].lower()
        if type_ in ('dat', ):
            d_cols.append(row['中文名称'])
        elif type_ in ('var', 'char'):
            s_cols.append(row['中文名称'])
        elif type_ in ('big', 'int'):
            i_cols.append(row['中文名称'])
        elif type_ in ('dec', ):
            f_cols.append(row['中文名称'])
    return {
        'd_cols': d_cols,
        's_cols': s_cols,
        'i_cols': i_cols,
        'f_cols': f_cols,
    }
