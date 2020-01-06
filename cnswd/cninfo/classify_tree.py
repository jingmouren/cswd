import json
import os
import re
import time

import pandas as pd

from .._seleniumwire import make_headless_browser
from ..utils.log_utils import make_logger
from ..utils.pd_utils import _concat
from .ops import toggler_close, toggler_open, wait_page_loaded

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'
CLASS_ID = re.compile(r'platetype=(.*?)(&|$)')

PLATE_LEVELS = {
    1: '137004',
    2: '137002',
    3: '137003',
    4: '137006',
    5: '137007',
    6: '137001',
}
PLATE_MAPS = {
    '137001': '市场分类',
    '137002': '证监会行业分类',
    '137003': '国证行业分类',
    '137004': '申万行业分类',
    '137006': '地区分类',
    '137007': '指数分类',
}


class _LevelEncoder(object):
    """自然顺序层级编码器

    Notes：
    ------
        编码按照输入顺序，而非代码本身的顺序。
    """
    def __init__(self):
        self.infoes = {}
        self.level = {}

    def _on_changed(self, i, depth):
        # 子级别重置为1，当前级别+1，父级别不变
        for loc in range(i + 1, depth):
            self.level[loc] = 1
        old = self.level.get(i, 1)
        self.level[i] = old + 1

    def encode(self, code_tuple):
        """输入代码元组，输出自然顺序层级编码
        
        Arguments:
            code_tuple {tuple}} -- 编码元组。如(1,'S11','01','03')
        
        Returns:
            str -- 以`.`分隔的层级。如`2.3.4.1`
        """
        depth = len(code_tuple)
        for i in range(depth):
            current = code_tuple[i]
            if self.infoes.get(i, None) is None:
                self.infoes[i] = current
                # 初始赋值 nth.1...
                self.level[0] = code_tuple[0]
                for i in range(1, depth):
                    self.level[i] = 1
            else:
                if self.infoes[i] != current:
                    # 当出现不一致时，子级只记录，不再比较
                    for j in range(i, depth):
                        self.infoes[j] = code_tuple[j]
                    self._on_changed(i, depth)
                    break
        return '.'.join([str(x) for x in self.level.values()])


def _level_split(nth, code):
    """分解分类编码"""
    if nth == 1:
        return (nth, code[:3], code[3:5], code[5:7])
    elif nth == 2:
        if len(code) == 3:
            return (nth, code[0], code[1:])
        else:
            return (nth, code[:3], code[3:5], code[5:7])
    elif nth == 3:
        return (nth, code[:3], code[3:5], code[5:7], code[7:])
    elif nth == 4:
        return (nth, code[:2], code[2:])
    elif nth == 5:
        return (nth, code)
    elif nth == 6:
        return (nth, code)


class ClassifyTree(object):
    """分类树"""
    btned = False
    api_name = '分类树'
    api_e_name = 'dataBrowse'

    check_loaded_css = '.nav-second > div:nth-child(1) > h1:nth-child(1)'
    check_loaded_css_value = api_name

    def __init__(self, log_to_file=None):
        start = time.time()
        self.log_to_file = log_to_file
        self.driver = make_headless_browser()
        name = str(os.getpid()).zfill(6)
        self.logger = make_logger(name, log_to_file)
        self._load_page()
        self.driver.maximize_window()
        # 确保加载完成
        self.driver.implicitly_wait(0.2)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')

    def _load_page(self):
        # 如果重复加载同一网址，耗时约为1ms
        self.logger.info(self.api_name)
        url = HOME_URL_FMT.format(self.api_e_name)
        self.driver.get(url)
        self.driver.wait_for_request(self.driver.last_request.path)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def _btn2(self):
        """高级搜索"""
        css = '#btn2'
        self.driver.find_element_by_css_selector(css).click()

    def btn2(self):
        """更改为高级搜索"""
        if not self.btned:
            self._btn2()
            self.btned = True

    def _construct_css(self, level):
        nums = level.split('.')
        head = f'.classify-tree > li:nth-child({nums[0]})'
        if len(nums) == 1:
            return head
        rest = [
            'ul:nth-child(2) > li:nth-child({})'.format(x) for x in nums[1:]
        ]
        return ' > '.join([head] + rest)

    def _construct_path(self, a):
        """构造api地址"""
        api = a.get_attribute('data-api')
        param = a.get_attribute('data-param')
        return f"{api}?{param}"

    # @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.3))
    def _get_classify_tree(self, level, values):
        """获取层级股票列表"""
        df = pd.DataFrame()
        cum_level = []
        for l in level.split('.'):
            cum_level.append(l)
            css = self._construct_css('.'.join(cum_level))
            li = self.driver.find_element_by_css_selector(css)
            toggler_open(li)
            # self.driver.save_screenshot(f"{'.'.join(cum_level)}.png")
            if li.get_attribute('class') == 'tree-empty':
                # 点击加载分类项下股票代码
                a = li.find_element_by_tag_name('a')
                a.click()
                path = self._construct_path(a)
                # Wait for the request/response to complete
                request = self.driver.wait_for_request(path)
                response = request.response

                data = json.loads(response.body)
                num = data['total']
                self.logger.info(f'分类树层级：{level} 行数:{num}')
                if num >= 1:
                    records = data['records']
                    df['证券代码'] = [x['SECCODE'] for x in records]
                    df['证券简称'] = [x['SECNAME'] for x in records]

                    df['分类层级'] = level
                    df['分类编码'] = values[0]
                    df['分类名称'] = values[1]
                    df['平台类别'] = values[2]
        # # 关闭根树
        css = self._construct_css(level.split('.')[0])
        li = self.driver.find_element_by_css_selector(css)
        toggler_close(li)
        return df

    @property
    def classify_bom(self):
        """股票分类BOM表"""
        self.btn2()
        roots = self.driver.find_elements_by_css_selector(
            '.classify-tree > li')
        items = []
        for r in roots:
            # 需要全部级别的分类编码名称
            items.extend(r.find_elements_by_tag_name('span'))
            items.extend(r.find_elements_by_tag_name('a'))
        data = []
        attrs = ('data-id', 'data-name')
        for item in items:
            data.append([item.get_attribute(name) for name in attrs])
        df = pd.DataFrame.from_records(data, columns=['分类编码', '分类名称'])
        return df.dropna().drop_duplicates(['分类编码', '分类名称'])

    def get_tree_attribute(self, nth):
        """获取分类树属性"""
        self.btn2()
        res = {}
        encoder = _LevelEncoder()
        valid_plate = PLATE_LEVELS[nth]
        tree_css = '.classify-tree > li:nth-child({})'.format(nth)
        li = self.driver.find_element_by_css_selector(tree_css)
        trees = li.find_elements_by_xpath(
            './/li[@class="tree-empty"]//descendant::a')
        for tree in trees:
            name = tree.get_attribute('data-name')
            id_ = tree.get_attribute('data-id')
            param = tree.get_attribute('data-param')
            plate = re.search(CLASS_ID, param).group(1)
            code_tuple = _level_split(nth, id_)
            level = encoder.encode(code_tuple)
            if plate == valid_plate:
                res[level] = (id_, name, plate)
        # # 注意:证监会 综合分类混乱。编码 2.19 开头
        return res

    def get_classify_tree(self, n):
        """获取分类树层级下的股票列表"""
        self.btn2()
        self.driver.implicitly_wait(0.2)
        levels = self.get_tree_attribute(n)
        status = {}
        res = []
        for level, values in levels.items():
            try:
                df = self._get_classify_tree(level, values)
                res.append(df)
                status[level] = True
            except Exception:
                status[level] = False
        for k in status.keys():
            if not status[k]:
                print(f'层级{k}失败')
        df = _concat(res)
        return df
