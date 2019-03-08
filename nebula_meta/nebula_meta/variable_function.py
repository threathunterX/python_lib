#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
变量的统计
"""

import json
from collections import OrderedDict
from .util import text, unicode_dict


class Function(object):

    def __init__(self, method, source, object, object_type, object_subtype, param=None, config=None):
        self._method = text(method or '')
        self._source = text(source or '')
        self._object = text(object or '')
        self._object_type = text(object_type or '')
        self._object_subtype = text(object_subtype or '')
        self._param = text(param if param is not None else '')
        self._config = unicode_dict(config or dict())

    def sanity_check(self):
        """
        进行整体的check
        :return:
        """

        if self.is_empty():
            if self._source or self._object or self._object_type or self.object_subtype or self._param or self._method:
                raise RuntimeError('空的Function不应该有多余的配置')
        else:
            if not self.source:
                raise RuntimeError('Function参数错误')

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, method):
        self._method = text(method or '')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = text(source or '')

    @property
    def object(self):
        return self._object

    @object.setter
    def object(self, object):
        self._object = text(object or '')

    @property
    def object_type(self):
        return self._object_type

    @object_type.setter
    def object_type(self, object_type):
        self._object_type = text(object_type or '')

    @property
    def object_subtype(self):
        return self._object_subtype

    @object_subtype.setter
    def object_subtype(self, object_subtype):
        self._object_subtype = text(object_subtype or '')

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = text(param if param is not None else '')

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = unicode_dict(config or dict())

    def fix_source(self, default_source):
        """
        source 没有指定时设置默认的source
        :param default_source: 默认的source
        :return:
        """
        if not self.source:
            self.source = default_source

    def check_type(self, source_fields_type_dict):
        """
        对filter里用到的object字段推导数据类型
        :param source_fields_type_dict: 来源变量为key，value字段类型映射的map
        :return:
        """

        if self.is_empty():
            return

        if not self.source:
            return

        if self.object == '' and self.method == 'count':
            # count可以没有操作数
            self.object_type = self.object_subtype = ''
            return

        if self.method == 'setblacklist':
            # special logic for vargen
            return

        fields_mapping = source_fields_type_dict.get(self.source)
        if not fields_mapping:
            raise RuntimeError('变量{}缺乏数据定义'.format(self.source))

        if self.object:
            field_type = fields_mapping.get(self.object)
            if not field_type:
                raise RuntimeError('变量{}找不到字段{}的定义'.format(self.source, self.object))
            self.object_type, self.object_subtype = field_type

    def is_empty(self):
        # 可能为空, 表示其实不需要配置
        return self.method == ''

    def get_dict(self):
        if self.is_empty():
            return dict()

        result = dict()
        result['method'] = self.method
        result['source'] = self.source
        result['object'] = self.object
        result['object_type'] = self.object_type
        result['object_subtype'] = self.object_subtype
        result['param'] = self.param
        result['config'] = self.config
        return result

    def get_ordered_dict(self):
        if self.is_empty():
            return OrderedDict()

        kv = list()
        kv.append(('method', self.method))
        kv.append(('source', self.source))
        kv.append(('object', self.object))
        kv.append(('object_type', self.object_type))
        kv.append(('object_subtype', self.object_subtype))
        kv.append(('param', self.param))
        kv.append(('config', self.config))
        return OrderedDict(kv)

    def get_simplified_ordered_dict(self, can_ignore_source):
        """
        返回简略模式，可推导的数据不写
        :param can_ignore_source: 可以被省略的source，表示可以被推导
        :return:
        """

        if self.is_empty():
            return OrderedDict()

        kv = list()
        kv.append(('method', self.method))
        if self.source and self.source != can_ignore_source:
            kv.append(('source', self.source))
        kv.append(('object', self.object))
        if self.param:
            kv.append(('param', self.param))
        if self.config:
            kv.append(('config', self.config))
        return OrderedDict(kv)

    def get_json(self):
        return json.dumps(self.get_ordered_dict())

    @staticmethod
    def from_dict(d):
        return Function(d.get('method'), d.get('source'), d.get('object'), d.get('object_type'),
                        d.get('object_subtype'), d.get('param'), d.get('config'))

    @staticmethod
    def from_json(json_str):
        return Function.from_dict(json.loads(json_str))

    def copy(self):
        return Function.from_dict(self.get_dict())

    def __str__(self):
        return "Function[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


# 简单运算表达式, 对当前值进行计算变形
SELF_CALCULATOR_MAP = {
    ('double', ''): {
        'long': ('long', ''),
        'str': ('string', ''),
    },
    ('string', ''): {
        'long': ('long', ''),
        'double': ('double', '')
    },
    ('long', ''): {
        'double': ('double', ''),
        'str': ('string', ''),
    },
    ('bool', ''): {
        'str': ('string', ''),
    },
    ('list', 'long'): {
        'min': ('long', ''),
        'max': ('long', ''),
        'sum': ('long', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'count': ('long', ''),
        'first': ('long', ''),
        'last': ('long', ''),
        'distinct': ('long', ''),
        'distinct_count': ('long', ''),
        'reverse': ('list', 'long'),
    },
    ('list', 'double'): {
        'min': ('double', ''),
        'max': ('double', ''),
        'sum': ('double', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'count': ('long', ''),
        'first': ('double', ''),
        'last': ('double', ''),
        'distinct': ('double', ''),
        'distinct_count': ('long', ''),
        'reverse': ('list', 'double'),
    },
    ('list', 'string'): {
        'count': ('long', ''),
        'first': ('string', ''),
        'last': ('string', ''),
        'distinct': ('list', 'string'),
        'reverse': ('list', 'string'),
        'distinct_count': ('long', ''),
    },
    ('map', 'long'): {
        'count': ('long', ''),
        'top': ('list', 'kv')
    },
    ('map', 'double'): {
        'count': ('long', ''),
        'top': ('list', 'kv')
    },
    ('map', 'string'): {
        'count': ('long', '')
    },
}


# 基础窗口统计表达式，对最近一段时间或者永久性的窗口进行统计
SIMPLE_CALCULATOR_MAP = {
    ('long', ''): {
        'max': ('long', ''),
        'min': ('long', ''),
        'sum': ('long', ''),
        'count': ('long', ''),
        'distinct_count': ('long', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'first': ('long', ''),
        'last': ('long', ''),
        'top': ('list', 'kv'),
        'distinct': ('list', 'long'),
        'lastn': ('list', 'long'),
        'global_latest': ('long', ''),
        'group_sum': ('map', 'long'),
    },
    ('double', ''): {
        'max': ('double', ''),
        'min': ('double', ''),
        'sum': ('double', ''),
        'count': ('long', ''),
        'distinct_count': ('long', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'first': ('double', ''),
        'last': ('double', ''),
        'top': ('list', 'kv'),
        'key': ('map', 'double'),
        'distinct': ('list', 'double'),
        'lastn': ('list', 'double'),
        'global_latest': ('double', ''),
        'group_sum': ('map', 'double'),
    },
    ('string', ''): {
        'count': ('long', ''),
        'distinct_count': ('long', ''),
        'first': ('string', ''),
        'last': ('string', ''),
        'distinct': ('list', 'string'),
        'lastn': ('list', 'string'),
        'group_count': ('map', 'long'),
        'global_latest': ('string', ''),
    },
    ('bool', ''): {
        'count': ('long', ''),
        'distinct_count': ('long', ''),
        'first': ('bool', ''),
        'last': ('bool', ''),
        'global_latest': ('bool', ''),
    },
    ('map', 'long'): {
        'merge': ('map', 'long'),
        'merge_value': ('long', ''),
        'last_value': ('long', ''),
        'global_latest': ('map', 'long'),
    },
    ('map', 'double'): {
        'merge': ('map', 'double'),
        'merge_value': ('double', ''),
        'last_value': ('double', ''),
        'global_latest': ('map', 'double'),
    },
    ('map', 'string'): {
        'merge': ('map', 'string'),
        'merge_value': ('string', ''),
        'last_value': ('string', ''),
        'global_latest': ('map', 'string'),
    },
    ('mmap', 'long'): {
        'merge': ('mmap', 'long'),
        'merge_value': ('map', 'long'),
        'global_latest': ('mmap', 'long'),
    },
    ('mmap', 'double'): {
        'merge': ('mmap', 'double'),
        'merge_value': ('map', 'double'),
        'global_latest': ('mmap', 'double'),
    },
    ('mmap', 'string'): {
        'merge': ('mmap', 'string'),
        'merge_value': ('map', 'string'),
        'global_latest': ('mmap', 'string'),
    },
    ('list', 'long'): {
        'collection': ('list', 'long'),
        'distinct': ('list', 'long'),
        'global_latest': ('list', 'long'),
    },
    ('list', 'double'): {
        'collection': ('list', 'double'),
        'distinct': ('list', 'double'),
        'global_latest': ('list', 'double'),
    },
    ('list', 'string'): {
        'collection': ('list', 'string'),
        'distinct': ('list', 'string'),
        'global_latest': ('list', 'string'),
    },
    ('mlist', 'long'): {
        'collection': ('mlist', 'long'),
        'distinct': ('mlist', 'long'),
        'merge_value': ('list', 'long'),
        'global_latest': ('mlist', 'long'),
    },
    ('mlist', 'double'): {
        'collection': ('mlist', 'double'),
        'distinct': ('mlist', 'double'),
        'merge_value': ('list', 'double'),
        'global_latest': ('mlist', 'double'),
    },
    ('mlist', 'string'): {
        'collection': ('mlist', 'string'),
        'distinct': ('mlist', 'string'),
        'merge_value': ('list', 'string'),
        'global_latest': ('mlist', 'string'),
    },
}


# 周期性的窗口统计
PERIODICALLY_WINDOW_CALCULATOR_MAP = {
    ('long', ''): {
        'max': ('map', 'long'),
        'min': ('map', 'long'),
        'distinct_count': ('map', 'long'),
        'stats': ('mlist', 'double'),
        'first': ('map', 'long'),
        'last': ('map', 'long'),
        'top': ('mlist', 'long'),
        'distinct': ('mlist', 'long'),
        'collection': ('mlist', 'long'),
        'lastn': ('mlist', 'long'),
        'sum': ('map', 'long'),
        'count': ('map', 'long'),
        'group_sum': ('map', 'long'),
    },
    ('double', ''): {
        'max': ('map', 'double'),
        'min': ('map', 'double'),
        'distinct_count': ('map', 'long'),
        'stats': ('mlist', 'double'),
        'first': ('map', 'double'),
        'last': ('map', 'double'),
        'top': ('mlist', 'double'),
        'distinct': ('mlist', 'double'),
        'collection': ('mlist', 'double'),
        'lastn': ('mlist', 'double'),
        'sum': ('map', 'double'),
        'count': ('map', 'long'),
        'group_sum': ('map', 'double'),
    },
    ('string', ''): {
        'distinct_count': ('map', 'long'),
        'first': ('map', 'string'),
        'last': ('map', 'string'),
        'distinct': ('mlist', 'string'),
        'collection': ('mlist', 'string'),
        'lastn': ('mlist', 'string'),
        'group_count': ('mmap', 'long'),
        'count': ('map', 'long'),
    },
    ('bool', ''): {
        'first': ('map', 'bool'),
        'last': ('map', 'bool'),
    },
    ('map', 'long'): {
        'top': ('mmap', 'long'),
        'merge': ('map', 'long')
    },
    ('map', 'double'): {
        'top': ('mmap', 'double'),
        'merge': ('map', 'double')
    },
    ('map', 'string'): {
        'top': ('mmap', 'string'),
        'merge': ('map', 'string')
    },
    ('mmap', 'long'): {
        'top': ('mmap', 'long'),
        'merge': ('mmap', 'long')
    },
    ('mmap', 'double'): {
        'top': ('mmap', 'double'),
        'merge': ('mmap', 'double')
    },
    ('mmap', 'string'): {
        'top': ('mmap', 'string'),
        'merge': ('mmap', 'string')
    },
}


# 复杂窗口统计。周期性的窗口统计值作为来源数据，在一定时间范围内进行二次聚合
COMPLEX_WINDOW_CALCULATOR_MAP = {
    ('map', 'long'): {
        'max': ('long', ''),
        'min': ('long', ''),
        'first': ('long', ''),
        'last': ('long', ''),
        'distinct_count': ('long', ''),
        'merge': ('map', 'long'),
        'sum': ('long', ''),
        'count': ('long', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'global_sum': ('long', ''),
    },
    ('map', 'double'): {
        'max': ('double', ''),
        'min': ('double', ''),
        'first': ('double', ''),
        'last': ('double', ''),
        'merge': ('map', 'double'),
        'sum': ('double', ''),
        'count': ('long', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'global_sum': ('double', ''),
    },
    ('map', 'string'): {
        'first': ('string', ''),
        'last': ('string', ''),
        'merge': ('map', 'string'),
        'count': ('long', ''),
    },
    ('map', 'bool'): {
        'first': ('bool', ''),
        'last': ('bool', ''),
    },
    ('mlist', 'double'): {
        'sum': ('double', ''),
        'avg': ('double', ''),
        'stddev': ('double', ''),
        'count': ('long', ''),
        'distinct_count': ('long', ''),
        'distinct': ('list', 'double'),
        'collection': ('list', 'double'),
        'lastn': ('list', 'double'),
        'merge': ('mlist', 'double'),
    },
    ('mlist', 'long'): {
        'distinct_count': ('long', ''),
        'topn': ('list', 'long'),
        'distinct': ('list', 'long'),
        'collection': ('list', 'long'),
        'lastn': ('list', 'long'),
        'merge': ('mlist', 'long'),
    },
    ('mlist', 'kv'): {
        'top': ('list', 'kv')
    },
    ('mlist', 'string'): {
        'distinct_count': ('long', ''),
        'distinct': ('list', 'string'),
        'collection': ('list', 'string'),
        'lastn': ('list', 'string'),
        'merge': ('mlist', 'string'),
    },
    ('mmap', 'long'): {
        'group_count': ('map', 'long'),
        'merge': ('mmap', 'long'),
        'global_map_merge_topn': ('map', 'long'),
    },
    ('mmap', 'string'): {
        'merge': ('mmap', 'string'),
    },
    ('mmap', 'double'): {
        'merge': ('mmap', 'double'),
        'global_map_merge_topn': ('map', 'double'),
    },
}


# 二元统计
BINARY_CALCULATOR_MAP = {
    ('long', ''): {
        '+': ('long', ''),
        '-': ('long', ''),
        '*': ('long', ''),
        '/': ('double', ''),
    },
    ('double', ''): {
        '+': ('double', ''),
        '-': ('double', ''),
        '*': ('double', ''),
        '/': ('double', ''),
    },
}


# 零窗口
SELF_PERIOD_TYPES = {'self'}
# 简单窗口
SIMPLE_PERIOD_TYPES = {'last_n_seconds', 'ever', 'recently', 'fromslot'}
# 基本窗口
PERIODICALLY_PERIOD_TYPES = {'hourly'}
# 复杂窗口
COMPLEX_PERIOD_TYPES = {'last_n_hours', 'last_n_days', 'last_hour', 'last_day', 'today', 'yesterday', 'scope'}


ALL_PERIOD_TYPES = set(list(SELF_PERIOD_TYPES) + list(SIMPLE_PERIOD_TYPES) + list(PERIODICALLY_PERIOD_TYPES) +
                       list(COMPLEX_PERIOD_TYPES))


def get_supported_period_types():
    return ALL_PERIOD_TYPES


def get_calculator_result_type(period_type, object_type, operation):
    """
    获取统计方法值的数据类型
    在每种period_type下，操作数object的类型object_type和操作operation就能决定结果值的数据类型

    :param period_type: 统计周期的类型，包括self, hourly等
    :param object_type: 被统计字段的类型, (type, subtype)
    :param operation: 具体统计的方法
    :return: 统计值的数据类型，是个tuple: (type, subtype)
    """

    result = None
    if period_type in SELF_PERIOD_TYPES:
        result = SELF_CALCULATOR_MAP.get(object_type, dict()).get(operation, None)
    elif period_type in SIMPLE_PERIOD_TYPES:
        if operation == 'count':
            # count 不需要操作数
            result = ('long', '')
        elif operation == 'group_count':
            result = ('map', 'long')
        else:
            result = SIMPLE_CALCULATOR_MAP.get(object_type, dict()).get(operation, None)
    elif period_type in PERIODICALLY_PERIOD_TYPES:
        if operation == 'count':
            result = ('map', 'long')
        elif operation == 'group_count':
            result = ('mmap', 'long')
        else:
            result = PERIODICALLY_WINDOW_CALCULATOR_MAP.get(object_type, dict()).get(operation, None)
    elif period_type in COMPLEX_PERIOD_TYPES:
        result = COMPLEX_WINDOW_CALCULATOR_MAP.get(object_type, dict()).get(operation, None)

    # 二元统计
    if not result and operation in {'+', '-', '*', '/'}:
        if period_type in SIMPLE_PERIOD_TYPES or period_type in PERIODICALLY_PERIOD_TYPES:
            result = BINARY_CALCULATOR_MAP.get(object_type, dict()).get(operation, None)

    if not result:
        raise RuntimeError('数据类型({}, {})不支持操作{}'.format(object_type[0], object_type[1], operation))

    return result


def is_calculator_support(period_type, object_type, operation):
    """
    操作是否支持

    :param period_type: 统计周期的类型，包括self, hourly等
    :param object_type: 被统计字段的类型
    :param operation: 具体统计的方法
    :return: 操作是否支持
    """

    try:
        if get_calculator_result_type(period_type, object_type, operation):
            return True
        else:
            return False
    except:
        return False
