#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
变量的过滤条件
"""

import json
import re
from collections import OrderedDict

from .util import utf8, text

from value_type import is_value_type_supported


class Filter(object):

    def __init__(self, source, object, object_type, object_subtype, operation, value, type, param, condition):
        self._source = text(source or '')
        self._object = text(object or '')
        self._object_type = text(object_type or '')
        self._object_subtype = text(object_subtype or '')
        self._operation = text(operation or '')
        self._value = text(value if value is not None else '')
        self._param = text(param if param is not None else '')
        condition = condition or list()
        self._condition = [Filter.from_dict(c) for c in condition]
        self._type = text(type or '')

    def sanity_check(self):
        """
        进行整体的check
        :return:
        """

        if self.is_empty():
            if self._source or self._object or self._object_type or self.object_subtype or self._value or \
                    self._param or self._type or self.condition:
                raise RuntimeError('空的过滤条件不应该有多余的配置')
            return

        if self.type not in {'and', 'or', 'not', 'simple'}:
            raise RuntimeError('{}不是正确的过滤类型'.format(self.type))

        if self.type == 'simple':
            if not self.object or not self.source or not self.object_type or not self.operation:
                raise RuntimeError('过滤条件不正确')

            if not is_value_type_supported(self.object_type, self.object_subtype):
                raise RuntimeError('过滤条件的参数类型不正确')

            if not is_filter_operation_support((self.object_type, self.object_subtype), self.operation):
                raise RuntimeError('数据类型({}, {})不支持操作{}'.format(self.object_type, self.object_subtype,
                                                                self.operation))
        else:
            for c in self.condition:
                c.sanity_check()

    def check_value(self):
        """
        对filter的value参数进行规范化

        1. 如果类型不正确或者操作不支持，返回异常
        2. 对value，规范化为合适的value

        :return:
        """

        if self.is_empty():
            return

        if self.type in {'and', 'or', 'not'}:
            # 复杂类型
            for c in self.condition:
                c.check_value()
        elif self.type == 'simple':
            # 简单类型
            key_type = (self.object_type, self.object_subtype)
            if not is_filter_operation_support(key_type, self.operation):
                raise RuntimeError('字段{}({})不支持操作{}'.format(self.object, key_type, self.operation))

            self.value = normalize_filter_value(key_type, self.operation, self.value)
        else:
            raise RuntimeError('不支持类型为{}的过滤条件'.format(self.type))

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
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, operation):
        self._operation = text(operation or '')

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = text(value if value is not None else '')

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type or '')

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = text(param if param is not None else '')

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        condition = condition or list()
        self._condition = [Filter.from_dict(c) for c in condition]

    def fix_source(self, default_source):
        """
        source 没有指定时设置默认的source
        :param default_source:
        :return:
        """

        # recursively
        if self.type in {'and', 'or', 'not'}:
            for c in self.condition:
                c.fix_source(default_source)

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

        if self.type != 'simple':
            for c in self.condition:
                c.check_type(source_fields_type_dict)
            return

        # now it's a simple filter
        if not self.source:
            return

        fields_mapping = source_fields_type_dict.get(self.source)
        if not fields_mapping:
            raise RuntimeError('变量{}缺乏数据定义'.format(self.source))

        field_type = fields_mapping.get(self.object)
        if not field_type:
            raise RuntimeError('变量{}找不到字段{}的定义'.format(self.source, self.object))
        self.object_type, self.object_subtype = field_type

    def is_empty(self):
        # 可能为空
        return self.type == ''

    def get_dict(self):
        if self.is_empty():
            return dict()

        result = dict()
        if self.type == 'simple':
            result['source'] = self.source
            result['object'] = self.object
            result['object_type'] = self.object_type
            result['object_subtype'] = self.object_subtype
            result['operation'] = self.operation
            result['value'] = self.value
            result['type'] = self.type
            result['param'] = self.param
        else:
            result['type'] = self.type
            result['condition'] = [c.get_dict() for c in self.condition]

        return result

    def get_ordered_dict(self):
        if self.is_empty():
            return OrderedDict()

        kv = list()
        if self.type == 'simple':
            kv.append(('source', self.source))
            kv.append(('object', self.object))
            kv.append(('object_type', self.object_type))
            kv.append(('object_subtype', self.object_subtype))
            kv.append(('operation', self.operation))
            kv.append(('value', self.value))
            kv.append(('type', self.type))
            kv.append(('param', self.param))
        else:
            kv.append(('type', self.type))
            kv.append(('condition', [c.get_ordered_dict() for c in self.condition]))

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
        if self.type == 'simple':
            if self.source and self.source != can_ignore_source:
                kv.append(('source', self.source))

            kv.append(('object', self.object))
            kv.append(('operation', self.operation))
            if self.value:
                kv.append(('value', self.value))
            kv.append(('type', self.type))
            if self.param:
                kv.append(('param', self.param))
        else:
            kv.append(('type', self.type))
            kv.append(('condition', [c.get_simplified_ordered_dict(can_ignore_source) for c in self.condition]))

        return OrderedDict(kv)

    def get_json(self):
        return json.dumps(self.get_ordered_dict())

    @staticmethod
    def from_dict(d):
        return Filter(d.get('source'), d.get('object'), d.get('object_type'), d.get('object_subtype'),
                      d.get('operation'), d.get('value'), d.get('type'), d.get('param'), d.get('condition'))

    @staticmethod
    def from_json(json_str):
        return Filter.from_dict(json.loads(json_str))

    def copy(self):
        return Filter.from_dict(self.get_dict())

    def __str__(self):
        return "Filter[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


# 过滤条件支持的操作和操作数类型
FILTER_OPERATION_VALUE_TYPE = {
    ('string', ''): {
        '==': 'string',
        '!=': 'string',
        'contains': 'string',
        '!contains': 'string',
        'contain': 'string',
        '!contain': 'string',
        'startwith': 'string',
        '!startwith': 'string',
        'endwith': 'string',
        '!endwith': 'string',
        'empty': 'null',
        '!empty': 'null',
        'match': 'regex',
        '!match': 'regex',
        'regex': 'regex',
        '!regex': 'regex',
        'containsby': 'string',
        '!containsby': 'string',
        'in': 'string',
        '!in': 'string',
        'locationequals': 'string',
        '!locationequals': 'string',
        'locationcontainsby': 'string',
        '!locationcontainsby': 'string',
    },
    ('long', ''): {
        '==': 'long',
        '!=': 'long',
        '>': 'long',
        '<': 'long',
        '>=': 'long',
        '<=': 'long',
    },
    ('double', ''): {
        '==': 'double',
        '!=': 'double',
        '>': 'double',
        '<': 'double',
        '>=': 'double',
        '<=': 'double',
    },
    ('bool', ''): {
        '==': 'bool',
        '!=': 'bool',
    }
}


def is_filter_operation_support(key_type, operation):
    """
    检查过滤条件是否支持

    :param key_type: 被过滤的字段类型
    :param operation: 过滤操作
    :return:
    """

    value_type = FILTER_OPERATION_VALUE_TYPE.get(key_type, dict()).get(operation)
    if value_type:
        return True
    else:
        return False


def normalize_filter_value(key_type, operation, value):
    """
    对普通型的过滤条件进行规范化，根据预定义的数据类型和操作，然后对过滤值进行变换
    :param key_type:
    :param operation:
    :param value:

    :return:
    """

    if not key_type or not operation or value is None:
        raise RuntimeError('filter参数不正确')

    value_type = FILTER_OPERATION_VALUE_TYPE.get(key_type, dict()).get(operation)
    if not value_type:
        raise RuntimeError('操作符不支持')

    if value_type == 'string':
        return utf8(value)
    elif value_type == 'bool':
        if value.lower() in {'true', 't', 'y', 'yes'}:
            return True
        else:
            return False
    elif value_type == 'long':
        try:
            return long(value)
        except:
            raise RuntimeError('{}无法转化为整型'.format(value))
    elif value_type == 'double':
        try:
            return float(value)
        except:
            raise RuntimeError('{}无法转化为浮点型'.format(value))
    elif value_type == 'stringlist':
        if isinstance(value, list):
            value = map(str, value)
            return value
        elif isinstance(value, (unicode, str)):
            value = value.split(',')
            return value
        else:
            raise RuntimeError('{}类型不正确'.format(value))
    elif value_type == 'regex':
        result = utf8(value)
        try:
            re.compile(value)
        except:
            raise RuntimeError('{}不是正确的正则表达式'.format(value))
        return result
    elif value_type == 'null':
        # 不需要额外的过滤参数
        return ''
    else:
        raise RuntimeError('数据类型{}不支持'.format(value_type))


