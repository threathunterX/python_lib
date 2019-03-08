#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
存放数据类型相关的信息
"""

import json
from .util import utf8


SUPPORTED_TYPES = {
    'long': {},
    'string': {},
    'double': {},
    'bool': {},
    'list': {'long', 'double', 'string', 'kv'},
    'map': {'long', 'double', 'string', 'list'},
    'mlist': {'long', 'double', 'string', 'kv'},
    'mmap': {'long', 'double', 'string', 'list'},
}


def get_supported_types():
    """
    获取所有支持的数据类型
    :return: set
    """

    return set(SUPPORTED_TYPES.keys())


def get_supported_subtypes(value_type):
    """
    获取数据类型下所有支持的子类型
    :return: set
    """

    return SUPPORTED_TYPES.get(value_type, set())


def is_value_type_supported(value_type, value_subtype):
    """
    判断数据类型是否是系统定义的数据类型
    :param value_type:
    :param value_subtype:
    :return:
    """

    if value_type not in get_supported_types():
        return False

    if value_subtype and value_subtype not in get_supported_subtypes(value_type):
        return False

    return True


def is_value_valid(value_type, value_subtype, value):
    """
    判断一个值是否是正确的值

    :param value_type:
    :param value_subtype:
    :param value:
    :return:
    """

    try:
        convert_value_by_type(value_type, value_subtype, value)
        return True
    except:
        return False


def convert_value_by_simple_type(value_type, value):
    """
    根据简单类型(long/double/string/bool)来转换数据

    :return:
    """

    if value_type == 'long':
        value = int(value)
    elif value_type == 'double':
        value = float(value)
    elif value_type == 'string':
        value = utf8(value)
    elif value_type == 'kv':
        if not isinstance(value, dict):
            raise RuntimeError('{}不是正确的键值对'.format(value))
        if 'k' not in value or not isinstance(value['k'], (str, unicode)):
            raise RuntimeError('{}不是正确的键值对'.format(value))
        if 'v' not in value or not isinstance(value['v'], (int, float)):
            raise RuntimeError('{}不是正确的键值对'.format(value))
    else:
        raise RuntimeError('{}不是一个简单数据类型'.format(value_type))

    return value


def convert_value_by_type(value_type, value_subtype, value):
    """
    根据数据的类型进行转换.
    有些值是前端配置的，可能是一个字符串，需要进行一定的转换

    :return:
    """

    if value_type in {'long', 'double', 'string'}:
        value = convert_value_by_simple_type(value_type, value)
    elif value_type == 'bool':
        # value 可能是string
        if isinstance(value, (unicode, str)):
            if value and value.lower() == 'true':
                value = True
            else:
                value = False
        else:
            value = bool(value)
    elif value_type in {'list', 'map', 'mlist', 'mmap'}:
        # value可能是string
        if isinstance(value, (unicode, str)):
            value = json.loads(value)

        if value_type == 'list':
            if not isinstance(value, list):
                raise RuntimeError('{}不是一个list'.format(value))

            value = [convert_value_by_simple_type(value_subtype, _) for _ in value]
        elif value_type == 'map':
            if not isinstance(value, dict):
                raise RuntimeError('{}不是一个map'.format(value))

            value = {str(k): convert_value_by_simple_type(value_subtype, v) for k, v in value.items()}
        elif value_type == 'mlist':
            if not isinstance(value, dict):
                raise RuntimeError('{}不是一个mlist'.format(value))

            new_value = dict()
            for k, v in value.items():
                if not isinstance(v, list):
                    raise RuntimeError('{}不是一个mlist'.format(value))

                new_value[str(k)] = [convert_value_by_simple_type(value_subtype, _) for _ in v]
            value = new_value
        elif value_type == 'mmap':
            if not isinstance(value, dict):
                raise RuntimeError('{}不是一个mmap'.format(value))

            new_value = dict()
            for k, v in value.items():
                if not isinstance(v, dict):
                    raise RuntimeError('{}不是一个mmap'.format(value))

                new_value[str(k)] = {str(ik): convert_value_by_simple_type(value_subtype, iv) for ik, iv in v.items()}
            value = new_value
    else:
            raise RuntimeError('不支持数据类型{}'.format(value_type))

    return value
