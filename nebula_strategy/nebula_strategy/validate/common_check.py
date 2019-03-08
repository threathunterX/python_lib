#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
定义正确性检查的通用函数
"""

import re
from threathunter_common.util import utf8
from ..event_var_data import get_event_schema, get_variable_schema, get_event_field_type

# 列举condition的操作符、中文名和可支持的类型
condition_operations = [
    ('>', u'大于', {'number'}),
    ('<', u'小于', {'number'}),
    ('>=', u'大于等于', {'number'}),
    ('<=', u'小于等于', {'number'}),
    ('==', u'等于', {'string', 'number'}),
    ('!=', u'不等于', {'string', 'number'}),
    ('between', u'介于', {'number'}),
    ('in', u'属于', {'string', 'number'}),
    ('!in', u'不属于', {'string', 'number'}),
    ('contain', u'包含', {'string'}),
    ('!contain', u'不包含', {'string'}),
    ('startwith', u'以..开始', {'string'}),
    ('!startwith', u'不以..开始', {'string'}),
    ('endwith', u'以..结束', {'string'}),
    ('!endwith', u'不以..结束', {'string'}),
    ('regex', u'匹配正则', {'string'}),
    ('!regex', u'不匹配正则', {'string'}),
    ]


expression_to_name_of_operations = {expression: name for expression, name, _
                                    in condition_operations}
name_to_expression_of_operations = {name: expression for expression, name, _
                                    in condition_operations}

operations_supported_on_string = filter(lambda _: 'string' in _[2], condition_operations)
operations_supported_on_number = filter(lambda _: 'number' in _[2], condition_operations)

expressions_supported_on_string = map(lambda _: _[0], operations_supported_on_string)
expressions_supported_on_number = map(lambda _: _[0], operations_supported_on_number)


def check_event_exist(event_name, error_prefix=''):
    """
    判断事件定义是否存在

    :param event_name: 事件名称
    :param error_prefix: 出错提示
    :raise: 事件不存在
    """

    get_event_schema(event_name, error_prefix)


def check_field_exist(event_name, field_name, error_prefix=""):
    """
    判断事件是否存在某个字段。必须保证事件已经存在

    :param event_name: 事件名称
    :param field_name: 字段名称
    :param error_prefix: 出错提示
    :raise: 字段不存在
    """

    get_event_field_type(event_name, field_name, error_prefix)


def check_variable_exist(variable_name, error_prefix=""):
    """
    检查变量是否存在
    :param variable_name:
    :param error_prefix: 出错提示
    :return:
    """

    get_variable_schema(variable_name, error_prefix)


def check_and_return_int(str_value, error_prefix=''):
    """
    检查是否可以转化为数字，并且返回
    :param str_value: 字符属性
    :return: 如果可转化为数字，转为数字
    :param error_prefix: 出错提示
    :raise: 无法转换为数字
    """

    try:
        result = int(str_value)
    except:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是合理的数字' % utf8(str_value))

    return result


def check_and_return_double(str_value, error_prefix=''):
    """
    检查是否可以转化为数字，并且返回
    :param str_value: 字符属性
    :return: 如果可转化为数字，转为数字
    :param error_prefix: 出错提示
    :raise: 无法转换为数字
    """

    try:
        result = float(str_value)
    except:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是合理的数字' % utf8(str_value))

    return result


def check_and_return_value(raw_value, field_type, error_prefix=''):
    """
    检查value是否符合给定的数据类型
    :param raw_value: 原始数据，string
    :param field_type: raw_value应该对应的错误类型
    :param error_prefix: 出错提示
    :return:
    """

    if isinstance(raw_value, list):
        if field_type in {'str', 'string'}:
            return raw_value
        elif field_type in {'int', 'long'}:
            return map(check_and_return_int, raw_value, [error_prefix] * len(raw_value))
        elif field_type in {'float', 'double'}:
            return map(check_and_return_double, raw_value, [error_prefix] * len(raw_value))
    else:
        if field_type in {'str', 'string'}:
            return raw_value
        elif field_type in {'int', 'long'}:
            return check_and_return_int(raw_value, error_prefix)
        elif field_type in {'float', 'double'}:
            return check_and_return_double(raw_value, error_prefix)


def check_and_return_pattern(str_value, error_prefix=''):
    """
    检查是否可以转化为正则表达式, 并且返回
    :param str_value: 字符属性
    :return: 如果可转化为正则，返回正则
    :param error_prefix: 出错提示
    :raise: 无法转换为正则
    """

    try:
        result = re.compile(str_value)
    except:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是合理的正则表达式' % utf8(str_value))

    return result


def check_right_exp(left_type, operation_exp, right_value, error_prefix=''):
    """
    检查左右表达式是否合法
    :param left_type: 左值数据类型
    :param operation_exp: 操作符
    :param right_value: 右值
    :param error_prefix: 出错提示
    :return:
    """

    if operation_exp in {'in', '!in', 'between'}:
        right_value = right_value.split(',')

    if 'regex' in operation_exp:
        check_and_return_pattern(right_value, error_prefix)
    else:
        # 非正则情况下，右表达式应该和做表达式数据类型一致
        check_and_return_value(right_value, left_type, error_prefix)


def check_condition_support(left_type, operation_exp, right_value, error_prefix=''):
    """
    检查左右表达式是否合法, 以及操作是否合法

    :param left_type: 左值数据类型
    :param operation_exp: 操作符
    :param right_value: 右值
    :param error_prefix: 出错提示
    :return:
    """

    support = False
    if left_type in ('long', 'int', 'double', 'float'):
        if operation_exp in expressions_supported_on_number:
            support = True

    elif left_type in ('str', 'string'):
        if operation_exp in expressions_supported_on_string:
            support = True
    else:
        raise RuntimeError(utf8(error_prefix) + '不支持类型为(%s)的操作' % utf8(left_type))

    if not support:
        raise RuntimeError(utf8(error_prefix) + '类型(%s)不支持操作(%s)' \
                           % (utf8(left_type), utf8(expression_to_name_of_operations[operation_exp])))

    check_right_exp(left_type, operation_exp, right_value, error_prefix)
    if 'between' in operation_exp:
        # should be two arguments
        if len(right_value.split(',')) != 2:
            raise RuntimeError(utf8(error_prefix) + '介于需要逗号分隔的两个数字')
