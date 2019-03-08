#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from .eventschema import get_event_schema
from .name_translate import expressions_supported_on_string, expressions_supported_on_number
from .name_translate import expression_to_name_of_operations


def check_event_exist(event_name, is_src):
    """
    判断事件定义是否存在

    :param event_name: 事件名称
    :param is_src: 是原始日志还是转化日志
    :raise: 事件不存在
    """
    if not get_event_schema(event_name):
        if is_src:
            raise RuntimeError(u"源日志(%s)定义配置不存在" % event_name)
        else:
            raise RuntimeError(u"转化日志(%s)定义配置不存在" % event_name)


def check_field_exist(event_name, field_name, is_src):
    """
    判断事件是否存在某个字段。必须保证事件已经存在

    :param event_name: 事件名称
    :param field_name: 字段名称
    :param is_src: 是原始日志还是转化日志
    :raise: 字段不存在
    """

    schema = get_event_schema(event_name)
    assert schema

    if field_name not in schema:
        if is_src:
            raise RuntimeError(u"源日志(%s)不包含字段(%s)" % (event_name, field_name))
        else:
            raise RuntimeError(u"转化日志(%s)不包含字段(%s)" % (event_name, field_name))


def check_and_return_int(str_value):
    """
    检查是否可以转化为数字，并且返回
    :param str_value: 字符属性
    :return: 如果可转化为数字，转为数字
    :raise: 无法转换为数字
    """

    try:
        result = int(str_value)
    except:
        raise RuntimeError(u"(%s)不是合理的数字" % str_value)

    return result


def check_and_return_double(str_value):
    """
    检查是否可以转化为数字，并且返回
    :param str_value: 字符属性
    :return: 如果可转化为数字，转为数字
    :raise: 无法转换为数字
    """

    try:
        result = float(str_value)
    except:
        raise RuntimeError(u"(%s)不是合理的数字" % str_value)

    return result


def check_and_return_value(raw_value, field_type):
    if isinstance(raw_value, list):
        if field_type in {"str", "string"}:
            return raw_value
        elif field_type in {"int", "long"}:
            return map(check_and_return_int, raw_value)
        elif field_type in {"float", "double"}:
            return map(check_and_return_double, raw_value)
    else:
        if field_type in {"str", "string"}:
            return raw_value
        elif field_type in {"int", "long"}:
            return check_and_return_int(raw_value)
        elif field_type in {"float", "double"}:
            return check_and_return_double(raw_value)


def check_and_return_pattern(str_value):
    """
    检查是否可以转化为正则表达式, 并且返回
    :param str_value: 字符属性
    :return: 如果可转化为正则，返回正则
    :raise: 无法转换为正则
    """

    try:
        result = re.compile(str_value)
    except:
        raise RuntimeError(u"(%s)不是合理的正则表达式" % str_value)

    return result


def check_operation_on_field(src_event_name, field_name, operation_exp):
    """
    检查该字段是否满足此类型操作

    :param src_event_name: 来源日志名称
    :param field_name: 字段名称
    :param operation_exp: 操作表达式
    :raise: 不支持
    """

    schema = get_event_schema(src_event_name)
    assert schema
    field_type = schema.get(field_name)
    assert field_type

    support = False
    if field_type in ("long", "int", "double", "float"):
        if operation_exp in expressions_supported_on_number:
            support = True
    elif field_type in ("str", "string"):
        if operation_exp in expressions_supported_on_string:
            support = True
    else:
        pass

    if not support:
        raise RuntimeError(u"字段(%s)不支持操作(%s)" % (field_name, expression_to_name_of_operations[operation_exp]))


def check_fields_type_mapping(src_event_name, src_field_name, dst_event_name, dst_field_name):
    """
    检查两个字段类型是否一致

    :param src_event_name: 来源日志名称
    :param src_field_name: 来源字段名称
    :param dst_event_name: 转化日志名称
    :param dst_field_name: 转化字段名称
    :raise: 类型不匹配
    """

    src_event_schema = get_event_schema(src_event_name)
    assert src_event_schema
    src_event_type = src_event_schema.get(src_field_name)
    assert src_event_type

    dst_event_schema = get_event_schema(dst_event_name)
    assert dst_event_schema
    dst_event_type = dst_event_schema.get(dst_field_name)
    assert dst_event_type

    if src_event_type != dst_event_type:
        raise RuntimeError(u"(%s.%s)由于类型原因不能转化为(%s.%s)" % (src_event_name, src_field_name, dst_event_name, dst_field_name))

