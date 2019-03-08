#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
strategy处理需要的事件和变量元数据
"""

from threathunter_common.util import utf8
from nebula_meta.variable_model import get_variable_fields_mapping_from_registry, get_variable_from_registry


def get_event_schema(event_name, error_prefix=''):
    """
    获取某个事件的schema，返回为一个字典{字段名称：字段属性}
    :param event_name: 事件名称
    :param error_prefix: 出错提示

    :return: dict(field->type)
    """

    # 策略里的event实际从event/filter variable里面来
    event_variable = get_variable_from_registry('nebula', event_name)
    if not event_variable:
        raise RuntimeError(utf8(error_prefix) + '事件(%s)定义配置不存在' % utf8(event_name))
    if event_variable.type not in {'event', 'filter'}:
        raise RuntimeError(utf8(error_prefix) + '事件(%s)定义配置不正确' % utf8(event_name))

    result = get_variable_fields_mapping_from_registry('nebula', event_name)
    if not result:
        raise RuntimeError(utf8(error_prefix) + '事件(%s)定义配置不存在' % utf8(event_name))

    return result


def get_event_field_type(event_name, field_name, error_prefix=''):
    """
    获取某个事件的某个字段的类型
    :param event_name: 事件名称
    :param field_name: 字段名称
    :param error_prefix: 出错提示
    :return: field type
    """

    schema = get_event_schema(event_name)
    result_type = schema.get(field_name)
    if not result_type:
        raise RuntimeError(utf8(error_prefix) + '事件(%s)不包含字段(%s)' % (utf8(event_name), utf8(field_name)))

    # 策略中的目前基本是基本类型
    if result_type[1]:
        raise RuntimeError(utf8(error_prefix) + '暂不支持%s(%s)这种复杂数据类型' % (utf8(field_name), utf8(result_type)))
    return result_type[0]


def get_variable_schema(variable_name, error_prefix=''):
    """
    获取某个变量的schema，返回为一个字典{字段名称：字段属性}
    :param variable_name: 变量名称
    :param error_prefix: 出错提示

    :return: dict(field->type)
    """

    variable = get_variable_from_registry('nebula', variable_name)
    if not variable:
        raise RuntimeError(utf8(error_prefix) + '变量(%s)定义配置不存在' % utf8(variable_name))

    result = get_variable_fields_mapping_from_registry('nebula', variable_name)
    if not result:
        raise RuntimeError(utf8(error_prefix) + '变量(%s)定义配置不存在' % utf8(variable_name))

    return result


def get_variable_field_type(variable_name, field_name, error_prefix=''):
    """
    获取某个变量的某个字段的类型
    """

    schema = get_variable_schema(variable_name)
    result_type = schema.get(field_name)
    if not result_type:
        raise RuntimeError(utf8(error_prefix) + '变量(%s)不包含字段(%s)' % (utf8(variable_name), utf8(field_name)))

    # 策略中的目前基本是基本类型
    if result_type[1]:
        raise RuntimeError(utf8(error_prefix) + '暂不支持%s(%s)这种复杂数据类型' % (utf8(field_name), utf8(result_type)))
    return result_type[0]
