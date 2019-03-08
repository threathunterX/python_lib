#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通过配置产生property_condition
"""

from nebula_meta.variable_filter import Filter
from threathunter_common.util import utf8
from ..event_var_data import get_variable_field_type


def gen_ordinary_filter(source_name, field, op, op_value):
    """
    根据操作符和操作数产生普通的条件配置

    """

    if not field:
        raise RuntimeError('条件的属性为空')

    field_type = get_variable_field_type(source_name, field)
    if not field_type:
        raise RuntimeError('无法从事件{}中取得字段{}的定义'.format(utf8(source_name), utf8(field)))

    if field_type == 'string':
        return _gen_string_filter(source_name, field, op, op_value)
    elif field_type == 'long':
        return _gen_number_filter(source_name, field, op, op_value, is_long=True)
    elif field_type == 'double':
        return _gen_number_filter(source_name, field, op, op_value, is_long=False)
    else:
        raise RuntimeError('不支持类型为({})的条件'.format(utf8(field_type)))


def gen_filter_from_location_exp(location_exp, trigger_event):
    """
    从getlocation配置中产生表达式

    :return:
    """

    fields = location_exp.source_event_field.split('.')
    if len(fields) != 3 or fields[0] != trigger_event[0] or fields[1] != trigger_event[1]:
        raise RuntimeError('地理位置参数不正确')

    if location_exp.op == '=':
        condition_type = 'locationequals'
    elif location_exp.op == '!=':
        condition_type = '!locationequals'
    elif location_exp.op == 'belong':
        condition_type = 'locationcontainsby'
    elif location_exp.op == '!belong':
        condition_type = '!locationcontainsby'
    else:
        raise RuntimeError('操作不正确')

    location_value = location_exp.location_value
    if isinstance(location_value, list):
        location_value = ','.join(location_value)
    location_value = utf8(location_value)

    return _gen_string_filter(fields[1], fields[2], condition_type, location_value, utf8(location_exp.location_type))


def gen_filter_from_event_exp(event_field_exp, op, constant_exp):
    """
    从event配置中产生条件表达式

    :param event_field_exp:
    :param op:
    :param constant_exp:
    :return:
    """

    event = event_field_exp.event
    field = event_field_exp.field

    return gen_ordinary_filter(event[1], field, op, constant_exp.value)


_string_condition_ops = {'==', '!=', 'in', '!in', 'contain', '!contain', 'contains', '!contains', 'startwith',
                         '!startwith', 'endwith', '!endwith', 'regex', '!regex', 'empty', '!empty', 'match',
                         '!match', 'containsby', '!containsby', 'locationequals', '!locationequals',
                         'locationcontainsby', '!locationcontainsby'}


def _gen_string_filter(source, string_field, op, op_value, param=''):
    """
    get string field function

    """

    if not op or op not in _string_condition_ops:
        raise RuntimeError('string类型不支持({})操作'.format(utf8(op)))

    if op == 'in':
        parts = op_value.split(',')
        if len(parts) > 10:
            raise RuntimeError('属于最多支持10个属性')

        condition = [_gen_string_filter(source, string_field, '==', part, param).get_dict() for part in parts]
        return Filter('', '', '', '', '', '', 'or', '', condition)

    elif op == '!in':
        parts = op_value.split(',')
        if len(parts) > 10:
            raise RuntimeError('属于最多支持10个属性')

        condition = [_gen_string_filter(source, string_field, '!=', part, param).get_dict() for part in parts]
        return Filter('', '', '', '', '', '', 'and', '', condition)

    if op == 'regex':
        op = 'match'
    elif op == '!regex':
        op = '!match'
    elif op == 'contain':
        op = 'contains'
    elif op == '!contain':
        op = '!contains'

    return Filter(source, string_field, '', '', op, utf8(op_value), 'simple', param, None)


_number_condition_ops = {'>', '<', '==', '!=', '>=', '<=', 'between', 'in', '!in'}


def _gen_number_filter(source, number_field, op, op_value, is_long=False):
    """
    get number field function

    """

    if not op or op not in _number_condition_ops:
        raise RuntimeError('数字类型不支持({})操作'.format(utf8(op)))

    if op == 'between':
        parts = op_value.split(',')
        if len(parts) != 2:
            raise RuntimeError('介于需要两个参数')
        left, right = parts
        left_condition = Filter(source, number_field, '', '', '>=', utf8(left), 'simple', '', None).get_dict()
        right_condition = Filter(source, number_field, '', '', '<=', utf8(right), 'simple', '', None).get_dict()
        return Filter('', '', '', '', '', '', 'and', '', [left_condition, right_condition])
    elif op == 'in':
        parts = op_value.split(',')
        if len(parts) > 10:
            raise RuntimeError('属于最多支持10个属性')

        condition = [_gen_number_filter(source, number_field, '==', part, is_long).get_dict() for part in parts]
        return Filter('', '', '', '', '', '', 'or', '', condition)
    elif op == '!in':
        parts = op_value.split(',')
        if len(parts) > 10:
            raise RuntimeError('属于最多支持10个属性')

        condition = [_gen_number_filter(source, number_field, '!=', part, is_long).get_dict() for part in parts]
        return Filter('', '', '', '', '', '', 'and', '', condition)

    # simple condition
    try:
        if is_long:
            op_value = int(op_value)
        else:
            op_value = float(op_value)
    except:
        raise RuntimeError('({})不是数字'.format(utf8(op_value)))

    return Filter(source, number_field, '', '', op, utf8(op_value), 'simple', '', None)

