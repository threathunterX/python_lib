#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证策略的条款正确性
"""

from threathunter_common.util import utf8

from .common_check import check_condition_support, check_variable_exist
from .common_check import check_field_exist, check_event_exist
from ..event_var_data import get_event_field_type, get_variable_schema


def _check_event_expression_and_return_type(exp, trigger_event, error_prefix):
    """
    检验event expression中的event存在性，并且返回使用字段的数据类型
    :param exp:
    :param trigger_event:
    :param error_prefix:
    :return:
    """

    event = exp.event
    field = exp.field

    if not isinstance(event, list) or len(event) != 2:
        raise RuntimeError(utf8(error_prefix) + '事件配置不正确')

    check_event_exist(event[1], error_prefix)
    check_variable_exist(event[1], error_prefix)
    check_field_exist(event[1], field, error_prefix)
    return get_event_field_type(event[1], field, error_prefix)


def _check_constant_expression_and_return_type(exp, trigger_event, error_prefix):
    """
    检验event expression的右值，应该是字符串常量.

    :param exp:
    :param trigger_event:
    :param error_prefix:
    :return:
    """

    if exp.value is None:
        raise RuntimeError(utf8(error_prefix) + '常量不能为NULL值')
    # todo value type
    return 'string'


def _check_getvariable_expression_and_return_type(exp, trigger_event, error_prefix):
    if not isinstance(exp.trigger_event, list) or len(exp.trigger_event) != 2:
        raise RuntimeError(utf8(error_prefix) + '事件配置不正确')

    check_event_exist(exp.trigger_event[1], error_prefix)

    if exp.trigger_event != trigger_event:
        raise RuntimeError(utf8(error_prefix) + '触发事件不一致')

    trigger_event = exp.trigger_event
    trigger_fields = exp.trigger_fields
    if not trigger_fields:
        raise RuntimeError(utf8(error_prefix) + '触发字段为空')

    for field in trigger_fields:
        check_field_exist(trigger_event[1], field, error_prefix)

    variable = exp.variable
    if not isinstance(variable, list) or len(variable) != 2:
        raise RuntimeError(utf8(error_prefix) + '变量配置不正确')

    check_variable_exist(variable[1], error_prefix)

    variable_schema = get_variable_schema(variable[1])
    # 实际数据基本为int、double,不过还是看实际类型
    value_type = variable_schema['value']
    # subtype优先，对应到profile等类型；一般类型为type
    return value_type[1] or value_type[0]


def _check_count_expression_and_return_type(exp, trigger_event, error_prefix):
    # source event

    source_event = exp.source_event
    if not isinstance(source_event, list) or len(source_event) != 2:
        raise RuntimeError(utf8(error_prefix) + '事件配置不正确')

    check_event_exist(source_event[1], error_prefix)

    if exp.trigger_event != trigger_event:
        raise RuntimeError(utf8(error_prefix) + '触发事件不一致')

    if not exp.trigger_fields:
        raise RuntimeError(utf8(error_prefix) + '触发字段为空')

    # todo: limitation, only single field is support now
    if len(exp.trigger_fields) > 1:
        raise RuntimeError(utf8(error_prefix) + '目前只支持单字段触发')
    if not exp.groupby or len(exp.groupby) > 1:
        raise RuntimeError(utf8(error_prefix) + '目前只支持单字段触发')

    # todo: condition
    for c in exp.condition:
        left = c['left']
        op = c['op']
        right = c['right']

        if op == '=':
            # 特殊的等于变量
            continue

        check_field_exist(source_event[1], left, error_prefix)
        left_type = get_event_field_type(source_event[1], left, error_prefix)
        check_condition_support(left_type, op, right, error_prefix)

    # interval
    if not exp.interval > 0:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的时间窗口值' % utf8(exp.interval))

    # algorithm
    if exp.algorithm == 'distinct':
        # 算法修正
        exp.algorithm = 'distinct_count'
    if exp.algorithm not in {'count', 'distinct_count', 'interval'}:
        raise RuntimeError(utf8(error_prefix) + '不支持算法(%s)' % utf8(exp.algorithm))

    # trigger event / fields
    trigger_event = exp.trigger_event
    trigger_fields = exp.trigger_fields

    if not isinstance(trigger_event, list) or len(trigger_event) != 2:
        raise RuntimeError(utf8(error_prefix) + '事件配置不正确')

    check_event_exist(trigger_event[1], error_prefix)
    for field in trigger_fields:
        check_field_exist(trigger_event[1], field, error_prefix)

    # group by
    for field in exp.groupby:
        check_field_exist(source_event[1], field, error_prefix)

    # operand
    if not exp.operand:
        raise RuntimeError(utf8(error_prefix) + '算法(%s)缺少统计对象' % utf8(exp.algorithm))

    for field in exp.operand:
        check_field_exist(source_event[1], field, error_prefix)

    groupby_trigger_matching = True
    if len(exp.groupby) != len(exp.trigger_fields):
        groupby_trigger_matching = False
    else:
        for groupby_field, trigger_field in zip(exp.groupby, exp.trigger_fields):
            # check if the data type matching
            if get_event_field_type(source_event[1], groupby_field, error_prefix) != \
                    get_event_field_type(trigger_event[1], trigger_field, error_prefix):
                groupby_trigger_matching = False
                break
    if not groupby_trigger_matching:
        raise RuntimeError(utf8(error_prefix) + '触发维度和统计维度不一致')

    # 实际数据为int double
    return 'double'


def _check_setblacklist_expression_and_return_type(exp, trigger_event, error_prefix):
    if exp.name not in {'VISITOR', 'ORDER', 'ACCOUNT', 'TRANSACTION', 'MARKETING', 'OTHER'}:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的场景' % utf8(exp.name))

    if exp.check_type not in {'IP', 'USER', 'DeviceID', 'OrderID'} \
            and (exp.check_type.find('(') == -1 or exp.check_type.find(')') == -1):
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的值类型' % utf8(exp.check_type))

    if exp.decision not in {'accept', 'review', 'reject'}:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的黑名单处置方式' % utf8(exp.decision))

    if not exp.ttl > 0:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的ttl值' % utf8(exp.ttl))

    # check_value, 黑名单字段
    check_field_exist(trigger_event[1], exp.check_value, error_prefix)
    return ''


def _check_time_expression_and_return_type(exp, trigger_event, error_prefix):
    # todo: the constructor has done that
    return ''


def _check_sleep_expression_and_return_type(exp, trigger_event, error_prefix):
    # todo: the constructor has done that
    return ''


def _check_spl_expression_and_return_type(exp, trigger_event, error_prefix):
    # todo: the constructor has done that
    return ''


def is_ascii(s):
    """
    判断是否全英文.

    :param s:
    :return:
    """
    return all(ord(c) < 128 for c in s)


def _check_location_expression_and_return_type(exp, trigger_event, error_prefix):
    source_event_field = exp.source_event_field
    source_event_field = source_event_field.split('.')
    if not isinstance(source_event_field, list) or len(source_event_field) != 3:
        raise RuntimeError(utf8(error_prefix) + '不正确的地理位置参数')

    _, event, field = source_event_field
    check_event_exist(event, error_prefix)
    check_field_exist(event, field, error_prefix)

    if get_event_field_type(event, field) not in {'string', 'str'}:
        raise RuntimeError(utf8(error_prefix) + 'getlocation的来源字段(%s)不是字符串类型' % utf8(field))

    if event != trigger_event[1]:
        raise RuntimeError(utf8(error_prefix) +
                           'getlocation的来源事件(%s)与触发事件(%s)不一致' % (utf8(event), utf8(trigger_event[1])))

    if exp.op not in {'belong', '!belong', '=', '!='}:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的操作符' % utf8(exp.op))

    if exp.location_type not in {'city', 'province'}:
        raise RuntimeError(utf8(error_prefix) + '(%s)不是正确的类型' % utf8(exp.location_type))

    if not exp.location_value or not filter(lambda _: _ is not None, exp.location_value):
        raise RuntimeError(utf8(error_prefix) + 'getlocation缺乏参数配置')

    if exp.op in {'=', '!='} and len(exp.location_value) > 1:
        raise RuntimeError(utf8(error_prefix) + '等于操作只支持一个变量')

    # check location string is in event
    for field in exp.location_value:
        if not is_ascii(field):
            # chinese
            break
        check_field_exist(trigger_event[1], field, error_prefix)
    return ''


def _check_exp_and_return_type(exp, trigger_event, error_prefix):
    exp_checking_fn_dict = {
        ('event', ''): _check_event_expression_and_return_type,
        ('constant', ''): _check_constant_expression_and_return_type,
        ('func', 'getvariable'): _check_getvariable_expression_and_return_type,
        ('func', 'count'): _check_count_expression_and_return_type,
        ('func', 'setblacklist'): _check_setblacklist_expression_and_return_type,
        ('func', 'time'): _check_time_expression_and_return_type,
        ('func', 'sleep'): _check_sleep_expression_and_return_type,
        ('func', 'spl'): _check_spl_expression_and_return_type,
        ('func', 'getlocation'): _check_location_expression_and_return_type
    }

    fn = exp_checking_fn_dict.get((exp.type, exp.subtype))
    if not fn:
        raise RuntimeError(utf8(error_prefix) + '表达式(%s:%s)不支持' % (utf8(exp.type), utf8(exp.subtype)))

    return fn(exp, trigger_event, error_prefix)


def check_term(term, trigger_event, error_prefix):
    """
    检查term正确性.

    :param term:
    :param trigger_event:
    :param error_prefix:
    :return:
    """

    if term.remark is None and term.left.subtype != 'setblacklist':
        raise RuntimeError(utf8(error_prefix) + '条款描述为空')

    if term.scope not in {'realtime', 'profile'}:
        raise RuntimeError(utf8(error_prefix) + '条款的适用类型错误')

    left = term.left
    if left is None:
        raise RuntimeError(utf8(error_prefix) + '条款左表达式为空')
    return_type = _check_exp_and_return_type(left, trigger_event, utf8(error_prefix) + '左表达式>>')

    if left.subtype in {'setblacklist', 'time', 'getlocation', 'sleep', 'spl'}:  # @todo no right set
        # no right exp
        pass
    else:
        right = term.right
        if right is None:
            raise RuntimeError(utf8(error_prefix) + '条款右表达式为空')
        _check_exp_and_return_type(right, trigger_event, utf8(error_prefix) + '右表达式>>')

        if left.type == 'event':
            check_condition_support(return_type, term.op, right.value, error_prefix)
        else:
            # 其他类型都默认为value字段
            check_condition_support(return_type, term.op, right.value, error_prefix)
