#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成策略的trigger变量
"""

from nebula_meta.variable_model import VariableModel, add_variable_to_registry
from nebula_meta.variable_filter import Filter
from threathunter_common.util import utf8

from .condition_gen import gen_filter_from_event_exp, gen_filter_from_location_exp
from .condition_gen import gen_ordinary_filter
from .gen_utils import get_trigger_event, get_dimension_from_trigger_keys, get_field_from_dimension


def gen_ip_trigger_variable_from_strategy(strategy, trigger_variable_name, is_delay=False):
    """
    产生ip维度的trigger变量
    new mode

    :return:
    """

    if not strategy:
        return

    conditions = []
    trigger_event = get_trigger_event(strategy, is_delay)

    for t in strategy.terms:
        left = t.left
        if left.type == 'event':
            c = gen_filter_from_event_exp(left, t.op, t.right)
            if c:
                conditions.append(c)

    # location condition has lower priority
    for t in strategy.terms:
        left = t.left
        if left.type == 'func' and left.subtype == 'getlocation':
            c = gen_filter_from_location_exp(left, trigger_event)
            if c:
                conditions.append(c)

    total_filter = {}
    if is_delay:
        total_filter = gen_ordinary_filter(trigger_event[1], 'delay_strategy', '==', strategy.name).get_dict()
    else:
        if conditions:
            conditions = [_.get_dict() for _ in conditions]
            total_filter = Filter('', '', '', '', '', '', 'and', '', conditions).get_dict()
    remark = 'ip trigger for strategy {}'.format(utf8(strategy.name))
    variable = VariableModel('realtime', 'nebula', trigger_variable_name, remark, remark, '', 'enable', 'filter',
                             '', '', '', [{'app': trigger_event[0], 'name': trigger_event[1]}], total_filter,
                             {}, {}, ['c_ip'])
    add_variable_to_registry(variable)

    return variable


def gen_dimension_trigger_variable_from_strategy(strategy, trigger_variable_name, dimension, is_delay=False):
    """
    产生uid/did维度的trigger变量
    new mode

    :return:
    """

    trigger_event = get_trigger_event(strategy, is_delay)

    # mappings, getvariable和count的触发字段必须包含在trigger event，这样在collect variable才能获取
    dimension_count = 0
    for t in strategy.terms:
        left = t.left

        # only care realtime vars
        if t.scope != 'realtime':
            continue

        # only dimension related
        if left.subtype in {'getvariable', 'count'}:
            if get_dimension_from_trigger_keys(left.trigger_fields) != dimension:
                continue
            else:
                dimension_count += 1

    if not dimension_count:
        return None

    dimension_field = get_field_from_dimension(dimension)
    filter_dict = gen_ordinary_filter(trigger_event[1], dimension_field, '!=', '').get_dict()

    remark = '{} trigger for strategy {}'.format(dimension, utf8(strategy.name))
    variable = VariableModel('realtime', 'nebula', trigger_variable_name, remark, remark, '', 'enable', 'filter',
                             '', '', '', [{'app': trigger_event[0], 'name': trigger_event[1]}], filter_dict, {},
                             {}, [dimension_field])
    add_variable_to_registry(variable)
    return variable
