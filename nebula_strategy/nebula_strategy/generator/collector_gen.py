#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成collector变量
"""

from threathunter_common.util import utf8
from nebula_meta.variable_model import VariableModel, add_variable_to_registry
from nebula_meta.variable_filter import Filter

from .getvariable_gen import gen_getvariables_filters_from_strategy
from .counter_gen import gen_counters_and_filters_from_strategy
from .gen_utils import get_field_from_dimension


def gen_dimension_collector_variable_from_strategy(s, collect_variable_name, trigger_variable_name, dimension,
                                                   before_sleep=True, is_delay=False):
    """
    为IP/UID/DID产生collector变量
    :param s: 策略
    :param collect_variable_name: collector变量
    :param trigger_variable_name: trigger变量
    :param dimension: 维度，ip/uid/did
    :param before_sleep: 是否找sleep之前的条款
    :param is_delay: 是否生成delaycollector
    :return: 产生该维度的变量
    """

    sources = list()
    sources.append({'app': 'nebula', 'name': trigger_variable_name})

    variables, variables_conditions = gen_getvariables_filters_from_strategy(s, dimension, before_sleep)
    for v in variables:
        sources.append({'app': v[0], 'name': v[1]})

    counters, collect_counters, counter_conditions = gen_counters_and_filters_from_strategy(s, dimension, before_sleep,
                                                                                            trigger_variable_name)
    for counter in collect_counters:
        sources.append({'app': 'nebula', 'name': counter.name})

    conditions = (variables_conditions or list()) + (counter_conditions or list())

    remark = 'collector for strategy {}'.format(utf8(s.name))
    total_filter = {}
    if conditions:
        conditions = [_.get_dict() for _ in conditions]
        total_filter = Filter('', '', '', '', '', '', 'and', '', conditions).get_dict()

    dimension_field = get_field_from_dimension(dimension)
    if is_delay:
        variable_type = 'delaycollector'
    else:
        variable_type = 'collector'

    collector_function = {
        'method': 'setblacklist',
        'param': s.name,
        'config': {
            'trigger': trigger_variable_name
        }
    }
    if is_delay:
        sleep_terms = [_ for _ in s.terms if _.left.subtype == 'sleep']
        if not sleep_terms:
            raise RuntimeError('sleep配置出错')
        duration = int(sleep_terms[0].left.duration)
        unit = sleep_terms[0].left.unit
        if unit == 'm':
            duration *= 60
        elif unit == 'h':
            duration *= 3600
        collector_function['config']['sleep'] = duration

    variable = VariableModel('realtime', 'nebula', collect_variable_name, remark, remark, '', 'enable', variable_type,
                             '', '', '', sources, total_filter, {}, collector_function, [dimension_field])
    add_variable_to_registry(variable)
    return counters, variable


