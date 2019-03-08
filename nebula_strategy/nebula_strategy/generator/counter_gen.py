#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据count表达式动态生成新的变量
"""

import base64

from threathunter_common.util import binary_data
from nebula_meta.variable_filter import Filter
from nebula_meta.variable_model import VariableModel, add_variable_to_registry
from nebula_meta.variable_function import Function

from .condition_gen import gen_ordinary_filter
from .reduction_gen import gen_function
from .gen_utils import get_dimension_from_trigger_keys


def gen_counters_and_filters_from_strategy(s, dimension, before_sleep, trigger_variable_name):
    """
    生成counter列表，以及每个counter列表相关联的条件

    :return: (counters, filters)
    """

    counters = []
    collect_counters = []
    filters = []

    sleep_found = False
    for tid, t in enumerate(s.terms):
        if t.left.subtype == 'sleep':
            sleep_found = True

        if before_sleep:
            if sleep_found:
                # 到此为止
                break
        else:
            if not sleep_found:
                continue

        # realtime
        if t.scope != 'realtime':
            continue

        left = t.left
        if left.type == 'func':
            if left.subtype == 'count':
                # tell dimension
                if get_dimension_from_trigger_keys(left.trigger_fields) != dimension:
                    continue

                counter_pattern = '_{}__strategy__{}__{}__counter__{}%s__rt'.format(
                    dimension,
                    s.version,
                    base64.b16encode(binary_data(s.name)),
                    tid+1)
                gen_counters, collect_counter, sub_filters = gen_counters_and_filters_from_count_exp(t, tid,
                                                                                                 counter_pattern,
                                                                                                 trigger_variable_name)
                counters.extend(gen_counters)
                collect_counters.append(collect_counter)
                filters.extend(sub_filters)

    return counters, collect_counters, filters


def gen_counters_and_filters_from_count_exp(term, termid, name_pattern, trigger_variable_name):
    """
    从count函数配置中生成新的counter.

    返回: (all counters, collect counter， filters)，这里有三块内容
    all counters: 所有生成的counter, 包括用来参与策略计算的counter和中间counter
    collect counter：最终参与策略计算的counter，这一个list就去掉了一些中间counter
    fitlers：collect counters关联的filters

    """

    # 统计的事件
    count_exp = term.left
    source_event_id = count_exp.source_event

    sub_filters = []
    for c in count_exp.condition:
        left = c['left']
        op = c['op']
        right = c['right']
        if op == '=':
            # 等于变量，在counter fix的时候被自动修正
            continue

        sub_filters.append(gen_ordinary_filter(source_event_id[1], left, op, right).get_dict())

    # distinct string should be non empty
    if 'distinct' in count_exp.algorithm:
        for field in count_exp.operand:
            # noinspection PyBroadException
            try:
                sub_filters.append(gen_ordinary_filter(source_event_id[1], field, '!=', '').get_dict())
            except:
                pass

    counter_filter = Filter('', '', '', '', '', '', 'and', '', sub_filters)

    if 'interval' == count_exp.algorithm:
        first_counter_name = name_pattern % '_1_'
        remark = 'interval 1st counter variable(last counter event) for term %s' % termid
        first_counter_function = Function('last', '', 'timestamp', '', '')
        first_variable = VariableModel('realtime', 'nebula', first_counter_name, remark, remark, '', 'enable',
                                       'aggregate', '', '', '',
                                       [{'app': source_event_id[0], 'name': source_event_id[1]}],
                                       counter_filter.get_dict(),
                                       {'type': 'last_n_seconds', 'value': count_exp.interval},
                                       first_counter_function.get_dict(), count_exp.groupby)
        add_variable_to_registry(first_variable)

        second_counter_name = name_pattern % '_2_'
        remark = 'interval 2nd counter variable(last trigger event) for term %s' % termid
        second_source = [
            {
                'app': 'nebula',
                'name': trigger_variable_name
            },
        ]
        second_counter_function = Function('last', '', 'timestamp', '', '')
        second_variable = VariableModel('realtime', 'nebula', second_counter_name, remark, remark, '', 'enable',
                                        'aggregate', '', '', '',
                                        second_source, {}, {'type': 'last_n_seconds', 'value': count_exp.interval},
                                        second_counter_function.get_dict(), count_exp.groupby)
        add_variable_to_registry(second_variable)

        third_counter_name = name_pattern % '_3_'
        remark = 'interval 3rd dual variable(-value) for term %s' % termid
        third_source = [
            {
                'app': 'nebula',
                'name': second_counter_name
            },
            {
                'app': 'nebula',
                'name': first_counter_name
            },
        ]
        third_counter_function = Function('-', '', 'value', '', '')
        third_variable = VariableModel('realtime', 'nebula', third_counter_name, remark, remark, '', 'enable',
                                       'dual', '', '', '',
                                       third_source, {}, {'type': 'last_n_seconds', 'value': count_exp.interval},
                                       third_counter_function.get_dict(), count_exp.groupby)
        add_variable_to_registry(third_variable)
        f = gen_ordinary_filter(third_counter_name, 'value', term.op, term.right.value)
        additional_f = gen_ordinary_filter(third_counter_name, 'value', '>', '0')
        return [first_variable, second_variable, third_variable], third_variable, [f, additional_f]
    else:
        counter_function = gen_function(count_exp.algorithm, count_exp.operand[0])
        counter_name = name_pattern % ''
        remark = 'temp counter for term %s' % termid
        counter_variable = VariableModel('realtime', 'nebula', counter_name, remark, remark, '', 'enable', 'aggregate',
                                         '', '', '', [{'app': source_event_id[0], 'name': source_event_id[1]}],
                                         counter_filter.get_dict(),
                                         {'type': 'last_n_seconds', 'value': count_exp.interval},
                                         counter_function.get_dict(), count_exp.groupby)
        add_variable_to_registry(counter_variable)
        f = gen_ordinary_filter(counter_name, 'value', term.op, term.right.value)
        return [counter_variable], counter_variable, [f]


def gen_counter_from_interval_count_exp(count_exp, counter_name):
    """ 为interval生成新的counter.

    新模式

    :param count_exp:
    :param counter_name:
    :return:
    """

    # 统计的事件
    source_event_id = count_exp.source_event
    counter_function = Function('last', '', 'timestamp', '', '', '')

    sub_filters = []
    for c in count_exp.condition:
        left = c['left']
        op = c['op']
        right = c['right']
        if op == '=':
            # 等于变量，在counter fix的时候被自动修正
            continue

        sub_filters.append(gen_ordinary_filter(source_event_id[1], left, op, right).get_dict())

    counter_filter = Filter('', '', '', '', '', '', 'and', '', sub_filters)

    remark = 'first temp variable from interval counter'

    counter_variable = VariableModel('realtime', 'nebula', counter_name, remark, remark, '', 'enable', 'aggregate',
                                     '', '', '', [{'app': source_event_id[0], 'name': source_event_id[1]}],
                                     counter_filter.get_dict(), {'type': 'ever', 'value': ''},
                                     counter_function.get_dict(), count_exp.groupby)
    add_variable_to_registry(counter_variable)
    return counter_variable
