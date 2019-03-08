#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从策略产生变量
"""

import logging
import base64

from threathunter_common.util import binary_data

from .effective_checking import is_strategy_effective
from .trigger_gen import gen_ip_trigger_variable_from_strategy, gen_dimension_trigger_variable_from_strategy
from .collector_gen import gen_dimension_collector_variable_from_strategy
from ..validate.strategy_checking import check_strategy
from ..strategy_fixing import fix_strategy
from .getvariable_gen import get_all_used_realtime_variables


logger = logging.getLogger('nebula.strategy.generator.online')
DEBUG_PREFIX = '==============='


def strategy_need_delay_trigger(s):
    return any(_.left.subtype == 'sleep' for _ in s.terms)


def gen_variables_from_strategy(s, effective_check=True):
    """
    产生变量列表.
    返回由策略生成的变量列表，和getvariable用到的实时变量列表,即 （generated variables, used rt variable）

    :param s:
    :param effective_check: 是否需要进行effective检查d
    :return:
    """
    if not s:
        return [], list()

    if effective_check and not is_strategy_effective(s):
        return [], list()

    fix_strategy(s)
    check_strategy(s)

    generated_variables = []
    used_realtime_variables = []

    # IP维度，包括event变量, getlocation变量，realtime的getvariable, realtime的count
    trigger_variable_name = '_ip__strategy__{}__{}__trigger__rt'.format(s.version,
                                                                        base64.b16encode(binary_data(s.name)))
    collect_variable_name = '_ip__strategy__{}__{}__collect__rt'.format(s.version,
                                                                        base64.b16encode(binary_data(s.name)))

    trigger_variable = gen_ip_trigger_variable_from_strategy(s, trigger_variable_name)
    generated_variables.append(trigger_variable)

    is_delay = strategy_need_delay_trigger(s)

    # sleep 函数支持 另外增加的IP维度的trigger和collector
    if is_delay:
        delay_trigger_vn = '_ip__strategy__{}__{}__delay_trigger__rt'.format(s.version,
                                                                             base64.b16encode(binary_data(s.name)))
        delay_collect_vn = '_ip__strategy__{}__{}__collector_delayer__rt'.format(s.version,
                                                                                 base64.b16encode(binary_data(s.name)))

        delay_counters, delay_collect_v = gen_dimension_collector_variable_from_strategy(s, delay_collect_vn, trigger_variable_name,
                                                                         'ip', before_sleep=True, is_delay=True)
        generated_variables.extend(delay_counters)
        generated_variables.append(delay_collect_v)

        delay_trigger_v = gen_ip_trigger_variable_from_strategy(s, delay_trigger_vn, is_delay=True)
        generated_variables.append(delay_trigger_v)

        # 如果是延迟策略collector的srcVariable是delay_trigger 和counter, trigger 是delay_trigger
        trigger_variable_name = delay_trigger_vn

    if is_delay:
        # collector拿sleep后面的条件
        before_sleep = False
    else:
        before_sleep = True
    counters, collect_variable = gen_dimension_collector_variable_from_strategy(s, collect_variable_name,
                                                                                trigger_variable_name, 'ip',
                                                                                before_sleep, is_delay=False)
    generated_variables.extend(counters)
    generated_variables.append(collect_variable)

    # DID/UID维度，包括realtime的getvariable，realtime的count
    for dimension in {'uid', 'did'}:
        trigger_variable_name = '_{}__strategy__{}__{}__trigger__rt'.format(dimension,
                                                                            s.version,
                                                                            base64.b16encode(binary_data(s.name)))
        trigger_variable = gen_dimension_trigger_variable_from_strategy(s, trigger_variable_name, dimension,
                                                                        is_delay=False)
        if not trigger_variable:
            continue
        generated_variables.append(trigger_variable)

        collect_variable_name = '_{}__strategy__{}__{}__collect__rt'.format(dimension,
                                                                            s.version,
                                                                            base64.b16encode(binary_data(s.name)))

        # sleep 函数支持 另外增加的IP维度的trigger和collector
        if is_delay:
            delay_collect_vn = \
                '_{}__strategy__{}__{}__collector_delayer__rt'.format(dimension,
                                                                      s.version,
                                                                      base64.b16encode(binary_data(s.name)))
            delay_counters, delay_collect_v = gen_dimension_collector_variable_from_strategy(s, delay_collect_vn,
                                                                             trigger_variable_name, dimension,
                                                                             before_sleep=True, is_delay=True)
            generated_variables.extend(delay_counters)
            generated_variables.append(delay_collect_v)

            delay_trigger_vn = '_{}__strategy__{}__{}__delay_trigger__rt'.format(dimension,
                                                                                 s.version,
                                                                                 base64.b16encode(binary_data(s.name)))
            delay_trigger_v = gen_dimension_trigger_variable_from_strategy(s, delay_trigger_vn, dimension,
                                                                           is_delay=True)

            generated_variables.append(delay_trigger_v)

            # 如果是延迟策略collector的srcVariable是delay_trigger 和counter, trigger 是delay_trigger
            trigger_variable_name = delay_trigger_vn

        if is_delay:
            # collector拿sleep后面的条件
            before_sleep = False
        else:
            before_sleep = True
        counters, collect_variable = gen_dimension_collector_variable_from_strategy(s, collect_variable_name,
                                                                                    trigger_variable_name, dimension,
                                                                                    before_sleep, is_delay=False)
        generated_variables.extend(counters)
        generated_variables.append(collect_variable)

    used_realtime_variables = get_all_used_realtime_variables(s)

    return generated_variables, used_realtime_variables

