#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
getvariable的生成相关
"""

from .condition_gen import gen_ordinary_filter
from .gen_utils import get_dimension_from_trigger_keys


def gen_getvariables_filters_from_strategy(s, dimension, before_sleep):
    """
    生成getvariable列表，以及每个variable列表相关联的条件

    :return: (variables, filters)
    """

    variables = []
    filters = []

    sleep_found = False
    for t in s.terms:
        if t.left.subtype == 'sleep':
            sleep_found = True

        if before_sleep:
            if sleep_found:
                # 到此为止
                break
        else:
            if not sleep_found:
                continue

        # only for real term
        if t.scope != "realtime":
            continue

        left = t.left
        if left.type == "func":
            if left.subtype == "getvariable":
                # dimension
                if get_dimension_from_trigger_keys(left.trigger_fields) != dimension:
                    continue

                variables.append(left.variable)

                f = gen_ordinary_filter(left.variable[1], 'value', t.op, t.right.value)
                filters.append(f)

    return variables, filters


def get_all_used_realtime_variables(s):
    result = []

    for t in s.terms:
        # only for real term
        if t.scope != "realtime":
            continue

        left = t.left
        if left.type == "func":
            if left.subtype == "getvariable":
                result.append(left.variable[1])

    return result
