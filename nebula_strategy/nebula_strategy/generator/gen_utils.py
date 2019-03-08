#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
generator用到的工具函数
"""


def get_trigger_event(s, is_delay=False):
    """
    获取触发事件
    """

    # 取第一个event term
    for t in s.terms:
        if t.left.type == 'event':
            if is_delay:
                new = t.left.event[:]
                new[-1] = new[-1] + '_DELAY'
                return new
            return t.left.event


def get_dimension_from_trigger_keys(trigger_keys):
    """
    从触发字段中判断维度

    :param trigger_keys:
    :return:
    """

    if 'uid' in trigger_keys:
        return 'uid'

    if 'did' in trigger_keys:
        return 'did'

    # ip by default
    return 'ip'


def get_field_from_dimension(dimension):
    if dimension == 'uid':
        return 'uid'
    elif dimension == 'did':
        return 'did'
    else:
        return 'c_ip'
