#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
前端传过来的数据结构可能有问题，
"""


def fix_strategy(strategy):
    """
    对策略进行一次修正。

    :param strategy:
    :return:
    """
    if not strategy:
        return

    for term in strategy.terms:
        # 1. null mark
        fix_term_null_remark(term)

        left = term.left
        if not left:
            continue

        # 2. trigger fields
        if left.type == 'func' and left.subtype == 'count':
            fix_counter_exp(left)

    # 3. scope issue
    fix_strategy_scope(strategy)


def fix_strategy_scope(strategy):
    """
    对策略的所有term的scope进行修正
    :param strategy:
    :return:
    """

    has_profile = False
    for term in strategy.terms:
        scope = fix_term_scope(term)
        if scope == 'profile':
            has_profile = True

    if has_profile:
        # fix set blacklist
        # 只有有一项是profile，setblacklist的scope就是profile

        for term in strategy.terms:
            if term.left and term.left.type == 'func' and term.left.subtype == 'setblacklist':
                term.scope = 'profile'


def fix_term_scope(term):
    """
    对term的scope进行硬处理

    :param term:
    :return:
    """

    scope = 'realtime'

    left = term.left
    if left and left.type == 'func' and left.subtype == 'getvariable' and 'profile' in left.variable[1]:
        scope = 'profile'

    # hard code
    if left and left.subtype == 'setblacklist' and left.check_type == 'USER(register_channel)':
        scope = 'profile'

    if left and left.subtype == 'spl':
        scope = 'profile'

    term.scope = scope
    return scope


def fix_term_null_remark(term):
    """
    防止term的remark为空
    :param term:
    :return:
    """

    if term.remark is None:
        term.remark = ''


def fix_counter_exp(counter_exp):
    """
    修正counter表达式，前端可能没有对groupby和trigger解析正确
    :param counter_exp:
    :return:
    """

    # in case the js part is not acting normally
    trigger_fields = []
    groupby_fields = []
    for counter in counter_exp.condition:
        if counter['op'] != '=':
            continue

        groupby_fields.append(counter['left'])
        trigger_fields.append(counter['right'])

    if trigger_fields:
        counter_exp.trigger_fields = trigger_fields
        counter_exp.groupby = groupby_fields
