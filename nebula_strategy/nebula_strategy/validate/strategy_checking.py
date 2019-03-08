#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检车策略正确性
"""

from threathunter_common.util import utf8
from .term_checking import check_term


def check_strategy(strategy, error_prefix=''):
    """
    检查策略是否正确

    :param strategy:
    :param error_prefix:
    :return:
    """

    if not strategy:
        raise RuntimeError(error_prefix + '策略为空')

    if not error_prefix:
        error_prefix = '策略(%s)>>' % utf8(strategy.name)

    if strategy.status not in {'online', 'inedit', 'outline', 'test'}:
        raise RuntimeError(error_prefix + '策略状态不正确')

    if not strategy.terms:
        raise RuntimeError(error_prefix + '策略缺乏条款')

    event_terms = \
        filter(lambda t: t.left.type == 'event' and t.left.subtype == '', strategy.terms)
    if not event_terms:
        raise RuntimeError(error_prefix + '策略缺乏事件条款')

    sbl_terms = filter(lambda t: t.left.type == 'func' and t.left.subtype == 'setblacklist', strategy.terms)
    if not sbl_terms:
        raise RuntimeError(error_prefix + '策略缺乏黑名单设置条款')

    trigger_event = event_terms[0].left.event
    for et in event_terms:
        if et.left.event != trigger_event:
            raise RuntimeError(error_prefix + '事件条款不能包含不同的事件')

    for i, t in enumerate(strategy.terms):
        check_term(t, trigger_event, error_prefix + '条款%d>>' % (i+1))

    try:
        int(strategy.start_effect)
        int(strategy.end_effect)
        int(strategy.modify_time)
        int(strategy.create_time)
    except:
        raise RuntimeError(error_prefix + '策略时间戳不正确')

    if not strategy.score >= 0:
        raise RuntimeError(error_prefix + '策略分值不正确')

    if strategy.category not in {'VISITOR', 'ORDER', 'ACCOUNT', 'TRANSACTION',
                                 'MARKETING', 'OTHER'}:
        raise RuntimeError(error_prefix + '(%s)不是正确的策略场景' % utf8(strategy.category))

    # check trigger_keys
    for t in strategy.terms:
        if t.left.type != 'func' or t.left.subtype not in {'count', 'getvariable'}:
            continue

        trigger_fields = t.left.trigger_fields
        # todo: limitation, only single field is support now
        if len(trigger_fields) > 1:
            raise RuntimeError(error_prefix + '目前只支持单字段触发')
        dimension_count = 0
        for d in {'c_ip', 'uid', 'did'}:
            if d in trigger_fields:
                dimension_count += 1

        if dimension_count > 1:
            raise RuntimeError(error_prefix + '不能包含ip/uid/did的组合key')

        if dimension_count == 0:
            raise RuntimeError(error_prefix + '必须包含ip/uid/did任一key')
