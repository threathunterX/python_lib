#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查看strategy是否需要生成
"""


def is_strategy_effective(s):
    """
    根据不同条件判断策略是否有效
    :param s:
    :return:
    """

    return is_strategy_ineffect_according_to_effect_config(s) and \
           is_strategy_ineffect_according_to_status(s) and \
           is_strategy_ineffect_according_to_time_exp(s)


def is_strategy_ineffect_according_to_time_exp(s):
    """
    根据time函数判断策略是否有效

    :param s:
    :return:
    """

    import datetime
    current = datetime.datetime.now()
    current_min_of_day = current.hour * 60 + current.minute
    for t in s.terms:
        left = t.left
        if left.type == 'func' and left.subtype == 'time':
            start_min_of_day = left.starthour * 60 + left.startmin
            end_min_of_day = left.endhour * 60 + left.endmin

            if start_min_of_day <= current_min_of_day <= end_min_of_day:
                return True

            if start_min_of_day > end_min_of_day:
                if start_min_of_day < current_min_of_day or current_min_of_day < end_min_of_day:
                    return True

            return False
        else:
            pass

    return True


def is_strategy_ineffect_according_to_effect_config(s):
    """
    根据生效时间戳判断策略是否有效

    :param s:
    :return:
    """

    from threathunter_common.util import millis_now
    now = millis_now()
    return s.start_effect < now < s.end_effect


def is_strategy_ineffect_according_to_status(s):
    """
    根据策略状态函数判断策略是否有效

    :param s:
    :return:
    """

    return s.status in {'test', 'online'}
