#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通过配置产生property_reduction
"""

from nebula_meta.variable_function import Function
from threathunter_common.util import utf8


def gen_function(method, field):
    """
    根据配置产生count/distinct count的聚合算子。

    :return:
    """

    if not method or method not in {'count', 'distinct_count'}:
        raise RuntimeError('不支持操作({})'.format(utf8(method)))

    return Function(method, '', field, '', '', '')

