#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threathunter_common.util import millis_now
from threathunter_common.util import run_in_thread

from .generator import gen_generator
from .parser_initializer import get_fn_load_parsers

current_generators = []
last_update_ts = 0


def get_current_generators():
    """
    获取当前所有的日志解析
    :return:
    """

    now = millis_now()

    # 初始化
    if last_update_ts == 0:
        load_parsers()

    if now - last_update_ts >= 30000:
        run_in_thread(load_parsers())

    return current_generators


def load_parsers():
    """
    更新日志解析列表
    :return:
    """
    global current_generators
    global last_update_ts

    # update ts first, as this operation costs time, it may send multiple update command if we don't do this
    last_update_ts = millis_now()

    try:
        parser_fn = get_fn_load_parsers()
        if parser_fn:
            parsers = parser_fn()
            generators = [gen_generator(p) for p in parsers]
            current_generators = generators
    except:
        pass


def test_parsers(parsers=None):
    if not parsers:
        parser_fn = get_fn_load_parsers()
        if parser_fn:
            parsers = parser_fn()
        else:
            return
    generators = [gen_generator(p) for p in parsers]
    return generators

