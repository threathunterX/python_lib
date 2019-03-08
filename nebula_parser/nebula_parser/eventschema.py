#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threathunter_common.util import millis_now
from threathunter_common.util import run_in_thread

from parser_initializer import get_fn_load_event_schemas

event_schema_dictionary = dict()
last_update_ts = 0


def get_event_schema(event_name):
    """
    获取某个事件的schema，返回为一个字典{字段名称：字段属性}
    :param event_name:
    :return:
    """

    now = millis_now()

    # 初始化
    if last_update_ts == 0:
        load_event_schemas()

    if now - last_update_ts >= 30000:
        run_in_thread(load_event_schemas)

    return event_schema_dictionary.get(event_name)


def get_event_field_type(event_name, field_name):
    """
    获取某个事件的某个字段的名称
    :param event_name: 事件名称
    :param field_name: 字段名称
    :return:
    """
    schema = get_event_schema(event_name)
    if not schema:
        raise RuntimeError(u"日志(%s)定义配置不存在" % event_name)

    result_type = schema.get(field_name)
    if not result_type:
        raise RuntimeError(u"日志(%s)不包含字段(%s)" % (event_name, field_name))

    return result_type


def load_event_schemas():
    """
    更新event schema
    :return:
    """
    global event_schema_dictionary
    global last_update_ts

    # update ts first, as this operation costs time, it may send multiple update command if we don't do this
    last_update_ts = millis_now()

    try:
        event_schema_dictionary = get_fn_load_event_schemas()()
    except:
        pass

