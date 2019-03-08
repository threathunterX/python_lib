#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threathunter_common.event import Event
from threathunter_common.eventmeta import EventMeta
from threathunter_common.property import Property
from threathunter_common.variable import Variable

__author__ = 'lw'


def test_eventmeta():
    m = EventMeta("app", "name", "simpletype", False, ("app", "sourceevent"),
                  [Property(("app", "sourceevent"), "propertyname", "string")],
                  10000L, "remark")
    print m.get_json()
    assert m == EventMeta.from_json(m.get_json())


def test_strategy():
    pass
    # s = Strategy("app", "testname", "description", 111111, "online", 11111, 11111, 11111, 111111, [])
    # print s.get_json()
    # assert s == Strategy.from_json(s.get_json())


def test_variable():
    v = Variable("app", "testname", "key", 0L, 0.0, {"key1": "value", "key2": 2})
    print v.get_json()
    assert v == Variable.from_json(v.get_json())





