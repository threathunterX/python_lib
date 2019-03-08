#!/usr/bin/env python
# -*- coding: utf-8 -*-


from nebula_meta.model.strategy import Strategy
from nebula_meta.model.term import Term
from nebula_strategy.generator.effective_checking import (is_strategy_effective,
                                                          is_strategy_ineffect_according_to_effect_config,
                                                          is_strategy_ineffect_according_to_time_exp,
                                                          is_strategy_ineffect_according_to_status)

from test.fixtures import *
from ..setup_test import load_strategy_content


def setup_module(module):
    print ("start testing")


def teardown_module(module):
    print ("finish testing")


strategy_example = load_strategy_content()
get_strategy_example = lambda: Strategy.from_json(strategy_example)


def test_time_term(patch_datetime_now):
    s = get_strategy_example()
    s.terms.append(Term.from_dict({
        "left": {
            "type": "func",
            "subtype": "time",
            "config": {
                "start": "11:00",
                "end": "17:00"
            }
        },
        "right": None,
        "op": None
    }))

    set_fake_time((2016, 11, 15, 13, 05, 55))
    assert is_strategy_ineffect_according_to_time_exp(s)

    set_fake_time((2016, 11, 15, 9, 05, 55))
    assert not is_strategy_ineffect_according_to_time_exp(s)

    set_fake_time((2016, 11, 15, 20, 05, 55))
    assert not is_strategy_ineffect_according_to_time_exp(s)

    s = get_strategy_example()
    s.terms.append(Term.from_dict({
        "left": {
            "type": "func",
            "subtype": "time",
            "config": {
                "start": "17:00",
                "end": "11:00"
            }
        },
        "right": None,
        "op": None
    }))

    set_fake_time((2016, 11, 15, 13, 05, 55))
    assert not is_strategy_ineffect_according_to_time_exp(s)

    set_fake_time((2016, 11, 15, 9, 05, 55))
    assert is_strategy_ineffect_according_to_time_exp(s)

    set_fake_time((2016, 11, 15, 20, 05, 55))
    assert is_strategy_ineffect_according_to_time_exp(s)

    s.terms = filter(lambda t: t.left.subtype != "time", s.terms)
    assert is_strategy_ineffect_according_to_time_exp(s)


def test_effective_timestamp(patch_datetime_now):
    s = get_strategy_example()
    s.start_effect = 1000
    s.end_effect = 5000

    set_fake_time_on_ts(3000)
    assert is_strategy_ineffect_according_to_effect_config(s)

    set_fake_time_on_ts(500)
    assert not is_strategy_ineffect_according_to_effect_config(s)

    set_fake_time_on_ts(10000)
    assert not is_strategy_ineffect_according_to_effect_config(s)


def test_status():
    s = get_strategy_example()

    s.status = "inedit"
    assert not is_strategy_ineffect_according_to_status(s)

    s.status = "outline"
    assert not is_strategy_ineffect_according_to_status(s)

    s.status = "test"
    assert is_strategy_ineffect_according_to_status(s)

    s.status = "online"
    assert is_strategy_ineffect_according_to_status(s)
