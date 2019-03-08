#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest
from nebula_meta.model.strategy import Strategy
from nebula_strategy.validate.strategy_checking import check_strategy
from nebula_strategy.generator.online_gen import gen_variables_from_strategy

from test.setup_test import load_config, load_strategy_content


def setup_module(module):
    print ("start testing")
    load_config()


def teardown_module(module):
    print ("finish testing")


strategy_example = load_strategy_content()
get_strategy_example = lambda: Strategy.from_json(strategy_example)


def test_strategy_successful():
    s = get_strategy_example()
    try:
        check_strategy(s)
    except Exception as t:
        pass


def test_strategy_fail_invalid_score():
    s = get_strategy_example()
    s.score = -1

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    print excinfo.value.message
    assert excinfo.value.message == '策略(策略)>>策略分值不正确'


def test_strategy_fail_null_strategy():
    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(None)
    assert excinfo.value.message == '策略为空'


def test_strategy_fail_invalid_category():
    s = get_strategy_example()
    s.category = "invalid"

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>(invalid)不是正确的策略场景'


def test_strategy_fail_invalid_field():
    s = get_strategy_example()
    s.terms[0].left.field = "not_exist"

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>条款1>>左表达式>>事件(ACCOUNT_LOGIN)不包含字段(not_exist)'


def test_strategy_fail_multiple_event():
    s = get_strategy_example()
    s.terms[0].left.event = ["nebula", "HTTP_DYNAMIC"]

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>事件条款不能包含不同的事件'


def test_strategy_fail_zero_event():
    s = get_strategy_example()
    s.terms = filter(lambda t: t.left.type != "event", s.terms)

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>策略缺乏事件条款'


def test_strategy_fail_mix_dimension():
    s = get_strategy_example()
    s.terms[2].left.trigger_fields = ["uid", "c_ip"]

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>目前只支持单字段触发'


def test_strategy_fail_zero_dimension():
    s = get_strategy_example()
    s.terms[2].left.trigger_fields = ["status"]

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>必须包含ip/uid/did任一key'


def test_strategy_fail_zero_setblacklist():
    s = get_strategy_example()
    s.terms = filter(lambda t: t.left.subtype != "setblacklist", s.terms)

    with pytest.raises(RuntimeError) as excinfo:
        check_strategy(s)
    assert excinfo.value.message == '策略(策略)>>策略缺乏黑名单设置条款'


def test_strategy_spl_term():
    import json
    j = json.dumps({
        "app": "nebula",
        "tags": [],
        "category": "VISITOR",
        "createtime": 0,
        "endeffect": 1508818130000,
        "modifytime": 0,
        "name": "fdf",
        "remark": "t",
        "starteffect": 1508731726000,
        "status": "inedit",
        "terms": [
            {
        "remark": "",
                "op": "contain",
                "left": {
                    "subtype": "",
                    "config": {
                        "field": "c_ip",
                        "event": [
                            "nebula",
                            "HTTP_DYNAMIC"
                        ]
          },
          "type": "event"
                },
        "right": {
            "subtype": "",
            "config": {
                "value": "."
            },
          "type": "constant"
        }
      },
      {
        "left": {
            "subtype": "spl",
            "config": {
                "expression": "$CHECK_NOTICE(keytype=ip,ip=c_ip,scene_name=VISITOR) > 0"
            },
            "type": "func"
        }
      },
      {
        "left": {
            "subtype": "setblacklist",
            "config": {
                "remark": "",
                "name": "VISITOR",
                "checktype": "USER",
                "decision": "review",
                "checkvalue": "uid",
                "checkpoints": "",
                "ttl": 3600
            },
          "type": "func"
        }
      }
        ]
    })
    s = Strategy.from_json(j)
    gen_variables_from_strategy(s, effective_check=False)
