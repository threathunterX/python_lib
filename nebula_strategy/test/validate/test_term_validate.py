#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest

from nebula_meta.model.term import *
from nebula_strategy.validate.term_checking import check_term

from test.setup_test import load_config


def setup_module(module):
    print ('start testing')
    load_config()


def teardown_module(module):
    print ('finish testing')


event_term = """
  {
      "remark": "登录IP包含.",
      "op": "contain",
      "right": {
        "subtype": "",
        "config": {
          "value": "."
        },
        "type": "constant"
      },
      "left": {
        "subtype": "",
        "config": {
          "field": "c_ip",
          "event": [
            "nebula",
            "ACCOUNT_LOGIN"
          ]
        },
        "type": "event"
      },
      "scope": "realtime"
    }
"""


def get_event_term():
    return Term.from_json(event_term)


def test_event_term_success():
    check_term(get_event_term(), ['nebula', 'ACCOUNT_LOGIN'], '')


def test_event_term_fail_invalid_event():
    term = get_event_term()
    term.left.event = ['nebula', 'not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>事件(not_exsit)定义配置不存在'


def test_event_term_fail_invalid_field():
    term = get_event_term()
    term.left.field = 'not_exsit'

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>事件(ACCOUNT_LOGIN)不包含字段(not_exsit)'


getvariable_term = """ {
      "remark": "单IP5分钟内登录请求次数大于1",
      "op": ">",
      "right": {
        "subtype": "",
        "config": {
          "value": "1"
        },
        "type": "constant"
      },
      "left": {
        "subtype": "getvariable",
        "config": {
          "variable": [
            "nebula",
            "ip__account_login_count__5m__rt"
          ],
          "trigger": {
            "keys": [
              "c_ip"
            ],
            "event": [
              "nebula",
              "ACCOUNT_LOGIN"
            ]
          }
        },
        "type": "func"
      },
      "scope": "realtime"
    }
"""
get_getvariable_term = lambda :Term.from_json(getvariable_term)


def test_getvariable_term_success():
    check_term(get_getvariable_term(), ['nebula', 'ACCOUNT_LOGIN'], '')


def test_getvariable_term_fail_invalid_event():
    term = get_getvariable_term()
    term.left.trigger_event = ['nebula', 'not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>事件(not_exsit)定义配置不存在'


def test_getvariable_term_fail_invalid_event():
    term = get_getvariable_term()
    term.left.trigger_fields = ['not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>事件(ACCOUNT_LOGIN)不包含字段(not_exsit)'


def test_getvariable_term_fail_invalid_variable():
    term = get_getvariable_term()
    term.left.variable = ['nebula', 'not_exist']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>变量(not_exist)定义配置不存在'


def test_getvariable_term_fail_inconsistent_event():
    term = get_getvariable_term()

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>触发事件不一致'


def test_getvariable_term_fail_null_trigger_field():
    term = get_getvariable_term()
    term.left.trigger_fields = []
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>触发字段为空'


count_term = """{
      "remark": "5分钟内单IP404次数超过10次",
      "op": ">",
      "right": {
        "subtype": "",
        "config": {
          "value": "10"
        },
        "type": "constant"
      },
      "left": {
        "subtype": "count",
        "config": {
          "algorithm": "count",
          "interval": 300,
          "sourceevent": [
            "nebula",
            "HTTP_DYNAMIC"
          ],
          "trigger": {
            "keys": [
              "c_ip"
            ],
            "event": [
              "nebula",
              "HTTP_DYNAMIC"
            ]
          },
          "operand": ["c_ip"],
          "groupby": [
            "c_ip"
          ],
          "condition": [
            {
              "right": "c_ip",
              "left": "c_ip",
              "op": "="
            },
            {
              "right": "404",
              "left": "status",
              "op": "=="
            }
          ]
        },
        "type": "func"
      },
      "scope": "realtime"
    }
"""


get_count_term = lambda : Term.from_json(count_term)


def test_count_term_success():
    check_term(get_count_term(), ['nebula', 'HTTP_DYNAMIC'], '')


def test_count_term_fail_invalid_event():
    # trigger event
    term = get_count_term()
    term.left.source_event = ['nebula', 'not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(not_exsit)定义配置不存在'

    # source event
    term = get_count_term()
    term.left.source_event = ['nebula', 'not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(not_exsit)定义配置不存在'


def test_count_term_fail_invalid_field():
    # trigger fields
    term = get_count_term()
    term.left.trigger_fields = ['not_exsit']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exsit)'

    term = get_count_term()
    term.left.operand = ['not_exist']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'


def test_count_term_fail_invalid_interval():
    # interval
    term = get_count_term()
    term.left.interval = -1

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>(-1)不是正确的时间窗口值'


def test_count_term_fail_invalid_algorithm():
    # algorithm
    term = get_count_term()
    term.left.algorithm = 'max'

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>不支持算法(max)'


def test_count_term_fail_null_operand():
    term = get_count_term()
    term.left.operand = []

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>算法(count)缺少统计对象'


def test_count_term_fail_invalid_groupby():
    # groupby
    term = get_count_term()
    term.left.groupby = ['not_exist']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'

    # groupby != trigger
    term = get_count_term()
    term.left.groupby = ['c_ip']
    term.left.trigger_fields = ['c_ip', 'uid']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>目前只支持单字段触发'


def test_only_support_one_dimension_now():
    # groupby
    term = get_count_term()
    term.left.groupby = ['c_ip', 'uid']

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>目前只支持单字段触发'

    term = get_count_term()
    term.left.trigger_fields = ['c_ip', 'uid']
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>目前只支持单字段触发'


def test_count_term_fail_invalid_condition():
    # condition
    term = get_count_term()
    term.left.condition.append({
        'left': 'not_exist',
        'op': '==',
        'right': '0'
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'

    term = get_count_term()
    term.left.condition.append({
        'left': 'c_ip',
        'op': '>',
        'right': '0'
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>类型(string)不支持操作(大于)'

    term = get_count_term()
    term.left.condition.append({
        'left': 'status',
        'op': '>',
        'right': 'a'
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>(a)不是合理的数字'

    term = get_count_term()
    term.left.condition.append({
        'left': 'uri_stem',
        'op': 'regex',
        'right': '('
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>(()不是合理的正则表达式'

    term = get_count_term()
    term.left.condition.append({
        'left': 'status',
        'op': 'between',
        'right': '1'
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>介于需要逗号分隔的两个数字'

    term = get_count_term()
    term.left.condition.append({
        'left': 'status',
        'op': 'between',
        'right': '1,a'
    })

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>(a)不是合理的数字'


def test_count_term_fail_inconsistent_trigger():
    term = get_count_term()
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '左表达式>>触发事件不一致'


def test_count_term_fail_null_trigger_field():
    term = get_count_term()
    term.left.trigger_fields = []
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')
    assert excinfo.value.message == '左表达式>>触发字段为空'


sbl_term = """
   {
      "remark": null,
      "op": "",
      "right": null,
      "left": {
        "subtype": "setblacklist",
        "config": {
          "remark": "登录请求近期未加载静态资源，单独请求API(PC端)",
          "name": "VISITOR",
          "checktype": "IP",
          "decision": "review",
          "checkvalue": "c_ip",
          "checkpoints": "",
          "ttl": 300
        },
        "type": "func"
      },
      "scope": "profile"
    }
    """
get_sbl_term = lambda: Term.from_json(sbl_term)


def test_sbl_term_success():
    check_term(get_sbl_term(), ['nebula', 'HTTP_DYNAMIC'], '')


def test_sbl_term_fail_invalid_name():
    # trigger event
    term = get_sbl_term()

    term.left.name = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(invalid)不是正确的场景'


def test_sbl_term_fail_invalid_decision():
    term = get_sbl_term()

    term.left.decision = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(invalid)不是正确的黑名单处置方式'


def test_sbl_term_fail_invalid_check_type():
    term = get_sbl_term()

    term.left.check_type = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(invalid)不是正确的值类型'


def test_sbl_term_fail_invalid_check_value():
    term = get_sbl_term()

    term.left.check_value = 'not_exist'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'


def test_sbl_term_fail_invalid_ttl():
    term = get_sbl_term()

    term.left.ttl = -1
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(-1)不是正确的ttl值'


getlocation_term = """
   {
      "remark": "",
      "op": "",
      "right": null,
      "left": {
          "type": "func",
          "subtype": "getlocation",
          "config": {
            "source_event_key": "nebula.HTTP_DYNAMIC.c_ip",
            "op": "!belong",
            "location_type": "city",
            "location_string": ["上海市"
            ]
           },
          "type": "func"
      },
      "scope": "profile"
    }
    """
get_getlocation_term = lambda: Term.from_json(getlocation_term)


def test_getlocation_term_success():
    check_term(get_getlocation_term(), ['nebula', 'HTTP_DYNAMIC'], '')


def test_getlocation_term_fail_null_location_value():
    # trigger event
    term = get_getlocation_term()

    term.left.location_value = []
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>getlocation缺乏参数配置'

    term = get_getlocation_term()
    term.left.location_value = [None]
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>getlocation缺乏参数配置'


def test_getlocation_term_fail_invalid_event():
    term = get_getlocation_term()
    term.left.source_event_field = 'nebula.not_exist.c_ip'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>事件(not_exist)定义配置不存在'


def test_getlocation_term_fail_invalid_field():
    term = get_getlocation_term()
    term.left.source_event_field = 'nebula.HTTP_DYNAMIC.not_exist'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'


def test_getlocation_term_fail_invalid_location_type():
    term = get_getlocation_term()
    term.left.location_type = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(invalid)不是正确的类型'


def test_getlocation_term_fail_invalid_op():
    term = get_getlocation_term()
    term.left.op = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>(invalid)不是正确的操作符'


def test_getlocation_term_fail_invalid_location_value():
    term = get_getlocation_term()
    term.left.location_value = ['not_exist']
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>事件(HTTP_DYNAMIC)不包含字段(not_exist)'


def test_getlocation_term_fail_multiple_location_value():
    term = get_getlocation_term()
    term.left.op = '='
    term.left.location_value = ['c_ip', 'did']
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>等于操作只支持一个变量'


def test_getlocation_term_fail_invalid_field_type():
    term = get_getlocation_term()
    term.left.source_event_field = 'nebula.HTTP_DYNAMIC.status'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>getlocation的来源字段(status)不是字符串类型'


def test_getlocation_term_fail_inconsistent_trigger():
    term = get_getlocation_term()
    term.left.source_event_field = 'nebula.ACCOUNT_LOGIN.c_ip'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'HTTP_DYNAMIC'], '')

    assert excinfo.value.message == '左表达式>>getlocation的来源事件(ACCOUNT_LOGIN)与触发事件(HTTP_DYNAMIC)不一致'


general_term = """ {
      "remark": "单IP5分钟内登录请求次数大于1",
      "op": ">",
      "right": {
        "subtype": "",
        "config": {
          "value": "1"
        },
        "type": "constant"
      },
      "left": {
        "subtype": "getvariable",
        "config": {
          "variable": [
            "nebula",
            "ip__account_login_count__5m__rt"
          ],
          "trigger": {
            "keys": [
              "c_ip"
            ],
            "event": [
              "nebula",
              "ACCOUNT_LOGIN"
            ]
          }
        },
        "type": "func"
      },
      "scope": "realtime"
    }
"""
get_general_term = lambda: Term.from_json(general_term)


def test_general_term_success():
    check_term(get_general_term(), ['nebula', 'ACCOUNT_LOGIN'], '')


def test_general_term_fail_null_remark():
    term = get_general_term()
    term.remark = None

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '条款描述为空'


def test_general_term_fail_wrong_scope():
    term = get_general_term()
    term.scope = 'invalid'

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '条款的适用类型错误'


def test_general_term_fail_wrong_op():
    term = get_general_term()
    term.op = 'regex'

    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    print excinfo.value.message
    assert excinfo.value.message == '类型(long)不支持操作(匹配正则)'


def test_general_term_fail_wrong_number():
    term = get_general_term()
    term.op = '=='
    term.right.value = 'a'
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '(a)不是合理的数字'


def test_general_term_fail_wrong_regex():
    term = get_event_term()
    term.left.field = 'uri_stem'
    term.op = 'regex'
    term.right.value = '('
    with pytest.raises(RuntimeError) as excinfo:
        check_term(term, ['nebula', 'ACCOUNT_LOGIN'], '')
    assert excinfo.value.message == '(()不是合理的正则表达式'
