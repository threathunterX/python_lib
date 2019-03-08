#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import json

from nebula_parser.parser_initializer import build_fn_load_event_schemas_on_file
from nebula_parser.parser_initializer import build_fn_load_parsers_on_file
from nebula_parser.parser_initializer import init_parser
from nebula_parser.eventschema import get_event_schema
from nebula_parser.autoparser import get_current_generators, test_parsers


parser_data = \
{
    "status": 1,
    "remark": "",
    "terms": {
        "then": [
            {
                "tar_col": "login_channel",
                "op_string": "11",
                "src_col": "",
                "op": "set"
            },
            {
                "tar_col": "remember_me",
                "op_string": "f",
                "src_col": "",
                "op": "set"
            },
            {
                "tar_col": "uid",
                "op_string": [
                    {
                        "op_string": [
                            {
                                "op_string": "1",
                                "src_col": "did",
                                "op": "!contain"
                            }
                        ],
                        "op_value": "1",
                        "src_col": "",
                        "op": ""
                    },
                    {
                        "op_value": "33",
                        "op": "default"
                    }
                ],
                "src_col": "",
                "op": "switch"
            }
        ],
        "when": [
            {
                "op_string": "POST",
                "src_col": "method",
                "op": "=="
            },
            {
                "op_string": "kvcollect",
                "src_col": "uri_stem",
                "op": "contain"
            }
        ]
    },
    "url": "kvcollect",
    "dest": "ACCOUNT_LOGIN",
    "source": "HTTP_DYNAMIC",
    "host": "",
    "id": 4
}
get_test_data = lambda: json.loads(json.dumps(parser_data))


def get_local_path(file_name):
    import os
    return os.path.join(os.path.dirname(__file__), file_name)


def setup_module(module):
    print ("start testing")
    fn_load_schema = build_fn_load_event_schemas_on_file(get_local_path("event_schema.json"))
    fn_load_parsers = build_fn_load_parsers_on_file(get_local_path("parser.json"))
    init_parser(fn_load_schema, fn_load_parsers)


def teardown_module(module):
    print ("finish testing")


def test_success_load():
    try:
        print get_event_schema("HTTP_DYNAMIC")
        print test_parsers()
        print test_parsers([get_test_data()])
        print get_current_generators()[0]
    except Exception as err:
        print err


def test_invalid_event():
    data = get_test_data()
    data["source"] = "unknown"
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'源日志(unknown)定义配置不存在'

    data = get_test_data()
    data["dest"] = "unknown"
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'转化日志(unknown)定义配置不存在'


def test_invalid_condition():
    data = get_test_data()
    data["terms"]["when"].append(
        {
            "op_string": "POST",
            "src_col": "c_bytes",
            "op": "contain"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'字段(c_bytes)不支持操作(包含)'

    data = get_test_data()
    data["terms"]["when"].append(
        {
            "op_string": "POST",
            "src_col": "method",
            "op": "between"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'字段(method)不支持操作(介于)'

    data = get_test_data()
    data["terms"]["when"].append(
        {
            "op_string": "invalid number",
            "src_col": "c_bytes",
            "op": "!="
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'(invalid number)不是合理的数字'

    data = get_test_data()
    data["terms"]["when"].append(
        {
            "op_string": "[",
            "src_col": "method",
            "op": "regex"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'([)不是合理的正则表达式'

    data = get_test_data()
    data["terms"]["when"].append(
        {
            "op_string": "aa",
            "src_col": "method1",
            "op": "regex"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'源日志(HTTP_DYNAMIC)不包含字段(method1)'


def test_invalid_then():
    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "s_bytes",
            "op_string": "aa",
            "src_col": "",
            "op": "set"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'(aa)不是合理的数字'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "tttt",
            "op_string": "aa",
            "src_col": "",
            "op": "set"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'转化日志(ACCOUNT_LOGIN)不包含字段(tttt)'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "s_bytes",
            "op_string": {
                "extract_context": "",
                "extract_type": "json",
                "extract_op": "self"
            },
            "src_col": "method",
            "op": "extract_set"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'(HTTP_DYNAMIC.method)由于类型原因不能转化为(ACCOUNT_LOGIN.s_bytes)'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "ttt",
            "op_string": {
                "extract_context": "",
                "extract_type": "json",
                "extract_op": "self"
            },
            "src_col": "method",
            "op": "extract_set"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'转化日志(ACCOUNT_LOGIN)不包含字段(ttt)'


def test_invalid_switch():
    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "method",
            "op_string": [
                {
                    "op_string": [
                        {
                            "op_string": "1",
                            "src_col": "s_bytes",
                            "op": "!contain"
                        }
                    ],
                    "op_value": "1",
                    "src_col": "",
                    "op": ""
                },
                {
                    "op_value": "33",
                    "op": "default"
                }
            ],
            "src_col": "",
            "op": "switch"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'字段(s_bytes)不支持操作(不包含)'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "s_bytes",
            "op_string": [
                {
                    "op_string": [
                        {
                            "op_string": "1",
                            "src_col": "method",
                            "op": "!contain"
                        }
                    ],
                    "op_value": "aa",
                    "src_col": "",
                    "op": ""
                },
                {
                    "op_value": "33",
                    "op": "default"
                }
            ],
            "src_col": "",
            "op": "switch"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'(aa)不是合理的数字'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "s_bytes",
            "op_string": [
                {
                    "op_string": [
                        {
                            "op_string": "1",
                            "src_col": "method",
                            "op": "!contain"
                        }
                    ],
                    "op_value": "1",
                    "src_col": "",
                    "op": ""
                },
                {
                    "op_value": "33a",
                    "op": "default"
                }
            ],
            "src_col": "",
            "op": "switch"
        }
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'(33a)不是合理的数字'


def test_invalid_extract_set():
    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "uid",
            "op_string": {
                "extract_context": "ttt",
                "extract_type": "json",
                "extract_op": "self"
            },
            "src_col": "aaa",
            "op": "extract_set"
        },
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'源日志(HTTP_DYNAMIC)不包含字段(aaa)'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "uid",
            "op_string": {
                "extract_context": "ttt",
                "extract_type": "json",
                "extract_op": "self"
            },
            "src_col": "method",
            "op": "extract_set"
        },
    )
    with pytest.raises(RuntimeError) as excinfo:
        test_parsers([data])
    assert excinfo.value.message == u'字段(method)不支持变量抽取'

    data = get_test_data()
    data["terms"]["then"].append(
        {
            "tar_col": "uid",
            "op_string": {
                "extract_context": "ttt",
                "extract_type": "json",
                "extract_op": "self"
            },
            "src_col": "c_body",
            "op": "extract_set"
        },
    )
    print test_parsers([data])[0]


if __name__ == '__main__':
    pytest.main()

