#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
列举condition的操作符、中文名和可支持的类型
"""


condition_opertaions = [
    (">", u"大于", {"string", "number"}),
    ("<", u"小于", {"string", "number"}),
    (">=", u"大于等于", {"string", "number"}),
    ("<=", u"小于等于", {"string", "number"}),
    ("==", u"等于", {"string", "number"}),
    ("!=", u"不等于", {"string", "number"}),
    ("between", u"介于", {"number"}),
    ("in", u"属于", {"string", "number"}),
    ("!in", u"不属于", {"string", "number"}),
    ("contain", u"包含", {"string"}),
    ("!contain", u"不包含", {"string"}),
    ("startwith", u"以..开始", {"string"}),
    ("!startwith", u"不以..开始", {"string"}),
    ("endwith", u"以..结束", {"string"}),
    ("!endwith", u"不以..结束", {"string"}),
    ("regex", u"匹配正则", {"string"}),
    ("!regex", u"不匹配正则", {"string"}),
]


expression_to_name_of_operations = {expression: name for expression, name, _ in condition_opertaions}
name_to_expression_of_operations = {name: expression for expression, name, _ in condition_opertaions}

operations_supported_on_string = filter(lambda _: "string" in _[2], condition_opertaions)
operations_supported_on_number = filter(lambda _: "number" in _[2], condition_opertaions)

expressions_supported_on_string = map(lambda _: _[0], operations_supported_on_string)
expressions_supported_on_number = map(lambda _: _[0], operations_supported_on_number)
