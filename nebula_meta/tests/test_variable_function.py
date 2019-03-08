#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pytest import raises
from nebula_meta.variable_function import Function, is_calculator_support, get_calculator_result_type


def test_variable_function():
    function_dict = {
        'method': 'count',
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'param': '',
        'config': {}
    }

    function = Function.from_dict(function_dict)
    function.sanity_check()
    assert not function.is_empty()
    assert function.get_dict() == function_dict

    # empty function
    function_dict = {
    }

    function = Function.from_dict(function_dict)
    function.sanity_check()
    assert function.is_empty()
    assert function.get_dict() == function_dict


def test_invalid_function():

    function_dict = {
        'method': 'count',
    }

    with raises(RuntimeError):
        function = Function.from_dict(function_dict)
        function.sanity_check()


def test_calculator_support():
    assert is_calculator_support('self', ('long', ''), 'str')
    assert is_calculator_support('self', ('string', ''), 'long')
    assert not is_calculator_support('self', ('string', ''), 'count')
    assert is_calculator_support('self', ('list', 'string'), 'count')
    assert is_calculator_support('self', ('list', 'long'), 'max')
    assert not is_calculator_support('self', ('list', 'string'), 'max')

    assert is_calculator_support('hourly', ('string', ''), 'count')
    assert is_calculator_support('hourly', ('string', ''), 'distinct')
    assert is_calculator_support('hourly', ('string', ''), 'distinct_count')
    assert not is_calculator_support('hourly', ('string', ''), 'sum')


def test_get_calculator_result_type():
    assert get_calculator_result_type('self', ('long', ''), 'str') == ('string', '')
    assert get_calculator_result_type('self', ('string', ''), 'long') == ('long', '')
    assert get_calculator_result_type('self', ('list', 'string'), 'count') == ('long', '')
    assert get_calculator_result_type('self', ('list', 'long'), 'max') == ('long', '')

    assert get_calculator_result_type('hourly', ('string', ''), 'count') == ('map', 'long')
    assert get_calculator_result_type('hourly', ('string', ''), 'distinct') == ('mlist', 'string')
    assert get_calculator_result_type('hourly', ('string', ''), 'distinct_count') == ('map', 'long')

    assert get_calculator_result_type('scope', ('mlist', 'double'), 'sum') == ('double', '')
    assert get_calculator_result_type('scope', ('map', 'long'), 'sum') == ('long', '')
    assert get_calculator_result_type('scope', ('map', 'long'), 'avg') == ('double', '')
    assert get_calculator_result_type('scope', ('mlist', 'string'), 'collection') == ('list', 'string')

    assert get_calculator_result_type('recently', ('long', ''), '+') == ('long', '')
    assert get_calculator_result_type('recently', ('long', ''), 'sum') == ('long', '')
