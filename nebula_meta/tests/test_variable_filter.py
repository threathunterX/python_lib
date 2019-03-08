#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pytest import raises
from nebula_meta.variable_filter import Filter, is_filter_operation_support, normalize_filter_value


def test_variable_filter():
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '==',
        'value': '1',
        'param': '',
        'type': 'simple',
    }

    filter = Filter.from_dict(filter_dict)
    filter.sanity_check()
    assert not filter.is_empty()
    assert filter.get_dict() == filter_dict

    # 空的filter
    filter_dict = {
    }

    filter = Filter.from_dict(filter_dict)
    filter.sanity_check()
    assert filter.is_empty()
    assert filter.get_dict() == filter_dict

    # complex filter
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '==',
        'value': '1',
        'param': '',
        'type': 'simple',
    }
    complex_filter_dict = {
        'type': 'and',
        'condition': [filter_dict, filter_dict]
    }
    complex_filter = Filter.from_dict(complex_filter_dict)
    complex_filter.sanity_check()
    assert not complex_filter.is_empty()
    assert complex_filter.get_dict() == complex_filter_dict


def test_simplified_filter():
    """
    简化的filter
    :return:
    """

    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '',
        'value': '1',
        'type': 'simple',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()


def test_invalid_filter():
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '',
        'value': '1',
        'param': '',
        'condition': [],
        'type': 'simple',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()

    # null object
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': '',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '==',
        'value': '1',
        'param': '',
        'condition': [],
        'type': 'simple',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()

    # invalid type
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'kv',
        'object_subtype': '',
        'operation': '==',
        'value': '1',
        'param': '',
        'condition': [],
        'type': 'simple',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()

    # invalid operation
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '>=',
        'value': '1',
        'param': '',
        'condition': [],
        'type': 'simple',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()

    # invalid type
    filter_dict = {
        'source': 'HTTP_DYNAMIC',
        'object': 'c_ip',
        'object_type': 'string',
        'object_subtype': '',
        'operation': '==',
        'value': '1',
        'param': '',
        'condition': [],
        'type': 'invalid type',
    }

    with raises(RuntimeError):
        filter = Filter.from_dict(filter_dict)
        filter.sanity_check()


def test_operation_support():
    assert is_filter_operation_support(('string', ''), 'contains')
    assert is_filter_operation_support(('string', ''), 'containsby')
    assert is_filter_operation_support(('string', ''), '==')
    assert is_filter_operation_support(('string', ''), 'match')
    assert is_filter_operation_support(('string', ''), 'empty')
    assert not is_filter_operation_support(('string', ''), '>=')

    assert is_filter_operation_support(('long', ''), '>=')
    assert not is_filter_operation_support(('long', ''), 'match')


def test_filter_value_normalization():
    assert normalize_filter_value(('string', ''), '==', '1') == '1'
    assert normalize_filter_value(('string', ''), '==', 1) == '1'
    assert normalize_filter_value(('string', ''), 'locationcontainsby', 'a') == 'a'
    assert normalize_filter_value(('string', ''), 'locationcontainsby', 'a,b') == 'a,b'
    normalize_filter_value(('string', ''), 'match', '.*') == '.*'

    # invalid regex
    with raises(RuntimeError):
        normalize_filter_value(('string', ''), 'match', '\\')

    assert normalize_filter_value(('long', ''), '==', 1) == 1
    assert normalize_filter_value(('long', ''), '==', '1') == 1

    assert normalize_filter_value(('string', ''), 'empty', '1') == ''
    assert normalize_filter_value(('bool', ''), '==', 't')
    assert normalize_filter_value(('bool', ''), '==', 'yes')
    assert not normalize_filter_value(('bool', ''), '==', 'random')

    # invalid operations
    with raises(RuntimeError):
        normalize_filter_value(('string', ''), '>=', '1')
    with raises(RuntimeError):
        normalize_filter_value(('long', ''), 'contain', 1)
    with raises(RuntimeError):
        normalize_filter_value(('invalid', ''), 'contain', 1)

    # invalid filter values
    with raises(RuntimeError):
        normalize_filter_value(('long', ''), '==', 'a')
    with raises(RuntimeError):
        normalize_filter_value(('double', ''), '>=', 'a')

