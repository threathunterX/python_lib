#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from nebula_strategy.generator.condition_gen import *
from nebula_meta.model.term import GetLocationExp, EventFieldExp, ConstantExp

from ..setup_test import *


def setup_module(module):
    print ('start testing')
    load_config()


def teardown_module(module):
    print ('finish testing')


def test_dimension_condition():
    not_null_filter = gen_ordinary_filter('HTTP_DYNAMIC', 'did', '!=', '')
    print not_null_filter


def test_string_ordinary_condition():
    print gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', '!=', 'aa')

    with pytest.raises(RuntimeError) as excinfo:
        print gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'between', 'aa')
    assert excinfo.value.message == 'string类型不支持(between)操作'

    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'in', 'aa')
    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', '!in', 'aa,bb')
    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'contain', 'aa,bb')
    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'startwith', 'aa,bb')
    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'endwith', 'aa,bb')
    gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', 'regex', 'aa,bb')

    with pytest.raises(RuntimeError) as excinfo:
        gen_ordinary_filter('HTTP_DYNAMIC', 'c_ip', '!in', 'aa,' * 11)
    assert excinfo.value.message == '属于最多支持10个属性'


def test_long_ordinary_condition():
    gen_ordinary_filter('HTTP_DYNAMIC', 'status', '!=', '5')

    with pytest.raises(RuntimeError) as excinfo:
        print gen_ordinary_filter('HTTP_DYNAMIC', 'status', 'regex', '5')
    assert excinfo.value.message == '数字类型不支持(regex)操作'

    gen_ordinary_filter('HTTP_DYNAMIC', 'status', 'in', '5')
    gen_ordinary_filter('HTTP_DYNAMIC', 'status', '!in', '4,5')
    gen_ordinary_filter('HTTP_DYNAMIC', 'status', 'between', '4,5')
    gen_ordinary_filter('HTTP_DYNAMIC', 'status', '>=', '5')

    with pytest.raises(RuntimeError) as excinfo:
        gen_ordinary_filter('HTTP_DYNAMIC', 'status', '!in', '5,' * 11)
    assert excinfo.value.message == '属于最多支持10个属性'

    with pytest.raises(RuntimeError) as excinfo:
        gen_ordinary_filter('HTTP_DYNAMIC', 'status', '>=', 'aa')
    assert excinfo.value.message == '(aa)不是数字'


def test_double_ordinary_condition():
    gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', '!=', '5')

    with pytest.raises(RuntimeError) as excinfo:
        print gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', 'regex', '5')
    assert excinfo.value.message == '数字类型不支持(regex)操作'

    gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', 'in', '5')
    gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', '!in', '4,5')
    gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', 'between', '4,5')
    gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', '>=', '5')

    with pytest.raises(RuntimeError) as excinfo:
        gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', '!in', '5,' * 11)
    assert excinfo.value.message == '属于最多支持10个属性'

    with pytest.raises(RuntimeError) as excinfo:
        gen_ordinary_filter('TRANSACTION_DEPOSIT', 'account_balance_before', '>=', 'aa')
    assert excinfo.value.message == '(aa)不是数字'


def test_condition_from_location_exp():
    get_location_exp = lambda: GetLocationExp.from_dict(
        {
            'type': 'func',
            'subtype': 'getlocation',
            'config': {
                'source_event_key': 'nebula.HTTP_DYNAMIC.c_ip',
                'op': '!belong',
                'location_type': 'city',
                'location_string': ['c_ip']
            }
        }
    )

    location_exp = get_location_exp()
    gen_filter_from_location_exp(location_exp, ['nebula', 'HTTP_DYNAMIC'])

    location_exp = get_location_exp()
    location_exp.source_event_field = 'nebula.invalid.c_ip'
    with pytest.raises(RuntimeError) as excinfo:
        gen_filter_from_location_exp(location_exp, ['nebula', 'HTTP_DYNAMIC'])
    assert excinfo.value.message == '地理位置参数不正确'

    location_exp = get_location_exp()
    location_exp.op = 'invalid'
    with pytest.raises(RuntimeError) as excinfo:
        gen_filter_from_location_exp(location_exp, ['nebula', 'HTTP_DYNAMIC'])
    assert excinfo.value.message == '操作不正确'

    location_exp = get_location_exp()
    location_exp.location_type = 'province'
    location_exp.location_value = ['c_ip', '上海市']
    gen_filter_from_location_exp(location_exp, ['nebula', 'HTTP_DYNAMIC'])

    location_exp = get_location_exp()
    location_exp.location_type = 'province'
    location_exp.op = '='
    location_exp.location_value = ['c_ip']
    gen_filter_from_location_exp(location_exp, ['nebula', 'HTTP_DYNAMIC'])


def test_condition_from_event_exp():
    get_event_exp = lambda: EventFieldExp.from_dict(
        {
            'type': 'event',
            'subtype': '',
            'config': {
                'event': ['nebula', 'HTTP_DYNAMIC'],
                'field': 'c_ip'
            }
        }
    )

    get_constant_exp = lambda: ConstantExp.from_dict(
        {
            'type': 'constant',
            'subtype': '',
            'config': {
                'value': '1'
            }
        }

    )

    event_exp = get_event_exp()
    constant_exp = get_constant_exp()
    gen_filter_from_event_exp(event_exp, '==', constant_exp)

    with pytest.raises(RuntimeError) as excinfo:
        gen_filter_from_event_exp(event_exp, '>', constant_exp)
    assert excinfo.value.message == 'string类型不支持(>)操作'

    event_exp = get_event_exp()
    event_exp.field = 'status'
    constant_exp = get_constant_exp()
    with pytest.raises(RuntimeError) as excinfo:
        gen_filter_from_event_exp(event_exp, 'regex', constant_exp)
    assert excinfo.value.message == '数字类型不支持(regex)操作'

    event_exp = get_event_exp()
    event_exp.field = 'status'
    constant_exp = get_constant_exp()
    constant_exp.value = 'a'
    with pytest.raises(RuntimeError) as excinfo:
        gen_filter_from_event_exp(event_exp, '>', constant_exp)
    assert excinfo.value.message == '(a)不是数字'

