#!/usr/bin/env python
# -*- coding: utf-8 -*-


from nebula_meta.value_type import get_supported_types
from nebula_meta.value_type import is_value_valid, convert_value_by_type, is_value_type_supported


def test_support_types():
    assert 'long' in get_supported_types()
    assert 'double' in get_supported_types()
    assert 'string' in get_supported_types()
    assert 'bool' in get_supported_types()
    assert 'int' not in get_supported_types()


def test_support_value_type():
    assert is_value_type_supported('long', '')
    assert is_value_type_supported('bool', '')
    assert is_value_type_supported('string', '')
    assert is_value_type_supported('double', '')
    assert is_value_type_supported('list', 'string')
    assert is_value_type_supported('list', 'long')
    assert is_value_type_supported('list', 'double')
    assert is_value_type_supported('list', 'kv')
    assert is_value_type_supported('mlist', 'string')
    assert is_value_type_supported('mlist', 'long')
    assert is_value_type_supported('mlist', 'double')
    assert is_value_type_supported('mlist', 'kv')
    assert is_value_type_supported('map', 'string')
    assert is_value_type_supported('map', 'long')
    assert is_value_type_supported('map', 'double')
    assert is_value_type_supported('mmap', 'string')
    assert is_value_type_supported('mmap', 'long')
    assert is_value_type_supported('mmap', 'double')
    assert is_value_type_supported('mmap', 'list')

    assert not is_value_type_supported('kv', '')
    assert not is_value_type_supported('map', 'map')
    assert not is_value_type_supported('map', 'kv')


def test_valid_value():
    assert is_value_valid('long', '', 1)
    assert is_value_valid('long', '', '1')
    assert not is_value_valid('long', '', 's')

    assert is_value_valid('double', '', 1)
    assert is_value_valid('double', '', '1')
    assert not is_value_valid('double', '', 's')

    assert is_value_valid('bool', '', True)
    assert is_value_valid('bool', '', 'true')
    assert is_value_valid('bool', '', 'True')
    assert is_value_valid('bool', '', 's'), 'invalid value means false'

    assert is_value_valid('list', 'long', [1, 2, 3])
    assert is_value_valid('list', 'long', '[1, 2, 3]')
    assert not is_value_valid('list', 'long', ['a', 2, 3])
    assert not is_value_valid('list', 'long', [1, [1,2], 3])
    assert not is_value_valid('list', 'long', '["a", 2, 3]')
    assert is_value_valid('list', 'double', [1, 2, 3])
    assert is_value_valid('list', 'double', '[1, 2, 3]')
    assert not is_value_valid('list', 'double', ['a', 2, 3])
    assert not is_value_valid('list', 'double', '["a", 2, 3]')
    assert not is_value_valid('list', 'double', 1)

    assert is_value_valid('map', 'long', {'k1': 1, 'k2': 2})
    assert is_value_valid('map', 'long', '{"k1": 1, "k2": 2}')
    assert not is_value_valid('map', 'long', {'k1': "a", 'k2': 2})
    assert not is_value_valid('map', 'long', '{"k1": "a", "k2": 2}')
    assert not is_value_valid('map', 'long', 1)

    assert is_value_valid('mlist', 'long', {'k1': [1, 2, 3], 'k2': []})
    assert is_value_valid('mlist', 'long', '{"k1": [1,2,3], "k2": []}')
    assert not is_value_valid('mlist', 'long', {'k1': ["a", 2, 3], 'k2': []})
    assert not is_value_valid('mlist', 'long', '{"k1": ["a",2,3], "k2": []}')

    assert is_value_valid('mmap', 'long', {'k1': {'ik1': 1, 'ik2': 2}, 'k2': {}})
    assert is_value_valid('mmap', 'long', '{"k1": {"ik1": 1, "ik2": 2}, "k2": {}}')
    assert not is_value_valid('mmap', 'long', {'k1': {'ik1': 'a', 'ik2': 2}, 'k2': {}})
    assert not is_value_valid('mmap', 'long', '{"k1": {"ik1": "a", "ik2": 2}, "k2": {}}')
    assert not is_value_valid('mmap', 'long', 1)
    assert not is_value_valid('mmap', 'long', {'k1': 1})

    assert is_value_valid('list', 'kv', [{'k': 'key', 'v': 1}])
    assert is_value_valid('list', 'kv', '[{"k": "key", "v": 1}]')
    assert not is_value_valid('list', 'kv', [{'v': 1}])
    assert not is_value_valid('list', 'kv', [{'k': 'key'}])
    assert not is_value_valid('list', 'kv', [1])

    assert is_value_valid('mlist', 'kv', {'1111': [{'k': 'key', 'v': 1}]})
    assert is_value_valid('mlist', 'kv', '{"1111": [{"k": "key", "v": 1}]}')
    assert not is_value_valid('mlist', 'kv', {'1111': [{'k': 'key'}]})
    assert not is_value_valid('mlist', 'kv', {'1111': [{'v': 'value'}]})
    assert not is_value_valid('mlist', 'kv', [1, 2, 3])
    assert not is_value_valid('mlist', 'kv', {'1111': {'v': 'value'}})

    assert not is_value_valid('boolean', '', 'true')
    assert not is_value_valid('map', 'map', {'k': {'ik': 1}})


def test_normalize_value():
    assert convert_value_by_type('long', '', 1) == 1
    assert convert_value_by_type('long', '', '1') == 1

    assert convert_value_by_type('double', '', 1) == 1
    assert convert_value_by_type('double', '', '1') == 1

    assert convert_value_by_type('bool', '', True)
    assert convert_value_by_type('bool', '', 'true')
    assert convert_value_by_type('bool', '', 'True')
    assert not convert_value_by_type('bool', '', 'false')
    assert not convert_value_by_type('bool', '', 's')

    assert convert_value_by_type('list', 'long', [1, 2, 3]) == [1, 2, 3]
    assert convert_value_by_type('list', 'long', '[1, 2, 3]') == [1, 2, 3]
    assert convert_value_by_type('list', 'double', [1, 2, 3]) == [1, 2, 3]
    assert convert_value_by_type('list', 'double', '[1, 2, 3]') == [1, 2, 3]
    assert convert_value_by_type('list', 'string', [1, 2, 3]) == ['1', '2', '3']
    assert convert_value_by_type('list', 'string', '[1, 2, 3]') == ['1', '2', '3']

    assert convert_value_by_type('map', 'long', {'k1': 1, 'k2': 2}) == {'k1': 1, 'k2': 2}
    assert convert_value_by_type('map', 'long', '{"k1": 1, "k2": 2}') == {'k1': 1, 'k2': 2}
    assert convert_value_by_type('map', 'string', {'k1': '1', 'k2': '2'}) == {'k1': '1', 'k2': '2'}
    assert convert_value_by_type('map', 'string', '{"k1": "1", "k2": "2"}') == {'k1': '1', 'k2': '2'}
    assert convert_value_by_type('map', 'string', '{"k1": "1", "k2": "2"}') == {'k1': '1', 'k2': '2'}
    assert convert_value_by_type('map', 'string', '{"k1": 1, "k2": 2}') == {'k1': '1', 'k2': '2'}

    assert convert_value_by_type('mlist', 'long', {'k1': [1, 2, 3], 'k2': []}) == {'k1': [1, 2, 3], 'k2': []}
    assert convert_value_by_type('mlist', 'long', '{"k1": [1,2,3], "k2": []}') == {'k1': [1, 2, 3], 'k2': []}

    assert convert_value_by_type('mmap', 'long', {'k1': {'ik1': 1, 'ik2': 2}, 'k2': {}}) == \
           {'k1': {'ik1': 1, 'ik2': 2}, 'k2': {}}
    assert convert_value_by_type('mmap', 'long', '{"k1": {"ik1": 1, "ik2": 2}, "k2": {}}') == \
           {'k1': {'ik1': 1, 'ik2': 2}, 'k2': {}}



