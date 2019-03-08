#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pytest import raises
from nebula_meta.variable_model import VariableModel, add_variable_to_registry, update_variables_to_registry, \
    get_all_variables, get_variable_from_registry, get_all_variables_in_dict, add_variables_to_registry
from nebula_meta.event_model import EventModel, add_event_to_registry, update_events_to_registry, get_all_events, \
    get_event_from_registry


def setup():
    # setup event
    update_events_to_registry([])
    update_variables_to_registry([])
    event_dict = {
        'app': 'nebula',
        'name': 'test',
        'visible_name': 'test_event',
        'remark': 'test_event',
        'type': '',
        'version': '1.0',
        'properties': [{
            'name': 'int_field',
            'type': 'long',
            'subtype': '',
            'visible_name': 'int field',
            'remark': 'int field',
        }, {
            'name': 'string_field',
            'type': 'string',
            'subtype': '',
            'visible_name': 'string field',
            'remark': 'string field',
        }, {
            'name': 'uid',
            'type': 'string',
            'subtype': '',
            'visible_name': 'test for uid',
            'remark': 'test for uid',
        }],
    }

    event_model = EventModel.from_dict(event_dict)
    event_model.sanity_check()
    add_event_to_registry(event_model)

    variable_dict = {
        'module': 'base',
        'app': 'nebula',
        'name': 'test',
        'remark': 'test_variable',
        'visible_name': 'test_variable',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'version': '1.0',
    }
    variable_model = VariableModel.from_dict(variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)


def add_test_variables():
    # 添加一个filter和aggregate变量做测试
    filter_variable_dict = {
        'module': 'base',
        'name': 'filter',
        'status': 'enable',
        'type': 'filter',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'filter': {
            'type': 'and',
            'condition': [
                {
                    'type': 'simple',
                    'object': 'int_field',
                    'operation': '>',
                    'value': '5',
                }
            ]
        }
    }
    variable_model = VariableModel.from_dict(filter_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)

    # 自动填充aggregator source
    aggregate_variable_dict = {
        'module': 'base',
        'name': 'aggregate',
        'status': 'enable',
        'type': 'aggregate',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': 'count',
            'object': ''
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(aggregate_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)

    collector_variable_dict = {
        'module': 'base',
        'name': 'collector',
        'status': 'enable',
        'type': 'collector',
        'source': [{
            'app': '',
            'name': 'aggregate'
        }],
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(collector_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)


def test_aggregate_variables():
    aggregate_variable_dict = {
        'module': 'base',
        'name': 'new_aggregate',
        'status': 'enable',
        'type': 'aggregate',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': 'sum',
            'object': 'int_field'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(aggregate_variable_dict)
    variable_model.sanity_check()

    aggregate_variable_dict = {
        'module': 'base',
        'name': 'new_aggregate',
        'status': 'enable',
        'type': 'aggregate',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': 'last',
            'object': 'string_field'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(aggregate_variable_dict)
    variable_model.sanity_check()


def test_other_variables():
    dual_variable_dict = {
        'module': 'base',
        'name': 'dual',
        'status': 'enable',
        'type': 'dual',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }, {
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': '+',
            'object': 'int_field'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(dual_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)

    sequence_variable_dict = {
        'module': 'base',
        'name': 'sequence',
        'status': 'enable',
        'type': 'sequence',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': '+',
            'object': 'int_field'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(sequence_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)

    collector_variable_dict = {
        'module': 'base',
        'name': 'collector',
        'status': 'enable',
        'type': 'collector',
        'source': [{
            'app': '',
            'name': 'sequence'
        }, {
            'app': '',
            'name': 'dual'
        }],
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(collector_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)

    delay_collector_variable_dict = {
        'module': 'base',
        'name': 'collector',
        'status': 'enable',
        'type': 'collector',
        'source': [{
            'app': '',
            'name': 'sequence'
        }, {
            'app': '',
            'name': 'dual'
        }],
        'groupbykeys': ['int_field']
    }
    variable_model = VariableModel.from_dict(delay_collector_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)


def test_fields_mapping():
    add_test_variables()
    assert get_event_from_registry('nebula', 'test').get_fields_mapping() == {
        'int_field': ('long', ''),
        'string_field': ('string', ''),
        'id': ('string', ''),
        'pid': ('string', ''),
        'timestamp': ('long', ''),
        'uid': ('string', ''),
    }

    assert get_variable_from_registry('nebula', 'test').get_fields_mapping() == {
        'int_field': ('long', ''),
        'string_field': ('string', ''),
        'id': ('string', ''),
        'pid': ('string', ''),
        'timestamp': ('long', ''),
        'uid': ('string', ''),
    }

    assert get_variable_from_registry('', 'filter').get_fields_mapping() == {
        'int_field': ('long', ''),
        'string_field': ('string', ''),
        'id': ('string', ''),
        'pid': ('string', ''),
        'timestamp': ('long', ''),
        'uid': ('string', ''),
    }

    assert get_variable_from_registry('', 'aggregate').get_fields_mapping() == {
        'int_field': ('long', ''),
        'value': ('long', ''),
    }

    assert get_variable_from_registry('', 'collector').get_fields_mapping() == {
        'int_field': ('long', ''),
        'aggregate': ('long', ''),
    }


def test_valid_variable():
    variable_dict = {
        'module': 'base',
        'app': 'nebula',
        'name': 'test',
        'remark': 'test_variable',
        'visible_name': 'test_variable',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'version': '1.0',
    }
    variable_model = VariableModel.from_dict(variable_dict)
    variable_model.sanity_check()


def test_source_fixing():
    # 自动填充filter source
    add_test_variables()
    assert get_variable_from_registry('', 'filter').filter.condition[0].source == 'test'
    assert get_variable_from_registry('', 'aggregate').function.source == 'test'


def test_simplified_variable():
    """
    variable的简化形式
    :return:
    """

    variable_dict = {
        'module': 'base',
        'name': 'test',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
    }
    variable_model = VariableModel.from_dict(variable_dict)
    variable_model.sanity_check()


def test_invalid_variable():
    """
    各种variable的非法形式
    :return:
    """

    variable_dict = {
        'module': 'base',
        'name': '',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
    }
    with raises(RuntimeError):
        variable_model = VariableModel.from_dict(variable_dict)
        variable_model.sanity_check()

    variable_dict = {
        'module': 'base',
        'name': 'test',
        'status': '',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
    }
    with raises(RuntimeError):
        variable_model = VariableModel.from_dict(variable_dict)
        variable_model.sanity_check()

    variable_dict = {
        'module': 'base',
        'name': 'test',
        'status': 'enable',
        'type': 'event',
    }
    with raises(RuntimeError):
        variable_model = VariableModel.from_dict(variable_dict)
        variable_model.sanity_check()

    variable_dict = {
        'module': 'base',
        'name': 'test',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'not_exist'
        }],
    }
    with raises(RuntimeError):
        variable_model = VariableModel.from_dict(variable_dict)
        variable_model.sanity_check()


def test_registry():
    update_variables_to_registry([])
    assert ('nebula', 'test') not in get_all_variables_in_dict()

    variable_dict = {
        'module': 'base',
        'app': 'nebula',
        'name': 'test',
        'remark': 'test_variable',
        'visible_name': 'test_variable',
        'status': 'enable',
        'type': 'event',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'version': '1.0',
    }

    variable_model = VariableModel.from_dict(variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)
    assert ('nebula', 'test') in get_all_variables_in_dict()

    update_variables_to_registry([])
    assert ('nebula', 'test') not in get_all_variables_in_dict()
    add_variables_to_registry([variable_model])
    assert ('nebula', 'test') in get_all_variables_in_dict()

    update_variables_to_registry([])
    assert ('nebula', 'test') not in get_all_variables_in_dict()
    update_variables_to_registry([variable_model])
    assert ('nebula', 'test') in get_all_variables_in_dict()

    assert get_all_variables_in_dict()[('nebula', 'test')] == variable_model
    assert get_variable_from_registry('nebula', 'test')
    assert get_all_variables()[0] == variable_model


def test_value_category():
    # 最普通的推断
    aggregate_variable_dict = {
        'module': 'base',
        'app': 'nebula',
        'name': 'aggregate',
        'status': 'enable',
        'type': 'aggregate',
        'source': [{
            'app': 'nebula',
            'name': 'test'
        }],
        'function': {
            'method': 'distinct',
            'object': 'uid'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': []
    }
    variable_model = VariableModel.from_dict(aggregate_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)
    assert variable_model.value_category == 'uid'
    assert variable_model.value_type, variable_model.value_subtype == ('list', 'string')

    # filter 二次继承
    filter_variable_dict = {
        'module': 'base',
        'app': 'nebula',
        'name': 'filter',
        'status': 'enable',
        'type': 'filter',
        'source': [{
            'app': 'nebula',
            'name': 'aggregate'
        }],
        'filter': {
            'type': 'and',
            'condition': []
        }
    }
    variable_model = VariableModel.from_dict(filter_variable_dict)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)
    assert variable_model.value_category == 'uid'
    assert variable_model.value_type, variable_model.value_subtype == ('list', 'string')

    # 衍生变量通过父变量来推断
    aggregate_variable_dict_1 = {
        'module': 'base',
        'app': 'nebula',
        'name': 'aggregate',
        'status': 'enable',
        'type': 'aggregate',
        'source': [{
            'app': 'nebula',
            'name': 'filter'
        }],
        'function': {
            'method': 'distinct',
            'object': 'value'
        },
        'period': {
            'type': 'recently'
        },
        'groupbykeys': []
    }
    variable_model = VariableModel.from_dict(aggregate_variable_dict_1)
    variable_model.sanity_check()
    add_variable_to_registry(variable_model)
    assert variable_model.value_category == 'uid'
