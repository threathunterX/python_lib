#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pytest import raises
from nebula_meta.event_model import EventModel
from nebula_meta.event_model import add_event_to_registry, add_events_to_registry, update_events_to_registry, \
    get_all_events, get_all_events_in_dict, get_event_from_registry


def test_valid_event():
    event_dict = {
        'app': 'nebula',
        'name': 'test',
        'visible_name': 'test_event',
        'remark': 'test_event',
        'source': [],
        'type': '',
        'version': '1.0',
        'properties': [{
            'name': 'property1',
            'type': 'long',
            'subtype': '',
            'visible_name': 'property 1',
            'remark': 'property 1',
        }
        ],
    }

    event_model = EventModel.from_dict(event_dict)
    event_model.sanity_check()
    assert event_dict == event_model.get_dict()


def test_simplified_event():
    """
    event 的简化形式
    :return:
    """

    event_dict = {
        'name': 'test'
    }

    event_model = EventModel.from_dict(event_dict)
    event_model.sanity_check()
    assert event_model.get_dict() == {
        'app': '',
        'name': 'test',
        'visible_name': '',
        'remark': '',
        'source': [],
        'type': '',
        'version': '',
        'properties': [],
    }, '最简单event，其他字段空值自动填补'

    event_dict = {
        'name': 'test',
        'properties': [{
            'name': 'property1',
            'type': 'long'
        }],
    }

    event_model = EventModel.from_dict(event_dict)
    event_model.sanity_check()
    assert event_model.get_dict() == {
        'app': '',
        'name': 'test',
        'visible_name': '',
        'remark': '',
        'source': [],
        'type': '',
        'version': '',
        'properties': [{
            'name': 'property1',
            'type': 'long',
            'subtype': '',
            'visible_name': '',
            'remark': '',
        }],
    }, 'property最简单形式'


def test_invalid_event():
    """
    各种event的非法形式
    :return:
    """

    # invalid event name
    event_dict = {
        'name': ''
    }
    event_model = EventModel.from_dict(event_dict)
    with raises(RuntimeError):
        event_model.sanity_check()

    # invalid property name
    event_dict = {
        'name': 'test',
        'properties': [{
            'name': '',
            'type': 'long'
        }],
    }
    with raises(RuntimeError):
        event_model = EventModel.from_dict(event_dict)
        event_model.sanity_check()

    # invalid property type
    event_dict = {
        'name': 'test',
        'properties': [{
            'name': 'property1',
            'type': 'ttt'
        }],
    }
    with raises(RuntimeError):
        event_model = EventModel.from_dict(event_dict)
        event_model.sanity_check()

    # invalid property data structure
    event_dict = {
        'name': 'test',
        'properties': [[1, 2]],
    }
    with raises(RuntimeError):
        event_model = EventModel.from_dict(event_dict)
        event_model.sanity_check()


def test_registry():
    assert ('nebula', 'test') not in get_all_events_in_dict()

    event_dict = {
        'app': 'nebula',
        'name': 'test',
        'visible_name': 'test_event',
        'remark': 'test_event',
        'type': '',
        'version': '1.0',
        'properties': [{
            'name': 'property1',
            'type': 'long',
            'subtype': '',
            'visible_name': 'property 1',
            'remark': 'property 1',
        }
        ],
    }

    event_model = EventModel.from_dict(event_dict)
    event_model.sanity_check()
    add_event_to_registry(event_model)
    assert ('nebula', 'test') in get_all_events_in_dict()

    update_events_to_registry([])
    assert ('nebula', 'test') not in get_all_events_in_dict()
    add_events_to_registry([event_model])
    assert ('nebula', 'test') in get_all_events_in_dict()

    update_events_to_registry([])
    assert ('nebula', 'test') not in get_all_events_in_dict()
    update_events_to_registry([event_model])
    assert ('nebula', 'test') in get_all_events_in_dict()

    assert get_all_events_in_dict()[('nebula', 'test')] == event_model
    assert get_event_from_registry('nebula', 'test')
    assert get_all_events()[0] == event_model
