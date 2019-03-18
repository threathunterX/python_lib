#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from .util import text, unicode_dict

__author__ = "nebula"

class Event(object):
    def __init__(self, app, name, key, timestamp, property_values, value=1.0):
        self._app = text(app)
        self._name = text(name)
        self._key = text(key)
        self._timestamp = timestamp
        self._property_values = unicode_dict(property_values)
        self._value = value

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = text(app)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = text(key)

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self._timestamp = timestamp

    @property
    def property_values(self):
        return self._property_values or dict()

    @property_values.setter
    def property_values(self, property_values):
        self._property_values = unicode_dict(property_values)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def get_dict(self):
        result = dict()
        result["app"] = self.app
        result["name"] = self.name
        result["key"] = self.key
        result["timestamp"] = self.timestamp
        result["propertyValues"] = self.property_values
        result["value"] = self.value
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return Event(d.get("app"), d.get("name"), d.get("key"), d.get("timestamp"), d.get("propertyValues"),
                     d.get('value', 1.0))

    @staticmethod
    def from_json(json_str):
        return Event.from_dict(json.loads(json_str))

    def copy(self):
        return Event.from_dict(self.get_dict())

    def __str__(self):
        return "Event[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.app) + hash(self.name) + hash(self.key) + hash(self.timestamp) +  hash(frozenset(self.property_values.items()))


if __name__ == "__main__":
    e = Event("app", "testname", "key", 0L, {"key1": "value", "key2": 2})
    print e.get_json()
    assert e == Event.from_json(e.get_json())
