#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .util import text, unicode_list

__author__ = 'lw'

class Property(object):
    def __init__(self, identifier, name, type):
        self._identifier = unicode_list(identifier or list())
        self._name = text(name)
        self._type = text(type)

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, identifier):
        self._identifier = unicode_list(identifier or list())

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type)

    def get_dict(self):
        return {"identifier": self.identifier, "name": self.name, "type": self.type}

    def get_json(self):
        return json.dumps(self.get_dict)

    @staticmethod
    def from_dict(d):
        if not d:
            return None
        return Property(d.get("identifier"), d.get("name"), d.get("type"))

    @staticmethod
    def from_json(json_str):
        return Property.from_dict(json.loads(json_str))

    def copy(self):
        return Property.from_dict(self.get_dict())

    def __str__(self):
        return "Property[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
