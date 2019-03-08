#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from ..variable_meta import VariableMeta

__author__ = 'lw'

class SimpleVariableMeta(VariableMeta):

    TYPE = "simple"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = SimpleVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)

    @property
    def propertyMappings(self):
        return []

    @property
    def propertyReductions(self):
        return []

    @property
    def propertyCondition(self):
        return None

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        result["config"] = dict()
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        return SimpleVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return SimpleVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return SimpleVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "SimpleVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
