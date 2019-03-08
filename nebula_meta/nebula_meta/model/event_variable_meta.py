#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..variable_meta import VariableMeta

__author__ = 'lw'


class EventVariableMeta(VariableMeta):

    TYPE = "event"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = EventVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._validate()

    def _validate(self):
        pass

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
        result["config"] = {}
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        return EventVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return EventVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return EventVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "EventVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
