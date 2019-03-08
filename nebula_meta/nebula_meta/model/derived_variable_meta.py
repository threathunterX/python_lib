#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..util import unicode_dict

from ..variable_meta import VariableMeta


class DerivedVariableMeta(VariableMeta):

    TYPE = "derived"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = DerivedVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._config = unicode_dict(kwargs['config'])

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        result["config"] = self.config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        return DerivedVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return DerivedVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return DerivedVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "DerivedVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
