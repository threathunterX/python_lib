#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..util import unicode_obj, unicode_list
from ..property_condition import PropertyCondition
from ..property_mapping import PropertyMapping, DirectPropertyMapping, VariableValuePropertyMapping

__author__ = 'lw'

class VarCollectorVariableMeta(VariableMeta):

    TYPE = "varcollector"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = VarCollectorVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._trigger = unicode_list(kwargs["trigger"])
        self._window = long(kwargs["window"])
        self._condition = PropertyCondition.from_dict(kwargs["condition"])
        self._mappings = [PropertyMapping.from_dict(_) for _ in kwargs.get("mappings", [])]
        self._onlyValueNeeded = bool(kwargs["onlyValueNeeded"])
        self._collectedVars = unicode_obj(kwargs["collectedVars"] or [])
        self._strategyName = unicode_obj(kwargs["strategyName"] or "")

        if not self.src_variablesid:
            srcVariables = [_["collectedVar"] for _ in self.collectedVars]
            if self.trigger not in srcVariables:
                srcVariables.append(self.trigger)
            self.src_variablesid = srcVariables

        self._validate()

    def _validate(self):
        if not self.properties:
            self.properties = self.extractProperties()

    @property
    def propertyMappings(self):
        result = []
        result.extend(self.mappings or [])
        thisid = [self.app, self.name]

        for srcVar in self.src_variablesid:
            if self.onlyValueNeeded:
                srcProperty = Property(srcVar, "value", "double").get_dict()
                destProperty = Property(thisid, srcVar[1], "double").get_dict()
                result.append(DirectPropertyMapping(srcProperty=srcProperty, destProperty=destProperty))
            else:
                destProperty = Property(thisid, srcVar[1], "object").get_dict()
                result.append(VariableValuePropertyMapping(srcIdentifier=srcVar, destProperty=destProperty))

        return result

    @property
    def propertyReductions(self):
        return []

    @property
    def propertyCondition(self):
        return self.condition

    @property
    def trigger(self):
        return self._trigger

    @trigger.setter
    def trigger(self, trigger):
        self._trigger = unicode_list(trigger)

    @property
    def window(self):
        return self._window

    @window.setter
    def window(self, window):
        self._window = long(window)

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        if isinstance(condition, dict):
            condition = PropertyCondition.from_dict(condition)

        self._condition = condition

    @property
    def mappings(self):
        return self._mappings or []

    @mappings.setter
    def mappings(self, mappings):
        if mappings and isinstance(mappings[0], dict):
            mappings = [PropertyMapping.from_dict(_) for _ in mappings]
        self._mappings = mappings

    @property
    def onlyValueNeeded(self):
        return self._onlyValueNeeded

    @onlyValueNeeded.setter
    def onlyValueNeeded(self, onlyValueNeeded):
        self._onlyValueNeeded = bool(onlyValueNeeded)

    @property
    def collectedVars(self):
        return self._collectedVars

    @collectedVars.setter
    def collectedVars(self, collectedVars):
        self._collectedVars = unicode_obj(collectedVars)

    @property
    def strategyName(self):
        return self._strategyName

    @strategyName.setter
    def strategyName(self, strategyName):
        self._strategyName = unicode_obj(strategyName)

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "trigger": self.trigger,
            "window": self.window,
            "onlyValueNeeded": self.onlyValueNeeded,
            "condition": self.condition.get_dict() if self.condition else None,
            "mappings": [_.get_dict() for _ in self.mappings],
            "collectedVars": self.collectedVars,
            "strategyName": self.strategyName,
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["trigger", "window", "onlyValueNeeded", "condition", "collectedVars", "mappings", "strategyName"]:
            kwargs[key] = config[key]
        return VarCollectorVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return VarCollectorVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return VarCollectorVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "VarCollectorVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
