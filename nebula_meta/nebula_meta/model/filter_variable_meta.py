#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..property_condition import PropertyCondition
from ..property_mapping import PropertyMapping, DirectPropertyMapping


__author__ = 'lw'


class FilterVariableMeta(VariableMeta):

    TYPE = "filter"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = FilterVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._mappings = [PropertyMapping.from_dict(_) for _ in kwargs["mappings"]]
        self._condition = PropertyCondition.from_dict(kwargs["condition"])
        self._validate()

    def _validate(self):
        # mapping all the src properties if there is no given yet
        srcVariableID = self.src_variablesid[0]
        srcVariable = VariableMeta.find_variable_meta_by_id(srcVariableID[0], srcVariableID[1])
        thisid = [self.app, self.name]
        if not self._mappings:
            self._mappings = []
            for p in srcVariable.properties:
                destProperty = Property(thisid, p.name, p.type).get_dict()
                self._mappings.append(DirectPropertyMapping(srcProperty=p.get_dict(), destProperty=destProperty))

        if not self.properties:
            self.properties = self.extractProperties()

    @property
    def propertyMappings(self):
        return self.genMappingsFromVariableData(self.mappings, [], [], [])

    @property
    def propertyReductions(self):
        return []

    @property
    def propertyCondition(self):
        return self.condition

    @property
    def mappings(self):
        return self._mappings

    @mappings.setter
    def mappings(self, mappings):
        if mappings and isinstance(mappings[0], dict):
            mappings = [PropertyMapping.from_dict(_) for _ in mappings]
        self._mappings = mappings

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        if isinstance(condition, dict):
            condition = PropertyCondition.from_dict(condition)

        self._condition = condition

    def addPropertiesFromSource(self):
        srcVariableID = self.src_variablesid[0]
        srcVariable = VariableMeta.find_variable_meta_by_id(srcVariableID[0], srcVariableID[1])
        thisid = [self.app, self.name]

        self._mappings = self._mappings or []

        destpropertynames = [_.destProperty.name for _ in self._mappings]
        for p in srcVariable.properties:
            if p.name in destpropertynames:
                continue

            destProperty = Property(thisid, p.name, p.type)
            newMapping = DirectPropertyMapping(srcProperty=p.get_dict(), destProperty=destProperty.get_dict())
            if newMapping not in self._mappings:
                self._mappings.append(newMapping)
            if destProperty not in self._properties:
                self._properties.append(destProperty)

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "mappings": [_.get_dict() for _ in self.mappings],
            "condition": self.condition.get_dict(),
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["mappings", "condition"]:
            kwargs[key] = config[key]
        return FilterVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return FilterVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return FilterVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "FilterVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
