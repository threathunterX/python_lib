#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..util import text, unicode_list
from ..property_mapping import DirectPropertyMapping

__author__ = "nebula"

class DualVarOperationVariableMeta(VariableMeta):

    TYPE = "dualvar"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = DualVarOperationVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)

        self._firstVariable = unicode_list(kwargs["firstVariable"] or [])
        self._firstMayBeNull = bool(kwargs["firstMayBeNull"])
        self._secondVariable = unicode_list(kwargs["secondVariable"] or [])
        self._secondMayBeNull = bool(kwargs["secondMayBeNull"])
        self._operation = text(kwargs["operation"])
        self._groupedKeys = [Property.from_dict(_) for _ in kwargs["groupedKeys"]]
        self._valueProperty = Property.from_dict(kwargs["valueProperty"])
        self._window = long(kwargs["window"])

        self._validate()

    def _validate(self):
        if not self.src_variablesid:
            self.src_variablesid = [self.firstVariable, self.secondVariable]

        if self.firstMayBeNull and self.secondMayBeNull:
            raise RuntimeError("at most one source variable could be null in dualvar")

        if self.firstMayBeNull:
            nonnull = self.secondVariable
        else:
            nonnull = self.firstMayBeNull

        if not self.valueProperty:
            self.valueProperty = Property(nonnull, "value", "double")

        if not self.properties:
            self.properties = self.extractProperties()

    @property
    def propertyMappings(self):
        result = self.genMappingsFromVariableData([], [], self.groupedKeys, ["value"])
        thisid = [self.app, self.name]
        result.append(DirectPropertyMapping(srcProperty=Property(self.firstVariable, "value", "double").get_dict(),
                                            destProperty=Property(thisid, self.firstVariable[1], "double").get_dict()))
        result.append(DirectPropertyMapping(srcProperty=Property(self.secondVariable, "value", "double").get_dict(),
                                            destProperty=Property(thisid, self.secondVariable[1], "double").get_dict()))

    @property
    def propertyReductions(self):
        return []

    @property
    def propertyCondition(self):
        return None

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, operation):
        self._operation = text(operation)

    @property
    def groupedKeys(self):
        return self._groupedKeys

    @groupedKeys.setter
    def groupedKeys(self, groupedKeys):
        if groupedKeys and isinstance(groupedKeys[0], dict):
            groupedKeys = [Property.from_dict(_) for _ in groupedKeys]

        self._groupedKeys = groupedKeys

    @property
    def valueProperty(self):
        return self._valueProperty

    @valueProperty.setter
    def valueProperty(self, valueProperty):
        if isinstance(valueProperty, dict):
            valueProperty = Property.from_dict(valueProperty)

        self._valueProperty = valueProperty

    @property
    def firstVariable(self):
        return self._firstVariable

    @firstVariable.setter
    def firstVariable(self, firstVariable):
        self._firstVariable = unicode_list(firstVariable or [])

    @property
    def secondVariable(self):
        return self._secondVariable

    @secondVariable.setter
    def secondVariable(self, secondVariable):
        self. secondVariable = unicode_list(secondVariable or [])

    @property
    def firstMayBeNull(self):
        return self._firstMayBeNull

    @firstMayBeNull.setter
    def firstMayBeNull(self, firstMayBeNull):
        self._firstMayBeNull = bool(firstMayBeNull)

    @property
    def secondMayBeNull(self):
        return self._secondMayBeNull

    @secondMayBeNull.setter
    def secondMayBeNull(self, secondMayBeNull):
        self._secondMayBeNull = bool(secondMayBeNull)

    @property
    def window(self):
        return self._window

    @window.setter
    def window(self, window):
        self._window = long(window)

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "operation": self.operation,
            "groupedKeys": [_.get_dict() for _ in self.groupedKeys],
            "valueProperty": self.valueProperty.get_dict(),
            "firstVariable": self.firstVariable,
            "firstMayBeNull": self.firstMayBeNull,
            "secondVariable": self.secondVariable,
            "secondMayBeNull": self.secondMayBeNull,
            "window": self.window,
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["operation", "groupedKeys", "valueProperty", "firstVariable", "firstMayBeNull", "secondVariable"
                    , "secondMayBeNull", "window"]:
            kwargs[key] = config[key]
        return DualVarOperationVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return DualVarOperationVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return DualVarOperationVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "DualVarOperationVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
