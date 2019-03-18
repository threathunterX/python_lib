#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..util import text

__author__ = "nebula"

class SequenceValueVariableMeta(VariableMeta):

    TYPE = "sequencevalue"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = SequenceValueVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._operation = text(kwargs["operation"])
        self._groupedKeys = [Property.from_dict(_) for _ in kwargs["groupedKeys"]]
        self._valueProperty = Property.from_dict(kwargs["valueProperty"])
        self._firstCondition = text(kwargs["firstCondition"])
        self._secondCondition = text(kwargs["secondCondition"])

        self._validate()

    def _validate(self):
        srcVariable = self.src_variablesid[0]
        if not self.valueProperty:
            self.valueProperty = Property(srcVariable, "value", "double")

        if not self.firstCondition:
            self.firstCondition = "is not null"

        if not self.secondCondition:
            self.secondCondition = "is not null"

        thisid = [self.app, self.name]
        if not self.properties:
            self.properties = self.extractProperties()
            self.properties.append(Property(thisid, "firstvalue", "double"))
            self.properties.append(Property(thisid, "secondvalue", "double"))

    @property
    def propertyMappings(self):
        return self.genMappingsFromVariableData([], [], self.groupedKeys, ["value"])

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
    def firstCondition(self):
        return self._firstCondition

    @firstCondition.setter
    def firstCondition(self, firstCondition):
        self._firstCondition = text(firstCondition)

    @property
    def secondCondition(self):
        return self._secondCondition

    @secondCondition.setter
    def secondCondition(self, secondCondition):
        self._secondCondition = text(secondCondition)

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "operation": self.operation,
            "groupedKeys": [_.get_dict() for _ in self.groupedKeys],
            "valueProperty": self.valueProperty.get_dict(),
            "firstCondition": self.firstCondition,
            "secondCondition": self.secondCondition,
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["operation", "groupedKeys", "valueProperty", "firstCondition", "secondCondition"]:
            kwargs[key] = config[key]
        return SequenceValueVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return SequenceValueVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return SequenceValueVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "SequenceValueVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
