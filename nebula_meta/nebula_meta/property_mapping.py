#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .property import Property
from threathunter_common.util import text

__author__ = "nebula"


class PropertyMapping(object):
    TYPE2Class = {
    }

    @property
    def type(self):
        raise RuntimeError("not implemented yet")

    @property
    def srcProperties(self):
        raise RuntimeError("not implemented yet")

    @property
    def destProperty(self):
        raise RuntimeError("not implemented yet")

    @staticmethod
    def from_dict(d):
        type = d["type"]
        cls = PropertyMapping.TYPE2Class[type]
        if not cls:
            raise RuntimeError("unsupported property mapping type: {}".format(type))

        return cls.from_dict(d)

    @staticmethod
    def from_json(jsonStr):
        return PropertyMapping.from_dict(json.loads(jsonStr))


class ConstantPropertyMapping(PropertyMapping):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._param = kwargs["param"]
        if self._type == "string":
            self._param = text(self._param)
        elif self._type == "long":
            self._param = long(self._param)
        elif self._type == "double":
            self._param = float(self._param)
        elif self._type == "boolean":
            self._param = bool(self._param)
        else:
            raise RuntimeError("unsupport type {} for ConstantPropertyMapping".format(self._type))

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = param
        if self._type == "string":
            self._param = text(self._param)
        elif self._type == "long":
            self._param = long(self._param)
        elif self._type == "double":
            self._param = float(self._param)
        elif self._type == "bool":
            self._param = bool(self._param)

    @property
    def type(self):
        return self._type

    @property
    def destProperty(self):
        return self._destProperty

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    @property
    def srcProperties(self):
        return []

    def get_dict(self):
        return {
            "type": self.type,
            "destProperty": self.destProperty.get_dict(),
            "param": self.param
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return ConstantPropertyMapping(**d)

    @staticmethod
    def from_json(jsonStr):
        return ConstantPropertyMapping.from_dict(json.loads(jsonStr))

    def copy(self):
        return ConstantPropertyMapping.from_dict(self.get_dict())

    def __str__(self):
        return "ConstantPropertyMapping[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyMapping.TYPE2Class.update({
    "boolean": ConstantPropertyMapping,
    "long": ConstantPropertyMapping,
    "double": ConstantPropertyMapping,
    "string": ConstantPropertyMapping,
})


class DirectPropertyMapping(PropertyMapping):
    def __init__(self, *args, **kwargs):
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])

    @property
    def type(self):
        return "direct"

    @property
    def destProperty(self):
        return self._destProperty

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    @property
    def srcProperties(self):
        return [self.srcProperty]

    @property
    def srcProperty(self):
        return self._srcProperty

    @srcProperty.setter
    def srcProperty(self, srcProperty):
        if isinstance(srcProperty, dict):
            srcProperty = Property.from_dict(srcProperty)

        self._srcProperty = srcProperty

    def get_dict(self):
        return {
            "type": "direct",
            "destProperty": self.destProperty.get_dict(),
            "srcProperty": self.srcProperty.get_dict()
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return DirectPropertyMapping(**d)

    @staticmethod
    def from_json(jsonStr):
        return DirectPropertyMapping.from_dict(json.loads(jsonStr))

    def copy(self):
        return DirectPropertyMapping.from_dict(self.get_dict())

    def __str__(self):
        return "DirectPropertyMapping[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class ConcatPropertyMapping(PropertyMapping):
    def __init__(self, *args, **kwargs):
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperties = [Property.from_dict(_) for _ in kwargs["srcProperties"]]

    @property
    def type(self):
        return "concat"

    @property
    def destProperty(self):
        return self._destProperty

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    @property
    def srcProperties(self):
        return self._srcProperties

    @srcProperties.setter
    def srcProperties(self, srcProperties):
        if srcProperties and isinstance(srcProperties[0], dict):
            srcProperties = [Property.from_dict(_) for _ in srcProperties]
        self._srcProperties = srcProperties

    def get_dict(self):
        return {
            "type": "concat",
            "destProperty": self.destProperty.get_dict(),
            "srcProperties": [_.get_dict() for _ in self.srcProperties]
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return ConcatPropertyMapping(**d)

    @staticmethod
    def from_json(jsonStr):
        return ConcatPropertyMapping.from_dict(json.loads(jsonStr))

    def copy(self):
        return ConcatPropertyMapping.from_dict(self.get_dict())

    def __str__(self):
        return "ConcatPropertyMapping[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class VariableValuePropertyMapping(PropertyMapping):
    def __init__(self, *args, **kwargs):
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcIdentifier = kwargs["srcIdentifier"] or []

    @property
    def type(self):
        return "variablevalue"

    @property
    def destProperty(self):
        return self._destProperty

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    @property
    def srcProperties(self):
        return [Property(self.srcIdentifier, "value", "double")]

    @property
    def srcIdentifier(self):
        return self._srcIdentifier

    @srcIdentifier.setter
    def srcIdentifier(self, srcIdentifier):
        self._srcIdentifier = srcIdentifier or []

    def get_dict(self):
        return {
            "type": "variablevalue",
            "destProperty": self.destProperty.get_dict(),
            "srcIdentifier": self.srcIdentifier
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return VariableValuePropertyMapping(**d)

    @staticmethod
    def from_json(jsonStr):
        return VariableValuePropertyMapping.from_dict(json.loads(jsonStr))

    def copy(self):
        return VariableValuePropertyMapping.from_dict(self.get_dict())

    def __str__(self):
        return "VariableValuePropertyMapping[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyMapping.TYPE2Class.update({
    "concat": ConcatPropertyMapping,
    "direct": DirectPropertyMapping,
    "variablevalue": VariableValuePropertyMapping
    })

