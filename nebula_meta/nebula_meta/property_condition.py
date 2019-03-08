#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from .property import Property
from .util import text, unicode_list


__author__ = 'lw'


class PropertyCondition(object):
    TYPE2Class = {
    }

    @property
    def type(self):
        raise RuntimeError("not implemented yet")

    @property
    def srcProperties(self):
        raise RuntimeError("not implemented yet")

    @staticmethod
    def from_dict(d):
        if not d:
            return None
        type = d["type"]
        cls = PropertyCondition.TYPE2Class[type]
        if not cls:
            raise RuntimeError("unsupported property mapping type: {}".format(type))

        return cls.from_dict(d)

    @staticmethod
    def from_json(jsonStr):
        return PropertyCondition.from_dict(json.loads(jsonStr))


class CompoundCondition(PropertyCondition):

    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._conditions = [PropertyCondition.from_dict(_) for _ in kwargs["conditions"]]

    @property
    def type(self):
        return self._type

    @property
    def srcProperties(self):
        result = []
        for c in self.condtions:
            result.extend(c.srcProperties)
        return result

    @property
    def condtions(self):
        return self._conditions

    @condtions.setter
    def conditions(self, conditions):
        if conditions and isinstance(conditions[0], dict):
            conditions = [PropertyCondition.from_dict(_) for _ in conditions]

        self._conditions = conditions

    def get_dict(self):
        return {
            "type": self.type,
            "conditions": [_.get_dict() for _ in self.conditions],
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return CompoundCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return CompoundCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return CompoundCondition.from_dict(self.get_dict())

    def __str__(self):
        return "CompoundCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class LongCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])
        self._param = long(kwargs["param"])

    @property
    def type(self):
        return self._type

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = long(param)

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
            "type": self.type,
            "srcProperty": self.srcProperty.get_dict(),
            "param": self.param,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return LongCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return LongCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return LongCondition.from_dict(self.get_dict())

    def __str__(self):
        return "LongCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

class DoubleCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])
        self._param = float(kwargs["param"])

    @property
    def type(self):
        return self._type

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = float(param)

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
            "type": self.type,
            "srcProperty": self.srcProperty.get_dict(),
            "param": self.param,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return DoubleCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return DoubleCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return DoubleCondition.from_dict(self.get_dict())

    def __str__(self):
        return "DoubleCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class StringCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        if "srcProperty" not in kwargs and "property" in kwargs:
            kwargs["srcProperty"] = kwargs["property"]
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])
        self._param = text(kwargs["param"])

    @property
    def type(self):
        return self._type

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = text(param)

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
            "type": self.type,
            "srcProperty": self.srcProperty.get_dict(),
            "param": self.param,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return StringCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return StringCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return StringCondition.from_dict(self.get_dict())

    def __str__(self):
        return "StringCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class DoubleCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])
        self._param = float(kwargs["param"])

    @property
    def type(self):
        return self._type

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, param):
        self._param = float(param)

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
            "type": self.type,
            "srcProperty": self.srcProperty.get_dict(),
            "param": self.param,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return DoubleCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return DoubleCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return DoubleCondition.from_dict(self.get_dict())

    def __str__(self):
        return "DoubleCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class GeneralCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._srcProperties = [Property.from_dict(_) for _ in kwargs["srcProperties"]]
        self._config = text(kwargs["config"])

    @property
    def type(self):
        return self._type

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = text(config)

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
            "type": self.type,
            "srcProperties": [_.get_dict() for _ in self.srcProperties],
            "config": self.config,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return GeneralCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return GeneralCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return GeneralCondition.from_dict(self.get_dict())

    def __str__(self):
        return "GeneralCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


class LocationCondition(PropertyCondition):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])
        self._paramType = text(kwargs["paramType"])
        self._rightParam = unicode_list(kwargs["rightParam"])

    @property
    def type(self):
        return self._type

    @property
    def paramType(self):
        return self._paramType

    @paramType.setter
    def paramType(self, paramType):
        self._paramType = text(paramType)

    @property
    def rightParam(self):
        return self._rightParam

    @rightParam.setter
    def rightParam(self, rightParam):
        self._rightParam = text(rightParam)

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
            "type": self.type,
            "srcProperty": self.srcProperty.get_dict(),
            "paramType": self.paramType,
            "rightParam": self.rightParam,
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return LocationCondition(**d)

    @staticmethod
    def from_json(jsonStr):
        return LocationCondition.from_dict(json.loads(jsonStr))

    def copy(self):
        return LocationCondition.from_dict(self.get_dict())

    def __str__(self):
        return "LocationCondition[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

PropertyCondition.TYPE2Class.update({
    "and": CompoundCondition,
    "or": CompoundCondition,
    "not": CompoundCondition,
    
    "general": GeneralCondition,
    
    "ipequals": LocationCondition,
    "ipnotequals": LocationCondition,
    "ipcontains": LocationCondition,
    "ipnotcontains": LocationCondition,
    
    "stringcontains": StringCondition,
    "stringnotcontains": StringCondition,
    "stringcontainsby": StringCondition,
    "stringnotcontainsby": StringCondition,
    "stringequals": StringCondition,
    "stringnotequals": StringCondition,
    "stringmatch": StringCondition,
    "stringnotmatch": StringCondition,
    "stringstartwith": StringCondition,
    "stringnotstartwith": StringCondition,
    "stringendwith": StringCondition,
    "stringnotendwith": StringCondition,
    
    "doublesmallerthan": DoubleCondition,
    "doublebiggerthan": DoubleCondition,
    "doubleequals": DoubleCondition,
    "doublenotequals": DoubleCondition,
    "doublesmallerequals": DoubleCondition,
    "doublebiggerequals": DoubleCondition,
    
    "longsmallerthan": LongCondition,
    "longbiggerthan": LongCondition,
    "longequals": LongCondition,
    "longnotequals": LongCondition,
    "longsmallerequals": LongCondition,
    "longbiggerequals": LongCondition,
    })

