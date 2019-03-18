#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .property import Property

__author__ = "nebula"


class PropertyReduction(object):
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
        cls = PropertyReduction.TYPE2Class[type]
        if not cls:
            raise RuntimeError("unsupported property reduction type: {}".format(type))

        return cls.from_dict(d)

    @staticmethod
    def from_json(jsonStr):
        return PropertyReduction.from_dict(json.loads(jsonStr))


class StringPropertyReduction(PropertyReduction):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])

    @property
    def type(self):
        return self._type

    @property
    def destProperty(self):
        return self._destProperty

    @property
    def srcProperties(self):
        return [self.srcProperty]

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

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
            "destProperty": self.destProperty.get_dict(),
            "srcProperty": self.srcProperty.get_dict()
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return StringPropertyReduction(**d)

    @staticmethod
    def from_json(jsonStr):
        return StringPropertyReduction.from_dict(json.loads(jsonStr))

    def copy(self):
        return StringPropertyReduction.from_dict(self.get_dict())

    def __str__(self):
        return "StringPropertyReduction[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyReduction.TYPE2Class.update({
    "stringcount": StringPropertyReduction,
    "stringdistinctcount": StringPropertyReduction,
    "stringlistdistinctcount": StringPropertyReduction,
    "stringlatest": StringPropertyReduction
    })


class WildcardPropertyReduction(PropertyReduction):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])

    @property
    def type(self):
        return self._type

    @property
    def destProperty(self):
        return self._destProperty

    @property
    def srcProperties(self):
        return []

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    def get_dict(self):
        return {
            "type": self.type,
            "destProperty": self.destProperty.get_dict(),
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return WildcardPropertyReduction(**d)

    @staticmethod
    def from_json(jsonStr):
        return WildcardPropertyReduction.from_dict(json.loads(jsonStr))

    def copy(self):
        return WildcardPropertyReduction.from_dict(self.get_dict())

    def __str__(self):
        return "WildcardPropertyReduction[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyReduction.TYPE2Class.update({
    "wildcardcount": WildcardPropertyReduction,
    "wildcarddistinctcount": WildcardPropertyReduction,
    })


class MultiplePropertyReduction(PropertyReduction):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperties = [Property.from_dict(_) for _ in kwargs.get("srcProperties", [])]

    @property
    def type(self):
        return self._type

    @property
    def destProperty(self):
        return self._destProperty

    @property
    def srcProperties(self):
        return self._srcProperties

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

    @srcProperties.setter
    def srcProperties(self, srcProperties):
        srcProperties = srcProperties or []
        if srcProperties and isinstance(srcProperties[0], dict):
            srcProperties = [Property.from_dict(_) for _ in srcProperties]

        self._srcProperties = srcProperties

    def get_dict(self):
        return {
            "type": self.type,
            "destProperty": self.destProperty.get_dict(),
            "srcProperties": [_.get_dict() for _ in self.srcProperties]
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return MultiplePropertyReduction(**d)

    @staticmethod
    def from_json(jsonStr):
        return MultiplePropertyReduction.from_dict(json.loads(jsonStr))

    def copy(self):
        return MultiplePropertyReduction.from_dict(self.get_dict())

    def __str__(self):
        return "MultiplePropertyReduction[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyReduction.TYPE2Class.update({
    "multiplecount": MultiplePropertyReduction,
    "multipledistinctcount": MultiplePropertyReduction,
    })


class LongPropertyReduction(PropertyReduction):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])

    @property
    def type(self):
        return self._type

    @property
    def destProperty(self):
        return self._destProperty

    @property
    def srcProperties(self):
        return [self.srcProperty]

    @destProperty.setter
    def destProperty(self, destProperty):
        if isinstance(destProperty, dict):
            destProperty = Property.from_dict(destProperty)

        self._destProperty = destProperty

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
            "destProperty": self.destProperty.get_dict(),
            "srcProperty": self.srcProperty.get_dict()
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return LongPropertyReduction(**d)

    @staticmethod
    def from_json(jsonStr):
        return LongPropertyReduction.from_dict(json.loads(jsonStr))

    def copy(self):
        return LongPropertyReduction.from_dict(self.get_dict())

    def __str__(self):
        return "LongPropertyReduction[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyReduction.TYPE2Class.update({
    "longmax": LongPropertyReduction,
    "longmin": LongPropertyReduction,
    "longsum": LongPropertyReduction,
    "longcount": LongPropertyReduction,
    "longdistinctcount": LongPropertyReduction,
    "longavg": LongPropertyReduction,
    "longfirst": LongPropertyReduction,
    "longlast": LongPropertyReduction,
    "longstddev": LongPropertyReduction,
    "longrange": LongPropertyReduction,
    "longamplitude": LongPropertyReduction,
    "longlatest": LongPropertyReduction
    })


class DoublePropertyReduction(PropertyReduction):
    def __init__(self, *args, **kwargs):
        self._type = kwargs["type"]
        self._destProperty = Property.from_dict(kwargs["destProperty"])
        self._srcProperty = Property.from_dict(kwargs["srcProperty"])

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
            "destProperty": self.destProperty.get_dict(),
            "srcProperty": self.srcProperty.get_dict()
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return DoublePropertyReduction(**d)

    @staticmethod
    def from_json(jsonStr):
        return DoublePropertyReduction.from_dict(json.loads(jsonStr))

    def copy(self):
        return DoublePropertyReduction.from_dict(self.get_dict())

    def __str__(self):
        return "DoublePropertyReduction[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


PropertyReduction.TYPE2Class.update({
    "doublemax": DoublePropertyReduction,
    "doublemin": DoublePropertyReduction,
    "doublesum": DoublePropertyReduction,
    "doublecount": DoublePropertyReduction,
    "doubledistinctcount": DoublePropertyReduction,
    "doubleavg": DoublePropertyReduction,
    "doublefirst": DoublePropertyReduction,
    "doublelast": DoublePropertyReduction,
    "doublestddev": DoublePropertyReduction,
    "doublerange": DoublePropertyReduction,
    "doubleamplitude": DoublePropertyReduction,
})
