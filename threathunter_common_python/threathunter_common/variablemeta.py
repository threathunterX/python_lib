#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .property import Property
from .util import text, unicode_obj
from .propertymapping import ConstantPropertyMapping, DirectPropertyMapping, ConcatPropertyMapping


__author__ = 'lw'

variable_meta_registry = dict()


class VariableMeta(object):
    _fixed_properties = {
        "app": "string",
        "name": "string",
        "key": "string",
        "timestamp": "long",
        "value": "double"
    }

    TYPE2Class = {
    }

    @staticmethod
    def from_dict(d):
        type = d["type"]
        cls = VariableMeta.TYPE2Class.get(type)
        if not cls:
            raise RuntimeError("unsupported property variabe meta type: {}".format(type))

        return cls.from_dict(d)

    @staticmethod
    def from_json(jsonStr):
        return VariableMeta.from_dict(json.loads(jsonStr))

    def __init__(self, *args, **kwargs):
        self._app = text(kwargs["app"])
        self._name = text(kwargs["name"])
        self._type = text(kwargs["type"])
        self._src_variablesid = unicode_obj(kwargs["srcVariablesID"] or list())
        self._src_eventid = unicode_obj(kwargs["srcEventID"] or None)
        self._priority = kwargs["priority"]
        self._properties = [Property.from_dict(_) for _ in kwargs["properties"]] or list()
        self._expire = kwargs["expire"]
        self._ttl = kwargs["ttl"]
        self._internal = bool(kwargs["internal"])
        self._topValue = bool(kwargs.get("topValue", False))
        self._keyTopValue = bool(kwargs.get("keyTopValue", False))
        self._remark = text(kwargs["remark"] or "")

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = text(app)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type)

    @property
    def src_variablesid(self):
        return self._src_variablesid

    @src_variablesid.setter
    def src_variablesid(self, src_variablesid):
        src_variablesid = src_variablesid or list()
        self._src_variablesid = unicode_obj(src_variablesid)

    @property
    def src_eventid(self):
        return self._src_eventid

    @src_eventid.setter
    def src_eventid(self, src_eventid):
        src_eventid = src_eventid or list()
        self._src_eventid = src_eventid

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, priority):
        self._priority = priority

    @property
    def properties(self):
        return self._properties

    @properties.setter
    def properties(self, properties):
        self._properties = properties or list()

    def has_property(self, property):
        for p in self._properties:
            if property.name == p.name and property.type == p.type:
                return True

        for n, t in VariableMeta._fixed_properties:
            if property.name == n and property.type == t:
                return True

        return False

    def find_property_by_name(self, field_name):
        for p in self._properties:
            if p.name == field_name:
                return p

        for key, type in VariableMeta._fixed_properties.iteritems():
            if key == field_name:
                return Property([self.app, self.name], key, type)

        return None

    @property
    def expire(self):
        return self._expire

    @expire.setter
    def expire(self, expire):
        self._expire = expire

    @property
    def ttl(self):
        return self._ttl

    @ttl.setter
    def ttl(self, ttl):
        self._ttl = ttl

    @property
    def internal(self):
        return self._internal

    @internal.setter
    def internal(self, internal):
        self._internal = bool(internal)

    @property
    def topValue(self):
        return self._topValue

    @topValue.setter
    def topValue(self, topValue):
        self._topValue = bool(topValue)

    @property
    def keyTopValue(self):
        return self._keyTopValue

    @keyTopValue.setter
    def keyTopValue(self, keyTopValue):
        self._keyTopValue = bool(keyTopValue)

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or "")

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = unicode_obj(config)

    @property
    def data_schema(self):
        result = {}
        for p in self.properties:
            result[p.name] = p.type

        result.extend(VariableMeta._fixed_properties)
        return result

    @property
    def propertyMappings(self):
        raise RuntimeError("not implemented")

    @property
    def propertyReductions(self):
        raise RuntimeError("not implemented")

    @property
    def propertyCondition(self):
        raise RuntimeError("not implemented")

    def get_dict(self):
        return {
            "app": self.app,
            "name": self.name,
            "type": self.type,
            "srcVariablesID": self.src_variablesid,
            "srcEventID": self.src_eventid,
            "priority": self.priority,
            "properties": [_.get_dict() for _ in self.properties],
            "expire": self.expire,
            "ttl": self.ttl,
            "internal": self.internal,
            "topValue": self.topValue,
            "keyTopValue": self.keyTopValue,
            "remark": self.remark,
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    def genMappingsFromVariableData(self, customizedMappings, reductions, groupedKeys, ignoreNames):
        reductions = reductions or []
        groupedKeys = groupedKeys or []
        customizedMappings = customizedMappings or []
        ignoreNames = ignoreNames or []
        resultMappings = []

        thisid = [self.app, self.name]
        if customizedMappings:
            resultMappings.extend(customizedMappings)

        # add grouped key mapping
        if groupedKeys:
            for p in groupedKeys:
                if p.name in [_.destProperty.name for _ in resultMappings] or \
                                p.name in [_.destProperty.name for _ in reductions]:
                    continue
                else:
                    srcProperty = p.get_dict()
                    destProperty = Property(thisid, p.name, p.type).get_dict()
                    resultMappings.append(DirectPropertyMapping(srcProperty=srcProperty,
                                                                destProperty=destProperty))
        # add default key mapping if there is not one
        if "key" in [_.destProperty.name for _ in resultMappings] or \
                        "key" in [_.destProperty.name for _ in reductions]:
            pass
        else:
            destProperty = Property(thisid, "key", "string").get_dict()
            if not groupedKeys:
                resultMappings.append(ConstantPropertyMapping(type="string", param="",
                                                              destProperty=destProperty))
            else:
                resultMappings.append(ConcatPropertyMapping(type="concat",
                                                            srcProperties=[_.get_dict() for p in groupedKeys],
                                                            destProperty=destProperty))

        # add default value mapping if there is not one
        if "value" in [_.destProperty.name for _ in resultMappings] or \
                        "value" in [_.destProperty.name for _ in reductions]:
            pass
        else:
            resultMappings.append(ConstantPropertyMapping(type="double", param=1.0,
                                                          destProperty=Property(thisid, "value", "double").get_dict()))

        # deal with ignore names
        if ignoreNames:
            resultMappings = filter(lambda m: m.destProperty.name not in ignoreNames, resultMappings)

        return resultMappings

    def extractProperties(self):
        result = []
        mappings = self.propertyMappings
        reductions = self.propertyReductions
        if mappings:
            result.extend([_.destProperty.copy() for _ in mappings])
        if reductions:
            result.extend([_.destProperty.copy() for _ in reductions])

        thisid = [self.app, self.name]
        for p in result:
            p.identifier = thisid

        return result

    @staticmethod
    def register_variable_meta(meta):
        id = (meta.app, meta.name)
        variable_meta_registry[id] = meta

    @staticmethod
    def unregister_variable_meta(meta):
        id = (meta.app, meta.name)
        if id in variable_meta_registry:
            del variable_meta_registry[id]

    @staticmethod
    def list_variable_meta():
        return variable_meta_registry.values()[:]

    @staticmethod
    def find_variable_meta_by_variable(variable):
        id = (variable.app, variable.name)
        return variable_meta_registry.get(id)

    @staticmethod
    def find_variable_meta_by_id(app, name):
        id = (app, name)
        return variable_meta_registry.get(id)

    def copy(self):
        return VariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "VariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


