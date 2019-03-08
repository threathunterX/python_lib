#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from property import *
from util import *


__author__ = 'lw'

event_meta_registry = dict()


class EventMeta(object):
    _fixed_properties = {
        "app": "string",
        "name": "string",
        "key": "string",
        "timestamp": "long",
        "value": "double"
    }

    def __init__(self, app, name, type, derived, src_variableid, properties, expire, remark):
        self._app = text(app)
        self._name = text(name)
        self._type = text(type)
        self._derived = derived
        self._src_variableid = unicode_list(src_variableid or list())
        self._properties = properties or list()
        self._expire = expire
        self._remark = text(remark or "")
        pass

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
    def derived(self):
        return self._derived

    @derived.setter
    def derived(self, derived):
        self._derived = derived

    @property
    def src_variableid(self):
        return self._src_variableid

    @src_variableid.setter
    def src_variableid(self, src_variableid):
        src_variableid = src_variableid or tuple()
        self._src_variableid = unicode_obj(src_variableid)

    @property
    def properites(self):
        return self._properties

    @properites.setter
    def properties(self, properties):
        self._properties = properties or list()

    def has_property(self, property):
        for p in self._properties:
            if property.name == p.name and property.type == p.type:
                return True

        for n, t in EventMeta._fixed_properties:
            if property.name == n and property.type == t:
                return True

        return False

    @property
    def expire(self):
        return self._expire

    @expire.setter
    def expire(self, expire):
        self._expire = expire

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or "")

    @property
    def data_schema(self):
        result = {}
        for p in self.properties:
            result[p.name] = p.type

        result.extend(EventMeta._fixed_properties)
        return result

    def get_dict(self):
        return {
            "app": self.app,
            "name": self.name,
            "type": self.type,
            "derived": self.derived,
            "srcVariableID": self.src_variableid,
            "properties": [_.get_dict() for _ in self.properties],
            "expire": self.expire,
            "remark": self.remark
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d:
            return None
        return EventMeta(d.get("app"), d.get("name"), d.get("type"), d.get("derived"), d.get("srcVariableID"),
                         [Property.from_dict(_) for _ in d.get("properties")], d.get("expire"), d.get("remark"))

    @staticmethod
    def from_json(json_str):
        return EventMeta.from_dict(json.loads(json_str))

    @staticmethod
    def register_event_meta(meta):
        id = (meta.app, meta.name)
        event_meta_registry[id] = meta

    @staticmethod
    def unregister_event_meta(meta):
        id = (meta.app, meta.name)
        if id in event_meta_registry:
            del event_meta_registry[id]

    @staticmethod
    def list_event_meta():
        return event_meta_registry.values()[:]

    @staticmethod
    def find_event_meta_by_event(event):
        id = (event.app, event.name)
        return event_meta_registry.get(id)

    @staticmethod
    def find_event_meta_by_id(app, name):
        id = (app, name)
        return event_meta_registry.get(id)

    def copy(self):
        return EventMeta.from_dict(self.get_dict())

    def __str__(self):
        return "EventMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
