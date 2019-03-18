#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..util import text, unicode_obj

__author__ = "nebula"

class Notice(object):
    def __init__(self, *args, **kwargs):
        self._timestamp = long(kwargs.get("timestamp", 0))
        self._key = text(kwargs["key"])
        self._checkpoints = text(kwargs.get("checkpoints",""))
        self._scene_name = text(kwargs["scene_name"])
        self._check_type = text(kwargs["check_type"])
        self._strategy_name = text(kwargs["strategy_name"])
        self._decision = text(kwargs["decision"])
        self._risk_score = long(kwargs.get("risk_score", 0))
        self._expire = long(kwargs["expire"])
        self._remark = text(kwargs.get("remark", ""))
        self._variable_values = unicode_obj(kwargs["variable_values"])
        self._test = bool(kwargs.get("test", True))
        self._geo_province = text(kwargs.get("geo_province", ""))
        self._geo_city = text(kwargs.get("geo_city", ""))
        self._tip = unicode_obj(kwargs.get("tip", ""))
        self._uri_stem = text(kwargs.get("uri_stem", ""))
        self._trigger_event = unicode_obj(kwargs.get("trigger_event"))

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self._timestamp = long(timestamp)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = text(key)

    @property
    def checkpoints(self):
        return self._checkpoints

    @checkpoints.setter
    def checkpoints(self, checkpoints):
        self._checkpoints = text(checkpoints)

    @property
    def scene_name(self):
        return self._scene_name

    @scene_name.setter
    def scene_name(self, scene_name):
        self._scene_name = text(scene_name)

    @property
    def check_type(self):
        return self._check_type

    @check_type.setter
    def check_type(self, check_type):
        self._check_type = text(check_type)

    @property
    def strategy_name(self):
        return self._strategy_name

    @strategy_name.setter
    def strategy_name(self, strategy_name):
        self._strategy_name = text(strategy_name)

    @property
    def decision(self):
        return self._decision

    @decision.setter
    def decision(self, decision):
        self._decision = text(decision)

    @property
    def risk_score(self):
        return self._risk_score

    @risk_score.setter
    def risk_score(self, risk_score):
        self._risk_score = long(risk_score)

    @property
    def expire(self):
        return self._expire

    @expire.setter
    def expire(self, expire):
        self._expire = long(expire)

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or "")

    @property
    def variable_values(self):
        return self._variable_values

    @variable_values.setter
    def variable_values(self, variable_values):
        self._variable_values = unicode_obj(variable_values)

    @property
    def test(self):
        return self._test

    @test.setter
    def test(self, test):
        self._test = bool(test)

    @property
    def geo_province(self):
        return self._geo_province

    @geo_province.setter
    def geo_province(self, geo_province):
        self._geo_province = text(geo_province)

    @property
    def geo_city(self):
        return self._geo_city

    @geo_city.setter
    def geo_city(self, geo_city):
        self._geo_city = text(geo_city)

    @property
    def tip(self):
        return self._tip

    @tip.setter
    def tip(self, tip):
        self._tip = unicode_obj(tip)

    @property
    def uri_stem(self):
        return self._uri_stem

    @uri_stem.setter
    def uri_stem(self, uri_stem):
        self._uri_stem = uri_stem

    @property
    def trigger_event(self):
        return self._trigger_event

    @trigger_event.setter
    def trigger_event(self, trigger_event):
        self._trigger_event = unicode_obj(trigger_event)
        
    def get_dict(self):
        return {
            "timestamp": self.timestamp,
            "key": self.key,
            "checkpoints": self.checkpoints,
            "strategy_name": self.strategy_name,
            "scene_name": self.scene_name,
            "check_type": self.check_type,
            "decision": self.decision,
            "risk_score": self.risk_score,
            "expire": self.expire,
            "remark": self.remark,
            "variable_values": self.variable_values,
            "geo_province": self.geo_province,
            "geo_city": self.geo_city,
            "test": self.test,
            "tip": self.tip,
            "uri_stem": self.uri_stem,
            "trigger_event": self.trigger_event
            }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return Notice(**d)

    @staticmethod
    def from_json(jsonStr):
        return Notice.from_dict(json.loads(jsonStr))

    def copy(self):
        return Notice.from_dict(self.get_dict())

    def __str__(self):
        return "Notice[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
