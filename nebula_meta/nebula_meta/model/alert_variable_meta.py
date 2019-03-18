#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..util import unicode_obj, text, unicode_list
from ..property_mapping import ConstantPropertyMapping

__author__ = "nebula"


class AlertVariableMeta(VariableMeta):

    TYPE = "alert"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = AlertVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._source = unicode_list(kwargs["source"])
        self._window = long(kwargs["window"])
        self._cooldown = long(kwargs["cooldown"])
        self._collectedVars = unicode_obj(kwargs["collectedVars"] or [])
        self._grouped_key = None
        if "groupedKey" in kwargs:
            self._grouped_key = Property.from_dict(kwargs["groupedKey"])
        self._strategy_name = text(kwargs.get("strategyName", ""))
        self._scene_name = text(kwargs.get("sceneName", ""))
        self._check_type = text(kwargs.get("checkType", ""))
        self._checkpoints = text(kwargs.get("checkpoints", "")) 
        self._tags = unicode_list(kwargs.get("tags", []))
        self._strategy_remark = text(kwargs.get("strategyRemark", ""))
        self._decision = text(kwargs["decision"])
        self._risk_score = long(kwargs["riskScore"])
        self._strategy_ttl = long(kwargs["strategyTtl"])
        self._test = bool(kwargs.get("test", True))

        self._validate()

    def _validate(self):
        if not self._grouped_key:
            self._grouped_key = Property(self._source, "key", "string")

        if not self.src_variablesid:
            srcVariables = [_["collectedVar"] for _ in self.collectedVars]
            if self.source not in srcVariables:
                srcVariables.append(self.source)
            self.src_variablesid = srcVariables

        if not self.properties:
            self.properties = self.extractProperties()
            thisid = [self.app, self.name]
            self.properties.append(Property(thisid, "strategyTtl", "long"))
            self.properties.append(Property(thisid, "variableValues", "map"))

    @property
    def propertyMappings(self):
        customizedMappings = []
        thisid = [self.app, self.name]

        customizedMappings.append(ConstantPropertyMapping(type="string", param=self.strategy_name, destProperty=Property(thisid, "strategyName", "string").get_dict()))
        customizedMappings.append(ConstantPropertyMapping(type="string", param=self.scene_name, destProperty=Property(thisid, "sceneName", "string").get_dict()))
        customizedMappings.append(ConstantPropertyMapping(type="string", param=self.check_type, destProperty=Property(thisid, "sceneName", "string").get_dict()))
        customizedMappings.append(ConstantPropertyMapping(type="string", param=self.strategy_remark, destProperty=Property(thisid, "strategyRemark", "string").get_dict()))
        customizedMappings.append(ConstantPropertyMapping(type="string", param=self.decision, destProperty=Property(thisid, "decision", "string").get_dict()))
        customizedMappings.append(ConstantPropertyMapping(type="long", param=self.risk_score, destProperty=Property(thisid, "riskScore", "long").get_dict()))

        return VariableMeta.genMappingsFromVariableData(self, customizedMappings, [], [self._grouped_key], [])

    @property
    def propertyReductions(self):
        return []

    @property
    def propertyCondition(self):
        return None

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source= unicode_list(source)

    @property
    def window(self):
        return self._window

    @window.setter
    def window(self, window):
        self._window = long(window)

    @property
    def cooldown(self):
        return self._cooldown

    @cooldown.setter
    def cooldown(self, cooldown):
        self._cooldown = long(cooldown)

    @property
    def collectedVars(self):
        return self._collectedVars

    @collectedVars.setter
    def collectedVars(self, collectedVars):
        self._collectedVars = unicode_obj(collectedVars)

    @property
    def grouped_key(self):
        return self._grouped_key

    @grouped_key.setter
    def grouped_key(self, grouped_key):
        if grouped_key and isinstance(grouped_key, dict):
            grouped_key = Property.from_dict(grouped_key)

        self._grouped_key = grouped_key

    @property
    def strategy_name(self):
        return self._strategy_name

    @strategy_name.setter
    def strategy_name(self, strategy_name):
        self._strategy_name = text(strategy_name or "")

    @property
    def scene_name(self):
        return self._scene_name

    @scene_name.setter
    def scene_name(self, scene_name):
        self._scene_name = text(scene_name)

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags= unicode_list(tags)
        
    @property
    def checkpoints(self):
        return self._checkpoints

    @checkpoints.setter
    def checkpoints(self, checkpoints):
        self._checkpoints= text(checkpoints)
        
    @property
    def check_type(self):
        return self._check_type

    @check_type.setter
    def check_type(self, check_type):
        self._check_type = text(check_type)

    @property
    def strategy_remark(self):
        return self._strategy_remark

    @strategy_remark.setter
    def strategy_remark(self, strategy_remark):
        self._strategy_remark = text(strategy_remark or "")

    @property
    def risk_score(self):
        return self._risk_score

    @risk_score.setter
    def risk_score(self, risk_score):
        self._risk_score = long(risk_score)

    @property
    def decision(self):
        return self._decision

    @decision.setter
    def decision(self, decision):
        self._decision = text(decision)

    @property
    def strategy_ttl(self):
        return self._strategy_ttl

    @strategy_ttl.setter
    def strategy_ttl(self, strategy_ttl):
        self._strategy_ttl = long(strategy_ttl)

    @property
    def test(self):
        return self._test

    @test.setter
    def test(self, test):
        self._test = bool(test)

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "source": self.source,
            "window": self.window,
            "cooldown": self.cooldown,
            "collectedVars": self.collectedVars,
            "groupedKey": self.grouped_key.get_dict(),
            "strategyName": self.strategy_name,
            "sceneName": self.scene_name,
            "checkpoints": self.checkpoints,
            "tags": self.tags,
            "checkType": self.check_type,
            "strategyRemark": self.strategy_remark,
            "strategyTtl": self.strategy_ttl,
            "decision": self.decision,
            "riskScore": self.risk_score,
            "test": self.test,
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["source", "window", "cooldown", "collectedVars", "strategyRemark",
                    "strategyTtl", "strategyName",
                    "riskScore", "decision", "sceneName", "tags", "checkpoints",
                    "checkType", "groupedKey", "test"]:
            kwargs[key] = config[key]
        return AlertVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return AlertVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return AlertVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "AlertVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

