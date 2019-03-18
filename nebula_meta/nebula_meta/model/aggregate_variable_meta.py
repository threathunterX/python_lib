#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from ..property import Property
from ..variable_meta import VariableMeta
from ..property_condition import PropertyCondition
from ..property_reduction import PropertyReduction

from ..util import text

__author__ = "nebula"


class AggregateVariableMeta(VariableMeta):

    TYPE = "aggregate"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = AggregateVariableMeta.TYPE
        VariableMeta.__init__(self, *args, **kwargs)
        self._period = long(kwargs["period"])
        self._aggregateType = text(kwargs["aggregateType"] or "realtime")
        self._groupedKeys = [Property.from_dict(_) for _ in kwargs["groupedKeys"]]
        self._reductions = [PropertyReduction.from_dict(_) for _ in kwargs["reductions"]]
        self._condition = PropertyCondition.from_dict(kwargs["condition"])
        self._validate()

    def _validate(self):
        if not self.src_variablesid:
            raise RuntimeError("no source variable for this aggregate variable {}".format())

        if not self.properties:
            self.properties = self.extractProperties()

    @property
    def propertyMappings(self):
        return self.genMappingsFromVariableData([], self.reductions, self.groupedKeys, [])

    @property
    def propertyReductions(self):
        return self.reductions

    @property
    def propertyCondition(self):
        return self.condition

    @property
    def period(self):
        return self._period

    @period.setter
    def period(self, period):
        self._period = long(period)

    @property
    def aggregateType(self):
        return self._aggregateType

    @aggregateType.setter
    def aggregateType(self, aggregateType):
        self._aggregateType = text(aggregateType)

    @property
    def groupedKeys(self):
        return self._groupedKeys

    @groupedKeys.setter
    def groupedKeys(self, groupedKeys):
        if groupedKeys and isinstance(groupedKeys[0], dict):
            groupedKeys = [Property.from_dict(_) for _ in groupedKeys]
        self._groupedKeys = groupedKeys

    @property
    def reductions(self):
        return self._reductions

    @reductions.setter
    def reductions(self, reductions):
        if reductions and isinstance(reductions[0], dict):
            reductions = [PropertyReduction.from_dict(_) for _ in reductions]
        self._reductions = reductions

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        if isinstance(condition, dict):
            condition = PropertyCondition.from_dict(condition)

        self._condition = condition

    def get_dict(self):
        result = VariableMeta.get_dict(self)
        config = {
            "period": self.period,
            "aggregateType": self.aggregateType,
            "groupedKeys": [_.get_dict() for _ in self.groupedKeys],
            "reductions": [_.get_dict() for _ in self.reductions],
            "condition": self.condition.get_dict() if self.condition else None,
        }
        result["config"] = config
        return result

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        kwargs = dict(d)
        config = kwargs["config"]
        for key in ["period", "aggregateType", "groupedKeys", "reductions", "condition"]:
            kwargs[key] = config[key]
        return AggregateVariableMeta(**kwargs)

    @staticmethod
    def from_json(jsonStr):
        return AggregateVariableMeta.from_dict(json.loads(jsonStr))

    def copy(self):
        return AggregateVariableMeta.from_dict(self.get_dict())

    def __str__(self):
        return "AggregateVariableMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

