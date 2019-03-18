#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import six, json

from ..util import text, unicode_obj, unicode_list


__author__ = "nebula"


class Term(object):
    def __init__(self, left, op, right, remark="", scope="rt"):
        if not isinstance(left, Exp):
            left = Exp.get_exp(left)
        if not left:
            raise RuntimeError("invalid left expression")

        if left.type == "func" and left.subtype in {"setblacklist", "time", "getlocation", "sleep", "spl"}:
            right = None
            op = None
        else:
            if not isinstance(right, Exp):
                right = Exp.get_exp(right)

            if not right:
                raise RuntimeError("invalid right expression")

        self._left = left
        self._right = right
        self._op = text(op or "")
        self._remark = text(remark)
        self._scope = scope

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, left):
        if not isinstance(left, Exp):
            left = Exp.get_exp(left)
        if not left:
            raise RuntimeError("invalid left expression")

        self._left = left

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, right):
        if not isinstance(right, Exp):
            right = Exp.get_exp(right)
        if not right:
            raise RuntimeError("invalid right expression")

        self._right = right

    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, op):
        self._op = text(op)

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark)

    @property
    def scope(self):
        return self._scope

    @scope.setter
    def scope(self, scope):
        self._scope = scope

    def get_dict(self):
        op = self.op
        if not op:
            op = ""
        if not self.right:
            right = None
        else:
            right = self.right.get_dict()
        return {
            "left": self.left.get_dict(),
            "op": op,
            "right": right,
            "remark": self.remark,
            "scope": self.scope
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return Term(d.get("left"), d.get("op"), d.get("right"), d.get("remark", ""), d.get("scope", "rt"))

    @staticmethod
    def from_json(json_str):
        return Term.from_dict(json.loads(json_str))

    def copy(self):
        return Term.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Term[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.left)
        result |= hash(self.op)
        result |= hash(self.right)
        result |= hash(self.remark)
        result |= hash(self.scope)
        return


class Exp(object):
    @staticmethod
    def get_exp(data):
        if isinstance(data, (six.text_type, six.binary_type)):
            data = text(data)
            data = json.loads(data)

        if not isinstance(data, dict):
            return None

        for cls in [ConstantExp, EventFieldExp, FuncCountExp, FuncGetVariableExp, SetBlacklistExp, TimeExp, GetLocationExp, SleepExp, SplExp]:
            result = cls.from_dict(data)
            if result:
                return result
    pass


class ConstantExp(Exp):
    """
    A constant string value.

        {
            "type": "constant",
            "subtype": "",
            "config": {
                "value": "1,2"
            }
        }

    """
    TYPE = "constant"
    SUBTYPE = ""

    def __init__(self, value):
        self._value = text(value)

    @property
    def type(self):
        return ConstantExp.TYPE

    @property
    def subtype(self):
        return ConstantExp.SUBTYPE

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = text(value)

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "value": self.value,
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != ConstantExp.TYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return ConstantExp(config.get("value"))

    @staticmethod
    def from_json(json_str):
        return ConstantExp.from_dict(json.loads(json_str))

    def copy(self):
        return ConstantExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "ConstantExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.value)
        return result


class EventFieldExp(Exp):
    """
    Get one field of event

        {
            "type": "event",
            "subtype": "",
            "config": {
                "event": ["online", "http_static"],
                "field": "c_ip"
            }
        }
    """

    TYPE = "event"
    SUBTYPE = ""

    def __init__(self, event, field):
        self._event = unicode_list(event or [])
        self._field = text(field)

    @property
    def type(self):
        return EventFieldExp.TYPE

    @property
    def subtype(self):
        return EventFieldExp.SUBTYPE

    @property
    def event(self):
        return self._event

    @event.setter
    def event(self, event):
        self._event = unicode_list(event or [])

    @property
    def field(self):
        return self._field

    @field.setter
    def field(self, field):
        self._field = text(field)

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "event": self.event,
                "field": self.field
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != EventFieldExp.TYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return EventFieldExp(config.get("event"), config.get("field"))

    @staticmethod
    def from_json(json_str):
        return EventFieldExp.from_dict(json.loads(json_str))

    def copy(self):
        return EventFieldExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "EventFieldExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        for i in self.event:
            result |= hash(i)

        result |= hash(self.field)
        return result


class FuncGetVariableExp(Exp):
    """
    Function, get one variable value by trigger event and its selected keys.

        {
            "type": "func",
            "subtype": "getvariable",
            "config": {
                "variable": ["online", "http_static_count_ip"],
                "trigger": {
                    "event": ["online", "http_static"],
                    "keys": ["field1", "field2"]
                }
            }
        }
    """

    TYPE = "func"
    SUBTYPE = "getvariable"

    def __init__(self, variable, trigger_event, trigger_fields):
        self._variable = unicode_list(variable or list())
        self._trigger_event = unicode_list(trigger_event or list())
        self._trigger_fields = unicode_list(trigger_fields or list())

    @property
    def type(self):
        return FuncGetVariableExp.TYPE

    @property
    def subtype(self):
        return FuncGetVariableExp.SUBTYPE

    @property
    def variable(self):
        return self._variable

    @variable.setter
    def variable(self, variable):
        self._variable = unicode_list(variable or list())

    @property
    def trigger_event(self):
        return self._trigger_event

    @trigger_event.setter
    def trigger_event(self, trigger_event):
        self._trigger_event = unicode_list(trigger_event or list())

    @property
    def trigger_fields(self):
        return self._trigger_fields

    @trigger_fields.setter
    def trigger_fields(self, trigger_fields):
        self._trigger_fields = unicode_list(trigger_fields or list())

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "variable": self.variable,
                "trigger": {
                    "event": self._trigger_event,
                    "keys": self._trigger_fields
                }
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != FuncGetVariableExp.TYPE or d.get("subtype") != FuncGetVariableExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return FuncGetVariableExp(config.get("variable"), config.get("trigger", {}).get("event"),
                                  config.get("trigger", {}).get("keys"))

    @staticmethod
    def from_json(json_str):
        return FuncGetVariableExp.from_dict(json.loads(json_str))

    def copy(self):
        return FuncGetVariableExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "FuncGetVariableExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        for i in self.variable:
            result |= hash(i)
        for i in self.trigger_event:
            result |= hash(i)
        for i in self.trigger_fields:
            result |= hash(i)

        return result


class FuncCountExp(Exp):
    """
    Build an interval counter.

      {
        "type": "func",
        "subtype": "count",
        "config": {
            "sourceevent": ["online", "http_dynamic"],
            "condition": [
                {
                    "left": "method",
                    "op": "=",
                    "right": "get"
                }
            ],
            "interval": 300,
            "algorithm": "count",
            "operand": [],
            "groupby": ["c_ip", "url"],
            "trigger": {
                "event": ["online", "http_static"],
                "keys": ["c_ip","url"]
            }
        }
      }
    """

    TYPE = "func"
    SUBTYPE = "count"

    def __init__(self, source_event, condition, interval, algorithm, operand, groupby, trigger_event, trigger_fields):
        self._source_event = unicode_list(source_event or list())
        self._condition = unicode_obj(condition)
        self._interval = int(interval)
        self._algorithm = text(algorithm)
        self._operand = unicode_list(operand or [])
        self._groupby = unicode_list(groupby or list())
        self._trigger_event = unicode_list(trigger_event or list())
        self._trigger_fields = unicode_list(trigger_fields or list())

    @property
    def type(self):
        return FuncCountExp.TYPE

    @property
    def subtype(self):
        return FuncCountExp.SUBTYPE

    @property
    def source_event(self):
        return self._source_event

    @source_event.setter
    def source_event(self, source_event):
        self._source_event = unicode_list(source_event or list())

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        self._condition = unicode_obj(condition)

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, interval):
        self._interval = int(interval)

    @property
    def algorithm(self):
        return self._algorithm

    @algorithm.setter
    def algorithm(self, algorithm):
        self._algorithm = text(algorithm)

    @property
    def operand(self):
        return self._operand

    @operand.setter
    def operand(self, operand):
        self._operand = unicode_list(operand or [])

    @property
    def groupby(self):
        return self._groupby

    @groupby.setter
    def groupby(self, groupby):
        self._groupby = unicode_list(groupby or list())

    @property
    def trigger_event(self):
        return self._trigger_event

    @trigger_event.setter
    def trigger_event(self, trigger_event):
        self._trigger_event = unicode_list(trigger_event or list())

    @property
    def trigger_fields(self):
        return self._trigger_fields

    @trigger_fields.setter
    def trigger_fields(self, trigger_fields):
        self._trigger_fields = unicode_list(trigger_fields or list())

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "sourceevent": self.source_event,
                "condition": self.condition,
                "interval": self.interval,
                "algorithm": self.algorithm,
                "groupby": self.groupby,
                "operand": self.operand,
                "trigger": {
                    "event": self._trigger_event,
                    "keys": self._trigger_fields
                }
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != FuncCountExp.TYPE or d.get("subtype") != FuncCountExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return FuncCountExp(config.get("sourceevent"), config.get("condition"), config.get("interval"),
                            config.get("algorithm"), config.get("operand"), config.get("groupby"), config.get("trigger", {}).get("event"),
                            config.get("trigger", {}).get("keys"))

    @staticmethod
    def from_json(json_str):
        return FuncCountExp.from_dict(json.loads(json_str))

    def copy(self):
        return FuncCountExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "FuncCountExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        for i in self.source_event:
            result |= hash(i)
        for i in self.condition:
            # left, op, right
            result |= hash(i.get("left", ""))
            result |= hash(i.get("op", ""))
            result |= hash(i.get("right", ""))
        result |= hash(self.interval)
        result |= hash(self.algorithm)
        for i in self.groupby:
            result |= hash(i)
        for i in self.trigger_event:
            result |= hash(i)
        for i in self.trigger_event:
            result |= hash(i)
        for i in self.trigger_fields:
            result |= hash(i)
        for i in self.operand:
            result |= hash(i)

        return result


class SetBlacklistExp(Exp):
    """
    Set a blacklist item action.

        {
            "type": "func",
            "subtype": "setblacklist",
            "config": {
                "name": self.name,
                "checktype": ,
                "checkvalue": self.check_value,
                "decision": self.decision,
                "checkpoints": "login",
                "ttl": self.ttl,
                "remark": self.remark
            }
        }
    """
    TYPE = "func"
    SUBTYPE = "setblacklist"

    def __init__(self, name, check_type, check_value, decision, ttl, remark, checkpoints):
        self._name = text(name)
        self._check_type = text(check_type)
        self._check_value = text(check_value)
        self._decision = decision
        self._checkpoints = text(checkpoints)
        self._ttl = int(ttl)
        self._remark = text(remark)

    @property
    def type(self):
        return SetBlacklistExp.TYPE

    @property
    def subtype(self):
        return SetBlacklistExp.SUBTYPE

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)

    @property
    def check_type(self):
        return self._check_type

    @check_type.setter
    def check_type(self, check_type):
        self._check_type = text(check_type)

    @property
    def check_value(self):
        return self._check_value

    @check_value.setter
    def check_value(self, check_value):
        self._check_value = text(check_value)

    @property
    def decision(self):
        return self._decision

    @decision.setter
    def decision(self, decision):
        self._decision = decision

    @property
    def ttl(self):
        return self._ttl

    @ttl.setter
    def ttl(self, ttl):
        self._ttl = int(ttl)

    @property
    def checkpoints(self):
        return self._checkpoints

    @checkpoints.setter
    def checkpoints(self, checkpoints):
        self._checkpoints = text(checkpoints)
        
    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark)

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "name": self.name,
                "checktype": self.check_type,
                "checkvalue": self.check_value,
                "decision": self.decision,
                "checkpoints": self.checkpoints,
                "ttl": self.ttl,
                "remark": self.remark
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != SetBlacklistExp.TYPE or d.get("subtype") != SetBlacklistExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return SetBlacklistExp(config.get("name"), config.get("checktype"),
                               config.get("checkvalue"), config.get("decision"),
                               config.get("ttl"), config.get("remark"),
                               config.get("checkpoints"),)

    @staticmethod
    def from_json(json_str):
        return SetBlacklistExp.from_dict(json.loads(json_str))

    def copy(self):
        return SetBlacklistExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "SetBlacklistExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.name)
        result |= hash(self.check_type)
        result |= hash(self.check_value)
        result |= hash(self.decision)
        result |= hash(self.ttl)
        result |= hash(self.checkpoints)
        result |= hash(self.remark)

        return result


class TimeExp(Exp):
    """
    time condition.

        {
            "type": "func",
            "subtype": "time",
            "config": {
                "start": "11:00",
                "end": "12:00"
            }
        }
    """
    TYPE = "func"
    SUBTYPE = "time"

    def __init__(self, starthour, startmin, endhour, endmin):
        self._starthour = int(starthour) % 24
        self._startmin = int(startmin) % 60
        self._endhour = int(endhour) % 24
        self._endmin = int(endmin) % 60

    @property
    def type(self):
        return TimeExp.TYPE

    @property
    def subtype(self):
        return TimeExp.SUBTYPE

    @property
    def starthour(self):
        return self._starthour

    @starthour.setter
    def starthour(self, starthour):
        self._starthour = int(starthour) % 24

    @property
    def startmin(self):
        return self._startmin

    @startmin.setter
    def startmin(self, startmin):
        self._startmin = int(startmin) % 60

    @property
    def endhour(self):
        return self._endhour

    @endhour.setter
    def endhour(self, endhour):
        self._endhour = int(endhour) % 24

    @property
    def endmin(self):
        return self._endmin

    @endmin.setter
    def endmin(self, endmin):
        self._endmin = int(endmin) % 60

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "start": "%02d:%02d" % (self.starthour, self.startmin),
                "end": "%02d:%02d" % (self.endhour, self.endmin),
                }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != TimeExp.TYPE or d.get("subtype") != TimeExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        start = config.get("start", "").split(":")[:2]
        end = config.get("end", "").split(":")[:2]
        if not start[0] or not start[1] or not end[0] or not end[1]:
            raise RuntimeError("invalid time config")
        return TimeExp(start[0], start[1], end[0], end[1])

    @staticmethod
    def from_json(json_str):
        return TimeExp.from_dict(json.loads(json_str))

    def copy(self):
        return TimeExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "TimeExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.starthour)
        result |= hash(self.startmin)
        result |= hash(self.endhour)
        result |= hash(self.endmin)

        return result


class GetLocationExp(Exp):
    """
    location compare.

        {
            "type": "func",
            "subtype": "time",
            "config": {
                "source_event_key": "nebula.undefined.c_ip",
                "op": "!belong",
                "location_type": "city",
                "location_string": [
                  null
                ]
            }
        }
    """
    TYPE = "func"
    SUBTYPE = "getlocation"

    def __init__(self, source_event_field, location_type, op, location_value):
        self._source_event_field = source_event_field
        self._location_type = location_type
        self._op = op
        self._location_value = location_value

    @property
    def type(self):
        return GetLocationExp.TYPE

    @property
    def subtype(self):
        return GetLocationExp.SUBTYPE

    @property
    def source_event_field(self):
        return self._source_event_field

    @source_event_field.setter
    def source_event_field(self, source_event_field):
        self._source_event_field = source_event_field

    @property
    def location_type(self):
        return self._location_type

    @location_type.setter
    def location_type(self, location_type):
        self._location_type = location_type

    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, op):
        self._op = op

    @property
    def location_value(self):
        return self._location_value

    @location_value.setter
    def location_value(self, location_value):
        self._location_value = location_value

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "source_event_key": self.source_event_field,
                "location_type": self.location_type,
                "op": self.op,
                "location_string": self.location_value,
                }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != GetLocationExp.TYPE or d.get("subtype") != GetLocationExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return GetLocationExp(config["source_event_key"], config["location_type"], config["op"], config["location_string"])

    @staticmethod
    def from_json(json_str):
        return GetLocationExp.from_dict(json.loads(json_str))

    def copy(self):
        return GetLocationExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "GetLocationExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.source_event_field)
        result |= hash(self.location_type)
        result |= hash(self.op)
        result |= hash(tuple(self.location_value))

        return result


class SleepExp(Exp):
    """
    sleep expression.

        {
            "type": "func",
            "subtype": "sleep",
            "config": {
                "duration": "10",
                "unit": "m",
            }
        }
    """
    TYPE = "func"
    SUBTYPE = "sleep"

    def __init__(self, duration, unit):
        self._duration = duration
        self._unit = unit

    @property
    def type(self):
        return SleepExp.TYPE

    @property
    def subtype(self):
        return SleepExp.SUBTYPE

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, duration):
        self._duration = duration

    @property
    def unit(self):
        return self._unit

    @unit.setter
    def unit(self, unit):
        self._unit = unit

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "duration": self.duration,
                "unit": self.unit,
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != SleepExp.TYPE or d.get("subtype") != SleepExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return SleepExp(config["duration"], config["unit"])

    @staticmethod
    def from_json(json_str):
        return SleepExp.from_dict(json.loads(json_str))

    def copy(self):
        return SleepExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "SleepExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.duration)
        result |= hash(self.unit)

        return result

class SplExp(Exp):
    """
    spl expression.

        {
            "type": "func",
            "subtype": "spl",
            "config": {
                "expression": "spl expression",
            }
        }
    """
    TYPE = "func"
    SUBTYPE = "spl"

    def __init__(self, expression):
        self._expression = expression

    @property
    def type(self):
        return SplExp.TYPE

    @property
    def subtype(self):
        return SplExp.SUBTYPE

    @property
    def expression(self):
        return self._expression

    @expression.setter
    def expression(self, expression):
        self._expression = expression

    def get_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "config": {
                "expression": self.expression,
            }
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        if not d or d.get("type") != SplExp.TYPE or d.get("subtype") != SplExp.SUBTYPE:
            return None

        config = d.get("config")
        if not config:
            return None
        return SplExp(config["expression"])

    @staticmethod
    def from_json(json_str):
        return SplExp.from_dict(json.loads(json_str))

    def copy(self):
        return SplExp.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "SplExp[{}]".format(self.get_dict())

    def __hash__(self):
        result = 0
        result |= hash(self.type)
        result |= hash(self.subtype)

        result |= hash(self.expression)

        return result
