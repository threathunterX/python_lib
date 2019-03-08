#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import atexit

# from ..concurrent.atomics import AtomicLong
from ..util import millis_now
from .metricsagent import MetricsAgent

__author__ = 'lw'

# remove atomics
# try:
#     from atomic import AtomicLong
# except ImportError:
class AtomicLong:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def compare_and_set(self, expect, new_value):
        if self._value == expect:
            self._value = new_value
            return True
        else:
            return False

    def __iadd__(self, other):
        self._value += other
        return self


class Tags:

    def __init__(self, tags):
        self.tags = dict(tags) # an copy
        self.hashcode = 0

    def get_tags(self):
        return self.tags

    def __hash__(self):
        if self.hashcode:
            return self.hashcode

        self.hashcode = hash(tuple(dict().iteritems()))
        return self.hashcode

    def __eq__(self, other):
        if self is other:
            return True

        if isinstance(other, Tags):
            return other.get_tags() == self.tags

        return False

    def __ne__(self, other):
        return not (self == other)


class MetricsRecorder(object):

    def __init__(self, metrics_name, expire=3*86400, interval=60, type="sum", db="default"):
        self.metrics_name = metrics_name
        self.db = db
        self.expire = expire
        self.interval = interval
        self.type = type

        # record the timestamp of the last flush to metrics backend
        self.last_flush_ts = 0

        # store all the values of different tags
        self.store = dict()

        # regist to the metrics agent
        self.regist_id = MetricsAgent.get_instance().add_recorder(self)
        atexit.register(self.flush)

    def record(self, value, tags={}):
        key = Tags(tags)
        v = self.store.setdefault(key, [AtomicLong(0), AtomicLong(0)])
        if self.type == "sum":
            v[0] += value
        elif self.type == "count":
            v[0] += 1
        elif self.type == "latest":
            v[0].get_and_set(value)
        elif self.type == "max":
            while True:
                current = v[0].value
                new_value = max(current, value)
                if v[0].compare_and_set(current, new_value):
                    break
        elif self.type == "min":
            while True:
                current = v[0].value
                new_value = min(current, value)
                if v[0].compare_and_set(current, new_value):
                    break
        elif self.type == "avg":
            v[0] += value
            v[1] += 1
        else:
            pass

    def flush(self):
        if (millis_now() - self.last_flush_ts) < (self.interval * 1000):
            return

        temp_store = self.store
        self.store = dict()
        for k, v in temp_store.iteritems():
            if self.type == "avg":
                value = float(v[0].value) / v[1].value
            else:
                value = v[0].value
            MetricsAgent.get_instance().add_metrics(self.db, self.metrics_name, k.get_tags(), value, self.expire)

        self.last_flush_ts = millis_now()

    def __del__(self):
        if self.regist_id:
            MetricsAgent.get_instance().remove_recorder(self.regist_id)
            self.regist_id = None


