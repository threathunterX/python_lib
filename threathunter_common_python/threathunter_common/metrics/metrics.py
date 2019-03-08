#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'lw'


class Metrics(object):

    def add_metrics(self, db, metrics_name, tags, value, expire_seconds):
        raise RuntimeError("not supported")

    def query(self, db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags=dict(),
              group_tags=list()):
        raise RuntimeError("not supported")
