#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import urlparse

from influxdb.influxdb08 import InfluxDBClient
from influxdb import InfluxDBClient as InfluxDBClientNew

from threathunter_common.metrics.metrics import Metrics
from threathunter_common.util import utf8

__author__ = "nebula"


class InfluxdbMetrics(Metrics):
    def __init__(self, url, username=None, password=None):
        self.url = url
        parse = urlparse.urlparse(url)
        self.username = username or parse.username
        self.password = password or parse.password
        self.hostname = parse.hostname
        self.port = parse.port
        self.idb = InfluxDBClient(host=self.hostname, port=self.port, username=username, password=password)

    def add_metrics(self, db, metrics_name, tags, value, expire_seconds, timestamp=None):
        self.idb.switch_database(db)
        fields = dict()
        if tags:
            for k, v in tags.iteritems():
                fields[utf8(k)] = utf8(v)
        fields["value"] = value
        if timestamp:
            fields["time"] = timestamp

        point = {"name": metrics_name, "columns": fields.keys(), "points": [fields.values()]}
        self.idb.write_points([point], database=db, time_precision="ms")

    def query(self, db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags=dict(),
              group_tags=list()):
        self.idb.switch_database(db)
        q = self._generate_query_string(metrics_name, aggregation_type, time_start, time_end, time_interval,
                                        filter_tags,
                                        group_tags)
        series = self.idb.query(q, time_precision="ms")

        result = dict()
        if series:
            for s in series:
                columns = s["columns"]
                points = s["points"]

                for p in points:
                    legend = list()
                    for c, v in zip(columns, p):
                        if c == "time":
                            ts = long(v)
                        elif c == aggregation_type:
                            value = float(v)
                        else:
                            legend.append(str(v))
                    legend = tuple(legend)
                    result.setdefault(ts, {})[legend] = value
        return result

    def clear(self, db, metrics_name):
        self.idb.switch_database(db)
        self.idb.delete_series(metrics_name)

    def _generate_query_string(self, metrics_name, aggregation_type, time_start, time_end, time_interval,
                               filter_tags={},
                               group_tags=[]):
        result = "select {}(value) from \"{}\"".format(aggregation_type, metrics_name)
        # the influxdb can only use open interval, so we need to adjust a little
        result += " where time > {}ms and time < {}ms".format(time_start - 1, time_end)
        if filter_tags:
            for tag_key, tag_value_list in filter_tags.iteritems():
                result += " and ("
                result += " or ".join(["\"{}\" = \'{}\'".format(tag_key, tag_value) for tag_value in tag_value_list])
                result += ")"

        result += " group by "
        if group_tags:
            for group_tag in group_tags:
                result += "{}, ".format(group_tag)

        result += "time({}s)".format(time_interval)
        return result


class InfluxdbMetricsNew(Metrics):
    """
    支持influxdb 1.0以上版本
    """

    def __init__(self, url, username=None, password=None):
        self.url = url
        parse = urlparse.urlparse(url)
        self.username = username or parse.username
        self.password = password or parse.password
        self.hostname = parse.hostname
        self.port = parse.port
        self.idb = InfluxDBClientNew(host=self.hostname, port=self.port, username=username, password=password)

    def add_metrics(self, db, metrics_name, tags, value, expire_seconds=None, timestamp=None):
        self.idb.switch_database(db)
        tags_dict = dict()
        fields = dict()
        if tags:
            for k, v in tags.iteritems():
                tags_dict[utf8(k)] = utf8(v)
        fields["value"] = float(value)

        point = {"measurement": metrics_name, "tags": tags_dict, "fields": fields}
        self.idb.write_points([point], database=db, time_precision="ms")

    def query(self, db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags=dict(),
              group_tags=list()):
        self.idb.switch_database(db)
        q = self._generate_query_string(metrics_name, aggregation_type, time_start, time_end, time_interval,
                                        filter_tags,
                                        group_tags)
        series = self.idb.query(q)
        return series

    def clear(self, db, metrics_name):
        self.idb.switch_database(db)
        self.idb.delete_series(metrics_name)

    def _generate_query_string(self, metrics_name, aggregation_type, time_start, time_end, time_interval,
                               filter_tags={},
                               group_tags=[]):
        result = "select {}(value) from \"{}\"".format(aggregation_type, metrics_name)
        # the influxdb can only use open interval, so we need to adjust a little
        result += " where time > {}ms and time < {}ms".format(time_start - 1, time_end)
        if filter_tags:
            for tag_key, tag_value_list in filter_tags.iteritems():
                result += " and ("
                result += " or ".join(["\"{}\" = \'{}\'".format(tag_key, tag_value) for tag_value in tag_value_list])
                result += ")"
        result += " group by "
        if group_tags:
            for group_tag in group_tags:
                result += "{}, ".format(group_tag)

        result += " time({}s) fill(none)".format(time_interval)
        return result


if __name__ == '__main__':
    a = InfluxdbMetricsNew(username='root',password='influxdbthreathunter',url='http://139.196.74.210:8086/')
    for x in xrange(1,30):
        a.add_metrics('redq','cpu2',{'ccc':'pppp','ggg':111},value=1)
        a.add_metrics('redq', 'cpu2', {'ccc': 'xxxxx'}, value=1)
        print a.query('redq','cpu2',aggregation_type='sum',time_start=1498060800000,time_end=1498233600000,time_interval=60,filter_tags={'ccc':['pppp']},group_tags=['ggg'])
        print 'ttt'


