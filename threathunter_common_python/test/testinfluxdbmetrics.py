#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.metrics.influxdbmetrics import InfluxdbMetrics
from threathunter_common.util import millis_now

__author__ = 'lw'


def extract_map_from_legend_map(data):
    if not data:
        return dict()

    result = dict()
    for tsdata in data.values():
        for legend, legend_data in tsdata.iteritems():
            result[legend] = legend_data

    return result


class TestInfluxdbMetrics:

    def setup_method(self, method):
        self.m = InfluxdbMetrics("http://127.0.0.1:8086/", "test", "test")
        self.m.clear("MetricsTest", "test_metrics")

    def teardown_method(self, method):
        self.m.clear("MetricsTest", "test_metrics")

    def test_simple(self):
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)

        assert {(): 3.0} == list(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-60000, millis_now(), 60, []).itervalues())[0]
        assert {(): 1.0} == list(self.m.query("MetricsTest", "test_metrics", "max", millis_now()-60000, millis_now(), 60, []).itervalues())[0]
        assert {(): 1.0} == list(self.m.query("MetricsTest", "test_metrics", "min", millis_now()-60000, millis_now(), 60, []).itervalues())[0]
        assert {(): 1.0} == list(self.m.query("MetricsTest", "test_metrics", "mean", millis_now()-60000, millis_now(), 60, []).itervalues())[0]

    def test_restore(self):
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)

        print "start sleep"
        import time
        time.sleep(10)
        print "finish sleep"
        import sys

        try:
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        except Exception as err:
            print err
            sys.stdout.flush()
            pass
        try:
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        except Exception as err:
            print err
            sys.stdout.flush()
            pass
        try:
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        except Exception as err:
            print err
            sys.stdout.flush()
            pass


        print "start sleep 2"
        import time
        time.sleep(10)
        print "finish sleep 2"

        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
        print list(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-600000, millis_now(), 600, []).itervalues())[0]

    def test_filter(self):
        test_data = (
            (1, 2, 3),
            (1, 1, 2),
            (4, 5, 6),
            (0, 0, 2),
            (7, 7, 7)
        )
        for a, b, c in test_data:
            self.m.clear("MetricsTest", "test_metrics")
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, a, 60)
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, b, 60)
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag2"}, c, 60)

            assert {(): float(a + b + c)} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-60000
                                                            , millis_now(), 60, {}, []))
            assert {(): float(max(a, b, c))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "max", millis_now()-60000
                                                            , millis_now(), 60, {}, []))
            assert {(): float(min(a, b, c))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "min", millis_now()-60000
                                                            , millis_now(), 60, {}, []))
            assert {(): (a + b + c) / 3.0} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "mean", millis_now()-60000
                                                            , millis_now(), 60, {}, []))

            assert {(): float(a + b)} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1"]}, []))
            assert {(): float(max(a, b))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "max", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1"]}, []))
            assert {(): float(min(a, b))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "min", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1"]}, []))
            assert {(): (a + b) / 2.0} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "mean", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1"]}, []))

            assert {(): float(a + b + c)} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1", "tag2"]}, []))
            assert {(): float(max(a, b, c))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "max", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1", "tag2"]}, []))
            assert {(): float(min(a, b, c))} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "min", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1", "tag2"]}, []))
            assert {(): (a + b + c) / 3.0} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "mean", millis_now()-60000
                                                            , millis_now(), 60, {"tag1": ["tag1", "tag2"]}, []))

    def test_groupby(self):
        test_data = (
            (1, 2, 3),
            (1, 1, 2),
            (4, 5, 6),
            (0, 0, 2),
            (7, 7, 7)
        )
        for a, b, c in test_data:
            self.m.clear("MetricsTest", "test_metrics")
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, a, 60)
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag1"}, b, 60)
            self.m.add_metrics("MetricsTest", "test_metrics", {"tag1": "tag2"}, c, 60)

            assert {("tag1",): float(a + b), ("tag2",): c} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "sum", millis_now()-60000,
                                               millis_now(), 60, {}, ["tag1"]))

            assert {("tag1",): float(max(a, b)), ("tag2",): c} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "max", millis_now()-60000,
                                                            millis_now(), 60, {}, ["tag1"]))

            assert {("tag1",): float(min(a, b)), ("tag2",): c} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "min", millis_now()-60000,
                                                            millis_now(), 60, {}, ["tag1"]))

            assert {("tag1",): (a + b) / 2.0, ("tag2",): c} == \
                   extract_map_from_legend_map(self.m.query("MetricsTest", "test_metrics", "mean", millis_now()-60000,
                                                            millis_now(), 60, {}, ["tag1"]))


if __name__ == "__main__":
    t = TestInfluxdbMetrics()
    t.setup_method(None)
    t.test_restore()
