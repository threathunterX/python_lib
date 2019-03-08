#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.metrics.metricsagent import MetricsAgent
from threathunter_common.util import millis_now
import os
import time

__author__ = 'lw'


def test_agent():
    c_file = os.path.join(os.path.dirname(__file__), "metrics.conf")
    MetricsAgent.get_instance().initialize(c_file)


def common_test():
    # MetricsAgent.get_instance().clear("MetricsTest", "test_metrics")
    MetricsAgent.get_instance().add_metrics("redq", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
    MetricsAgent.get_instance().add_metrics("redq", "test_metrics", {"tag1": "tag1"}, 1.0, 60)
    MetricsAgent.get_instance().add_metrics("redq", "test_metrics", {"tag1": "tag1"}, 1, 60)


def test_redis():
    c_file = os.path.join(os.path.dirname(__file__), "metrics.conf")
    MetricsAgent.get_instance().initialize(c_file, "redis")
    common_test()


def test_influxdb():
    c_file = os.path.join(os.path.dirname(__file__), "metrics.conf")
    MetricsAgent.get_instance().initialize(c_file, "influxdb")
    common_test()

if __name__ == '__main__':
    test_influxdb()

