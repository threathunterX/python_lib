#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
import urllib2
from threathunter_common.metrics.influxdbproxy import _extract_metrics_params, get_metrics
from threathunter_common.metrics.metricsagent import MetricsAgent
from threathunter_common.metrics.redismetrics import RedisMetrics

__author__ = 'lw'


def test_extract_params():
    print _extract_metrics_params("/db/Monitor/series?p=test&q=select+event%2C+count%28event%29+from+%22Event%22+where+time+%3E+now%28%29-1h+group+by+time%281m%29%2C+event+order+asc&u=root")
    print _extract_metrics_params("/db/Monitor/series?p=test&q=select mean(value) from \"test_metrics\" where time > 1438333217779ms and time < 1438333277779ms and (\"tag1\" = 'tag1' or \"tag1\" = 'tag2') group by time(60s)")
    print _extract_metrics_params("/db/Monitor/series?p=test&q=select+sum(sum_count)+from+%22auth_pv%22+where+time+%3E+now()-1h+and+hit%3D1+and+qtype%3D%27mobile%27+group+by+time(15m)+fill(0)+order+asc")
    print _extract_metrics_params("/db/Monitor/series?p=test&q=select+source_mark,+sum(sum_count)+from+%22crawl_mobile%22+where+time+%3E+now()-1h+group+by+time(10m),+source_mark+order+asc")


def test_redis():
    MetricsAgent.get_instance().initialize_by_dict({"redis": {"type": "redis", "host": "localhost", "port": "6379"}}, "redis")
    MetricsAgent.get_instance().clear("test", "test")
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag1"}, 1.0, 60)
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag2"}, 3.0, 60)
    time.sleep(1)
    result = get_metrics("/db/test/series?p=test&q=select sum(value) from \"test\" where time > now()-1h and (\"tag1\" = 'tag1' or \"tag1\" = 'tag2') group by time(60s)")
    print result
    assert result[0]["points"][0][1] == 4.0


def test_influxdb():
    MetricsAgent.get_instance().initialize_by_dict({"influxdb": {"type": "influxdb", "url": "http://127.0.0.1:8086/", "username": "test", "password": "test"}}, "influxdb")
    MetricsAgent.get_instance().clear("test", "test")
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag1"}, 1.0, 60)
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag2"}, 3.0, 60)
    time.sleep(1)
    result = get_metrics("/db/test/series?p=test&q=select sum(value) from \"test\" where time > now()-1h and (\"tag1\" = 'tag1' or \"tag1\" = 'tag2') group by time(60s)")
    print result
    assert result[0]["points"][0][1] == 4.0


def test_proxy():
    MetricsAgent.get_instance().initialize_by_dict({"influxdb": {"type": "influxdb", "url": "http://127.0.0.1:8086/", "username": "test", "password": "test"}}, "influxdb")
    MetricsAgent.get_instance().clear("test", "test")
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag1"}, 1.0, 60)
    MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag2"}, 3.0, 60)
    time.sleep(1)
    url = "http://127.0.0.1:8086/db/test/series?p=test&q=select%20sum(value)%20from%20test%20where%20time%20%3E%20now()-1h%20and%20(tag1%20=%20%27tag1%27%20or%20tag1%20=%20%27tag2%27)%20group%20by%20time(60s)&u=root"
    original_result = json.loads(urllib2.urlopen(url).read())
    proxy_result = get_metrics(url)
    print original_result
    print proxy_result
    assert original_result == proxy_result
