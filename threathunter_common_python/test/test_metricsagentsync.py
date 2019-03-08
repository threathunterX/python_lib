#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threathunter_common.metrics.metricsagent import MetricsAgent, set_async_mode
from threathunter_common.util import millis_now


class TestRedisMetricsAgentSync(object):

    def setup_method(self, method):
        MetricsAgent.get_instance().initialize_by_dict(
            {
                "redis": {
                    "type": "redis", "host": "localhost", "port": "6379"
                }
             }, db="test", server_name="redis")
        MetricsAgent.get_instance().clear("test", "test")

    def teardown_method(self, method):
        MetricsAgent.get_instance().clear("test", "test")

    def test_simple(self):
        MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag1"}, 1.0, 60)
        MetricsAgent.get_instance().add_metrics("test", "test", {"tag1": "tag2"}, 3.0, 60)
        import time
        time.sleep(1)
        now = millis_now()
        assert MetricsAgent.get_instance().query("test", "test", "sum", now - 60000, now, 60, {}, {}).values()[0].values()[0] == 4.0

