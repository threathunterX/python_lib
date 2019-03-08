# -*- coding: utf-8 -*-
import time
from threathunter_common.redis.redisctx import RedisCtx
from threathunter_common.metrics.metricsagent import MetricsAgent

nodes = [
    dict(host="127.0.0.1", port=7000),
    dict(host="127.0.0.1", port=7001),
    dict(host="127.0.0.1", port=7002),
    dict(host="127.0.0.1", port=7003),
    dict(host="127.0.0.1", port=7004),
    dict(host="127.0.0.1", port=7005),
]
RedisCtx.get_instance().nodes = nodes

metrics_dict = {
    "app": "nebula_web",
    "redis": {
        "type": "redis",
        "host": "127.0.0.1",
        "port": 7000,
        "password": "foobared",
        "nodes": nodes
    },
    "influxdb": {
        "type": "influxdb",
        "url": "127.0.0.1",
        "username": "test",
        "password": "test"
    },
    "server": "redis"
}

MetricsAgent.get_instance().initialize_by_dict(metrics_dict)

test_db = "nebula.offline"
# proxy的是可以传进去ts的...
test_ts = time.time() * 1000
test_type = "sum"
metrics_name = "cronjob.notice_stat"
test_interval = 300

MetricsAgent.get_instance().add_metrics(test_db, metrics_name, {"status":"run", "ts":test_ts}, 1.0, 600)
MetricsAgent.get_instance().add_metrics(test_db, metrics_name, {"status":"failed", "ts":test_ts}, 1.0, 600)

print("record..")
time.sleep(30)
now = int(time.time()*1000)

print("query result at %s: " % now)
a = MetricsAgent.get_instance().query(test_db, metrics_name, test_type, now - 60000, now, test_interval, {}, [])
print(a)
assert a.values()[0].values()[0] == 2.0

print ("Test pass...")
