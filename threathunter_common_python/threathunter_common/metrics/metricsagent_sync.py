#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import atexit
import logging

from Queue import Empty
from multiprocessing import Lock, Queue

from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from ..network import get_local_ip
from ..util import millis_now, gen_uuid
from ..util import run_in_thread, run_in_subprocess

__author__ = "nebula"

logger = logging.getLogger()


class MetricsAgentSync(object):
    _INSTANCE = None
    _LOCK = Lock()

    def __init__(self):
        logger.info("MetricsAgent Start")
        self.m = None
        self.task = None
        self.queue = Queue(maxsize=30000)
        self.batch_queue = Queue(maxsize=30000)
        self.metric_hash = {}  # 聚合hash map by wxt
        self.task = None
        self.running = False
        self.db = None  # 设置 db 的默认值 by wxt
        self.expire_seconds = 604800
        self.app = ""
        self.recorders = dict()
        atexit.register(self.close)

    def add_recorder(self, recorder):
        id = gen_uuid()
        self.recorders[id] = recorder

    def remove_recorder(self, id):
        if id in self.recorders:
            del self.recorders[id]

    @staticmethod
    def get_instance():
        if MetricsAgentSync._INSTANCE is None:
            with MetricsAgentSync._LOCK:
                if MetricsAgentSync._INSTANCE is None:
                    MetricsAgentSync._INSTANCE = MetricsAgentSync()
        return MetricsAgentSync._INSTANCE

    def initialize_by_dict(self, data, db=None, server_name=None):
        with MetricsAgentSync._LOCK:
            if self.task:
                self.running = False
                self.task.join()
                self.task = None
            if not server_name:
                server_name = data["server"]

            if db:
                self.db = db
            server_data = data[server_name]
            if server_data["type"] == "redis":
                from threathunter_common.metrics.redismetrics import RedisMetrics
                self.m = RedisMetrics(server_data["host"], server_data["port"],
                                      password=server_data.get("password"),
                                      nodes=server_data.get("nodes"))
            elif server_data["type"] == "influxdb":
                from threathunter_common.metrics.influxdbmetrics import InfluxdbMetrics
                self.m = InfluxdbMetrics(server_data["url"], server_data["username"], server_data["password"])
            elif server_data["type"] == "influxdb_new":
                from threathunter_common.metrics.influxdbmetrics import InfluxdbMetricsNew
                self.m = InfluxdbMetricsNew(server_data["url"], server_data["username"], server_data["password"])
            else:
                raise RuntimeError("the underlying metrics is not supported")

            self.app = data.get("app", "")
            self.running = True
            self.task = run_in_thread(self.backend_task)

    def initialize(self, c_file, db=None, server_name=None):
        data = load(file(c_file), Loader=Loader) or dict()
        self.initialize_by_dict(data, db, server_name)

    def add_metrics(self, db, metrics_name, tags, value, expire_seconds):
        if self.m is None:
            raise RuntimeError("metrics is not initialized yet")
        tags = dict(tags) if tags else dict()
        try:
            self.queue.put_nowait((db, metrics_name, tags, value, expire_seconds))
        except Exception as err:
            print err

    def advenced_write(self, name, obj={}):
        try:
            obj = dict(obj)
            obj["metrics_name"] = name
            self.batch_queue.put_nowait(obj)
        except Exception, e:
            logger.exception("metric_write error")

    def query(self, db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags=dict(),
              group_tags=list()):
        if self.m is None:
            raise RuntimeError("metrics is not initialized yet")
        result = self.m.query(db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags,
                              group_tags)
        return result

    def clear(self, db, metrics_name):
        if self.m is None:
            raise RuntimeError("metrics is not initialized yet")
        return self.m.clear(db, metrics_name)

    def normalize_tags(self, tags):
        if tags is None:
            tags = dict()
        if "hostip" not in tags:
            tags["hostip"] = get_local_ip()
        if "app" not in tags:
            tags["app"] = self.app

    def backend_task(self):
        last_send_batch = millis_now()
        last_record_flush = millis_now()
        while self.running:
            try:
                current = millis_now()
                if (current - last_record_flush > 30000):
                    recorders = self.recorders.values()
                    for r in recorders:
                        r.flush()
                    last_record_flush = current

                if (current - last_send_batch > 10000):
                    self.do_batch_process()
                    last_send_batch = current

                db, metrics_name, tags, value, expire_seconds = self.queue.get(True, 0.5)
                self.normalize_tags(tags)
                self.m.add_metrics(db, metrics_name, tags, value, expire_seconds)
            except Empty:
                import time
                time.sleep(0.5)
            except Exception as err:
                import traceback
                print traceback.print_exc()

                continue

    def do_batch_process(self):
        for i in xrange(10000):
            try:
                record = self.batch_queue.get_nowait()
                mhash = hash(frozenset(record.items()))
                if self.metric_hash.has_key(mhash):
                    self.metric_hash[mhash]["count"] += 1
                else:
                    self.metric_hash[mhash] = {"data": record, "count": 1}
            except Empty, e:
                break
        for metric in self.metric_hash.values():
            try:
                d = metric["data"]
                metrics_name = d.pop("metrics_name")
                if d.has_key("metrics_db"):
                    metrics_db = d.pop("metrics_db")
                else:
                    metrics_db = self.db
                if d.has_key("expire_seconds"):
                    expire_seconds = d.pop("expire_seconds")
                else:
                    expire_seconds = self.expire_seconds
                if d.has_key("value"):
                    value = d.pop("value")
                else:
                    value = metric["count"]
                self.normalize_tags(d)
                self.m.add_metrics(db=metrics_db, metrics_name=metrics_name, tags=d, value=value,
                                   expire_seconds=expire_seconds)
            except Exception, e:
                logger.exception('metrics_record error %s', str(e))
        self.metric_hash = {}
        dict()

    def close(self):
        recorders = self.recorders.values()
        for r in recorders:
            r.flush()

        self.do_batch_process()

        while True:
            try:
                db, metrics_name, tags, value, expire_seconds = self.queue.get(True, 0.5)
                self.normalize_tags(tags)
                self.m.add_metrics(db, metrics_name, tags, value, expire_seconds)
            except Empty:
                return
            except Exception as err:
                import traceback
                print traceback.print_exc()
                continue
