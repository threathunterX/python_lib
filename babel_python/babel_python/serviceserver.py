#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from multiprocessing.pool import ThreadPool, Queue

from threathunter_common.metrics.metricsagent import get_latency_str_for_millisecond
from threathunter_common.metrics.metricsrecorder import MetricsRecorder
from threathunter_common.util import run_in_thread, millis_now

from .mail import Mail
from .mailutil import extract_data_from_mail, populate_data_into_mail
from .util import gen_uuid

__author__ = "nebula"


class ServiceServer(object):
    processed_metrics = MetricsRecorder("babel.server.processcount", type="count", db="fx", interval=5)
    cost_range_metrics = MetricsRecorder("babel.server.costrange", type="count", db="fx", interval=5)
    cost_avg_metrics = MetricsRecorder("babel.server.cost.avg", type="avg", db="fx", interval=5)
    cost_max_metrics = MetricsRecorder("babel.server.cost.max", type="max", db="fx", interval=5)
    cost_min_metrics = MetricsRecorder("babel.server.cost.min", type="min", db="fx", interval=5)
    error_metrics = MetricsRecorder("babel.server.error", type="count", db="fx", interval=5)

    def __init__(self, service_meta, func=None, workers=5, **kwargs):
        self.service_meta = service_meta
        self.server_id = kwargs.get("server_id")
        self.func = func
        self.sender_cache = dict()
        self.kwargs = kwargs
        self.workers = workers

        if not self.server_id:
            self.server_id = gen_uuid()

        if service_meta.serverimpl == "rabbitmq":
            from . import babelrabbitmq
            self.impl = babelrabbitmq
        elif service_meta.serverimpl == "redis":
            from . import babelredis
            self.impl = babelredis
        else:
            raise RuntimeError("serverimpl {} not implemented yet".format(service_meta.serverimpl))

        if "sdc" not in self.service_meta.options:
            raise RuntimeError("sdc not in service meta")
        self.sdc = self.service_meta.options.get("sdc", "")

        if "," in self.service_meta.options["sdc"]:
            raise RuntimeError("server should only have one dc")

        if service_meta.coder != "mail":
            raise RuntimeError("coder {} is not supported yet".format(service_meta.coder))

        self._receiver = self.impl.get_server_receiver(service_meta, **kwargs)
        #if not func:
            #raise RuntimeError("the service implementation should not by empty")

        if self.workers > 1:
            self.worker_pool = ThreadPool(processes=self.workers)

        self.running = True
        self.metrics_tags = {
            "service": service_meta.name,
            "delivery": service_meta.delivermode,
            "call": service_meta.callmode,
            "impl": service_meta.serverimpl,
            "serverid": self.server_id,
            "sdc": self.sdc
        }

    def _get_sender(self, cdc, client_id):
        key = "{}.{}".format(cdc, client_id)
        result = self.sender_cache.get(key)
        if result:
            return result

        result = self.impl.get_server_sender(cdc, client_id, **self.kwargs)
        self.sender_cache[key] = result
        return result

    def start(self,func=None,sync=False):
        if not self.func:
            self.func = func
        self.running = True
        self._receiver.start_consuming()
        if sync:
            self.accept()
        else:
            self.accept_task = run_in_thread(self.accept)

    def start_sync(self,func=None):
        self.start(func=func, sync=True)

    def close(self):
        if not self.running:
            return

        self.running = False
        self._receiver.stop_consuming()

        if self.accept_task:
            self.accept_task.join()
            self.accept_task = None

        if self.worker_pool:
            self.worker_pool.close()
            self.worker_pool.join()
            self.worker_pool = None

        if self._receiver:
            self._receiver.close()
            self._receiver = None

    def work(self, *args, **kwargs):
        if self.workers == 1:
            # run in this thread
            self.process_mail(*args, **kwargs)
        else:
            self.worker_pool.apply(self.process_mail, args, kwargs)

    def accept(self):
        while self.running:
            try:
                request_mail = self._receiver.get(True, 0.5)
                request_mail = Mail.from_json(request_mail)

                accept_ts = millis_now()
                self.worker_pool.apply(self.process_mail, [(request_mail, accept_ts)])
            except Queue.Empty:
                pass
            except Exception as error:
                tags = {"type": "accept"}
                tags.update(self.metrics_tags)
                ServiceServer.error_metrics.record(1, tags)
                print error

        self._receiver.close()
        for request_mail in self._receiver.dump_cache():
            self.worker_pool.apply(self.process_mail, request_mail)

    def process_mail(self, args):
        try:
            cdc = ""
            client_id = ""
            try:
                request_mail, accept_ts = args
                cdc = request_mail.get_header("cdc", "")
                client_id = request_mail.f

                process_start_ts = millis_now()

                tags = {"type": "accept2process", "cdc": cdc, "clientid": client_id}
                tags.update(self.metrics_tags)
                cost = process_start_ts - accept_ts
                ServiceServer.cost_avg_metrics.record(cost, tags)
                ServiceServer.cost_max_metrics.record(cost, tags)
                ServiceServer.cost_min_metrics.record(cost, tags)

                events = extract_data_from_mail(request_mail)
                if isinstance(events, list):
                    for e in events:
                        result = self.func(e)
                else:
                    result = self.func(events)

                finish_func_ts = millis_now()
                tags = {"type": "invokefunction", "cdc": cdc, "clientid": client_id}
                tags.update(self.metrics_tags)
                cost = finish_func_ts - process_start_ts
                ServiceServer.cost_avg_metrics.record(cost, tags)
                ServiceServer.cost_max_metrics.record(cost, tags)
                ServiceServer.cost_min_metrics.record(cost, tags)

                success = True
            except Exception,err:
                tags = {"type": "invokefunction", "cdc": cdc, "clientid": client_id}
                tags.update(self.metrics_tags)
                ServiceServer.error_metrics.record(1, tags)
                success = False
                result = str(err)

            if self.service_meta.callmode != "notify":
                sender = self._get_sender(cdc, client_id)
                response_mail = Mail.new_mail(self.server_id, client_id, request_mail.requestid)
                populate_data_into_mail(response_mail, result)
                sender.send(response_mail.get_json(), False, 1)

                after_response_ts = millis_now()
                tags = {"type": "sendresponse", "cdc": cdc, "clientid": client_id}
                tags.update(self.metrics_tags)
                cost = after_response_ts - finish_func_ts
                ServiceServer.cost_avg_metrics.record(cost, tags)
                ServiceServer.cost_max_metrics.record(cost, tags)
                ServiceServer.cost_min_metrics.record(cost, tags)
        except Exception as error:
            tags = {"type": "sendresponse", "cdc": cdc, "clientid": client_id}
            tags.update(self.metrics_tags)
            ServiceServer.error_metrics.record(1, tags)
            success = False
        finally:
            finish_ts = millis_now()
            tags = {"type": "total", "cdc": cdc, "clientid": client_id}
            tags.update(self.metrics_tags)
            cost = finish_ts - accept_ts
            ServiceServer.cost_avg_metrics.record(cost, tags)
            ServiceServer.cost_max_metrics.record(cost, tags)
            ServiceServer.cost_min_metrics.record(cost, tags)

            tags = {"range": get_latency_str_for_millisecond(finish_ts-accept_ts), "cdc": cdc, "clientid": client_id}
            tags.update(self.metrics_tags)
            ServiceServer.cost_range_metrics.record(1, tags)

            tags = {"success": "true" if success else "false", "cdc": cdc, "clientid": client_id}
            tags.update(self.metrics_tags)
            ServiceServer.processed_metrics.record(1, tags)
