#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import atexit
from Queue import Empty
from threading import Lock

from threathunter_common.metrics.metricsagent import get_latency_str_for_millisecond
from threathunter_common.metrics.metricsrecorder import MetricsRecorder
from threathunter_common.event import Event
from threathunter_common.util import millis_now, run_in_thread

from .semaphore import CountDownLatch
from .mailutil import populate_data_into_mail, extract_data_from_mail
from .mail import Mail
from .util import gen_uuid

try:
    from atomic import AtomicLong
except ImportError:
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


__author__ = 'lw'


class RequestCache(object):
    def __init__(self):
        self.cache = dict()
        self.lock = Lock()

    def clear_request(self, requestid):
        with self.lock:
            if requestid in self.cache:
                del self.cache[requestid]

    def add_request(self, requestid, latch):
        with self.lock:
            self.cache[requestid] = (requestid, latch, list())

    def get_request(self, requestid):
        with self.lock:
            return self.cache[requestid][2]

    def add_response(self, requestid, response):
        with self.lock:
            t = self.cache.get(requestid)
            if t is None:
                return
            id, latch, receivedEvents = t
            receivedEvents.append(response)
            latch.countdown()


class ServiceClient(object):

    send_metrics = MetricsRecorder("babel.client.sendcount", db="fx", type="count")
    costrange_metrics = MetricsRecorder("babel.client.costrange", db="fx", type="count")
    cost_max_metrics = MetricsRecorder("babel.client.cost.max", db="fx", type="max")
    cost_min_metrics = MetricsRecorder("babel.client.cost.min", db="fx", type="min")
    cost_avg_metrics = MetricsRecorder("babel.client.cost.avg", db="fx", type="avg")
    timeout_metrics = MetricsRecorder("babel.client.timeout", db="fx", type="count")
    error_metrics = MetricsRecorder("babel.client.error", db="fx", type="count")

    def __init__(self, service_meta, **kwargs):
        self.raise_if_connect_error = kwargs.get('raise_if_connect_error',True) #add by wxt 2015-12-16 如果初始化失败是否raise异常
        self.running = False
        try:
            self.service_meta = service_meta
            # atexit
            if self.service_meta.callmode == "notify":
                atexit.register(self.batch_notify_flush)

            self.client_id = kwargs.get("client_id")
            if not self.client_id:
                self.client_id = gen_uuid()

            self.client_id = '_client.{}.{}'.format(self.service_meta.name,self.client_id)

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

            if "cdc" not in self.service_meta.options:
                raise RuntimeError("cdc not in service meta")
            self.cdc = self.service_meta.options.get("cdc", "")

            if "," in self.service_meta.options["sdc"] and self.service_meta.callmode != "notify":
                raise RuntimeError("only notify supports multiple data center")

            self._sender = self.impl.get_client_sender(service_meta, **kwargs)
            if service_meta.callmode != "notify":
                # need a queue to receive response
                self._receiver = self.impl.get_client_receiver(service_meta, self.client_id, **kwargs)
            else:
                self._receiver = None

            if service_meta.coder != "mail":
                raise RuntimeError("coder {} is not supported yet".format(service_meta.coder))

            self.requestid_base = AtomicLong(0)
            self.request_cache = RequestCache()
            self.running = True
            self.response_task = None

            self.metrics_tags = {
                "service": service_meta.name,
                "delivery": service_meta.delivermode,
                "call": service_meta.callmode,
                "impl": service_meta.serverimpl,
                "clientid": self.client_id,
                "sdc": self.sdc,
                "cdc": self.cdc
            }

            # caching requests for batch_notify
            self.batch_cache = list()
            self.running = True
        except Exception,e:
            if self.raise_if_connect_error:
                print e
                raise RuntimeError('babel connect error')

    def gen_next_requestid(self):
        while True:
            result = self.requestid_base.value
            if not self.requestid_base.compare_and_set(result, result + 1):
                continue
            return "{}_{}".format(self.client_id, result)

    def start(self):
            if self._receiver:
                self._receiver.start_consuming()
                self.response_task = run_in_thread(self.process_mails)

    def close(self):
        self.running = False

        if self._sender:
            self._sender.close()
            self._sender = None

        if self.response_task:
            self.response_task.join()
            self.response_task = None

        if self._receiver:
            self._receiver.close()
            self._receiver = None

    def send(self, request, key, block=True, timeout=10, expire=None):
        if not self.running:
            if self.raise_if_connect_error:
                raise RuntimeError("the service client is closed or not connected")
            else:
                return False

        client_process_start = millis_now()
        try:
            result = self._send(request, key, block, timeout, expire)
        except Exception as err:
            tags = {"type": "unknown"}
            tags.update(self.metrics_tags)
            ServiceClient.error_metrics.record(1, tags)
            result = False, err
        finally:
            client_process_finish = millis_now()
            tags = {"range": get_latency_str_for_millisecond(client_process_finish-client_process_start)}
            tags.update(self.metrics_tags)
            ServiceClient.costrange_metrics.record(1, tags)

            tags = {"type": "total"}
            tags.update(self.metrics_tags)
            cost = client_process_finish - client_process_start
            ServiceClient.cost_avg_metrics.record(cost, tags)
            ServiceClient.cost_max_metrics.record(cost, tags)
            ServiceClient.cost_min_metrics.record(cost, tags)

            success = result and len(result) > 1 and result[0]
            tags = {"success": "true" if success else "false"}
            tags.update(self.metrics_tags)
            ServiceClient.send_metrics.record(1, tags)
            return result

    def _send(self, request, key, block=True, timeout=10, expire=None):
        # expire time calc
        if expire is None:
            expire = millis_now() + int(timeout * 1000)
        else:
            expire = millis_now() + int(expire * 1000)

        # prepare mail
        requestid = self.gen_next_requestid()
        request_mails = list()
        before_send_reqeust = millis_now()

        if isinstance(request, Event):
            request_mail = Mail.new_mail(self.client_id, self.service_meta.name, requestid)
            populate_data_into_mail(request_mail, request)
            request_mails.append((request_mail, self._sender.get_sharding_key(request.key)))
        elif isinstance(request, list):
            groups = dict()
            #group events by sharding key
            for e in request:
                k = self._sender.get_sharding_key(e.key)
                groups.setdefault(k, list()).append(e)

            for k in groups.keys():
                request_mail = Mail.new_mail(self.client_id, self.service_meta.name, requestid)
                populate_data_into_mail(request_mail, groups[k])
                request_mails.append((request_mail, k))

        for request_mail, key in request_mails:
            request_mail.add_header("expire", str(expire+5000))
            request_mail.add_header("cdc", self.service_meta.options["cdc"])

        countOfReplies = 0
        if self._receiver:
            countOfReplies = self.service_meta.options.get("servercardinality", 1)

        if countOfReplies > 0:
            latch = CountDownLatch(value=countOfReplies)
            self.request_cache.add_request(requestid, latch)

        try:
            for reqeust_mail, key in request_mails:
                self._sender.send(request_mail.get_json(), key, block, timeout)
        except Exception as err:
            tags = {"type": "senderror"}
            tags.update(self.metrics_tags)
            ServiceClient.error_metrics.record(1, tags)
            return False, err
        finally:
            after_send_request = millis_now()
            tags = {"type": "sendrequest"}
            tags.update(self.metrics_tags)
            cost = after_send_request - before_send_reqeust
            ServiceClient.cost_avg_metrics.record(cost, tags)
            ServiceClient.cost_max_metrics.record(cost, tags)
            ServiceClient.cost_min_metrics.record(cost, tags)

        if not countOfReplies:
            return True, None

        success = False
        try:
            success = latch.wait(True, timeout)
        finally:
            after_recv_response = millis_now()
            tags = {"type": "receiveresponse"}
            tags.update(self.metrics_tags)
            cost = after_recv_response - after_send_request
            ServiceClient.cost_avg_metrics.record(cost, tags)
            ServiceClient.cost_max_metrics.record(cost, tags)
            ServiceClient.cost_min_metrics.record(cost, tags)

        if not success:
            tags = {"type": "timeout"}
            tags.update(self.metrics_tags)
            ServiceClient.error_metrics.record(1, tags)
            ServiceClient.timeout_metrics.record(1, self.metrics_tags)

        receivedEvents = self.request_cache.get_request(requestid)
        self.request_cache.clear_request(requestid)
        if self.service_meta.callmode == "rpc":
            # special treatment for one response
            # no response
            if len(receivedEvents) == 0:
                tags = {"type": "noresponse"}
                tags.update(self.metrics_tags)
                ServiceClient.error_metrics.record(1, tags)
                return False, RuntimeError("received no response")

            # remote exception
            response = receivedEvents[0]
            if isinstance(response, Exception):
                tags = {"type": "servererror"}
                tags.update(self.metrics_tags)
                ServiceClient.error_metrics.record(1, tags)
                return False, response

            # success
            return True, response
        else:
            return success, receivedEvents

    def process_mails(self):
        if not self._receiver:
            return

        while self.running:
            try:
                content = self._receiver.get(True, 1)
                self.process_mail(content)
            except Empty:
                pass

        self._receiver.close()
        for content in self._receiver.dump_cache():
            self.process_mail(content)

    def process_mail(self, mail_injson):
        if not mail_injson:
            return
        response_mail = Mail.from_json(mail_injson)
        requestid = response_mail.requestid
        try:
            response = extract_data_from_mail(response_mail)
        except Exception as err:
            response = err

        self.request_cache.add_response(requestid, response)

    # useful handlers
    def notify(self, request, key="", block=True, timeout=10, expire=None):
        assert self.service_meta.callmode == "notify"
        return self.send(request, key, block, timeout, expire)

    def batch_notify(self, request, key="", limit=100):
        assert self.service_meta.callmode == "notify"

        #TODO: not thread safe
        self.batch_cache.append(request)
        if len(self.batch_cache) >= limit:
            temp_cache = self.batch_cache
            self.batch_cache = list()
            return self.notify(temp_cache)
        else:
            return True, None

    def batch_notify_flush(self):
        assert self.service_meta.callmode == "notify"

        if len(self.batch_cache) != 0:
            temp_cache = self.batch_cache
            self.batch_cache = list()
            return self.notify(temp_cache)

    def rpc(self, request, key="", block=True, timeout=10, expire=None):
        assert self.service_meta.callmode == "rpc"
        return self.send(request, key, block, timeout, expire)

    def mrpc(self, request, key="", block=True, timeout=10, expire=None):
        assert self.service_meta.callmode == "polling"
        return self.send(request, key, block, timeout, expire)



