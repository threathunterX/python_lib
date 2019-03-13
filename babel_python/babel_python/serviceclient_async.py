#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import sys, logging, time

if 'threading' in sys.modules:
    del sys.modules['threading']
import gevent
import gevent.socket
import gevent.monkey
gevent.monkey.patch_all()

from .semaphore_async import CountDownLatch
from .mailutil import populate_data_into_mail, extract_data_from_mail
from .mail import Mail
from .util import gen_uuid, millis_now
from .serviceclient import AtomicLong
from threathunter_common.metrics.metricsagent import get_latency_str_for_millisecond
from threathunter_common.metrics.metricsrecorder import MetricsRecorder
from threathunter_common.event import Event


__author__ = 'lw'

logger = logging.getLogger('babel_python.service_client_async')


class RequestCache(object):
    def __init__(self):
        self.cache = dict()

    def clear_request(self, requestid):
        if requestid in self.cache:
            del self.cache[requestid]
    
    def has_request(self, requestid):
        return self.cache.has_key(requestid)

    def add_request(self, requestid, latch):
        self.cache[requestid] = (requestid, latch, list())

    def get_request(self, requestid):
        return self.cache[requestid][2]

    def add_response(self, requestid, response):
        t = self.cache.get(requestid)
        if t is None:
            return
        id, latch, receivedEvents = t
        receivedEvents.append(response)
        latch.countdown()

# Timeout requests
# requestid: count
TimeoutRequestCount = dict()

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
            self.client_id = kwargs.get("client_id")
            if not self.client_id:
                self.client_id = gen_uuid()

            self.client_id = '_client.{}.{}'.format(self.service_meta.name,self.client_id)

            if service_meta.serverimpl == "rabbitmq":
                from . import babelrabbitmq_async
                self.impl = babelrabbitmq_async
                from .kombu import Empty
                self.Empty = Empty
            elif service_meta.serverimpl == "redis":
                from . import babelredis_async
                from gevent.queue import Empty
                self.Empty = Empty
                self.impl = babelredis_async
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

            self.batch_cache = list()
        except Exception,e:
            if self.raise_if_connect_error:
                import traceback
                raise RuntimeError('babel connect error: %s' % traceback.format_exc())

    def gen_next_requestid(self):
        while True:
            result = self.requestid_base.value
            if not self.requestid_base.compare_and_set(result, result + 1):
                continue
            return "{}_{}".format(self.client_id, result)

    def start(self):
        if self._receiver:
            self._receiver.start_consuming()
#            self.response_task = gevent.spawn(self.process_mails)

    def close(self):
        self.running = False

        if self._sender:
            self._sender.close()
            self._sender = None

#        if self.response_task:
#            self.response_task.join()
#            self.response_task = None

        if self._receiver:
            self._receiver.close()
            self._receiver = None

    def send(self, request, key, block=True, timeout=10, expire=None, least_ret=None):
        if not self.running:
            if self.raise_if_connect_error:
                raise RuntimeError("the service client is closed or not connected")
            else:
                return False

        client_process_start = millis_now()
        try:
            result = self._send(request, key, block, timeout, expire, least_ret)
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

    def _send(self, request, key, block=True, timeout=10, expire=None, least_ret=None):
        # expire time calc
        if expire is None:
            expire = millis_now() + int(timeout * 1000)
        else:
            expire = millis_now() + int(expire * 1000)

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
            if least_ret:
                latch = CountDownLatch(value=least_ret)
            else:
                latch = CountDownLatch(value=countOfReplies)
            self.request_cache.add_request(requestid, latch)
        logger.debug("Service: %s request once[%s]" % (self.service_meta.name, requestid))

        try:
            block = False
            for reqeust_mail, key in request_mails:
                self._sender.send(request_mail.get_json(), key, block, timeout)
        except Exception as err:
            # it's too busy in the send queue
            logger.error("fail to send message, err: %s", err)
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
            gs = [ gevent.spawn(self.process_mail_job, timeout, requestid)
                   for _ in xrange(latch.value)]
            gevent.joinall(gs, timeout=timeout, count=latch.value, raise_error=True)
            success = latch.wait(True, timeout)
        finally:
            if any(not _.dead for _ in gs):
                logger.debug("=============== Kill undead process mail jobs.")
                gevent.killall(gs, exception=gevent.GreenletExit, block=False)
            after_recv_response = millis_now()
            tags = {"type": "receiveresponse"}
            tags.update(self.metrics_tags)
            cost = after_recv_response - after_send_request
            ServiceClient.cost_avg_metrics.record(cost, tags)
            ServiceClient.cost_max_metrics.record(cost, tags)
            ServiceClient.cost_min_metrics.record(cost, tags)
            
        # timeout flag switch to if request cache is None or len == 0
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
                return False, None

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

    def process_mail_job(self, timeout, requestid):
        if not self._receiver:
            logger.error("Serivce Client have not receiver, can't spawn fetch job") # @todo info about service client name
            return
#        with gevent.Timeout(timeout):
        st = time.time()
        while True:
            try:
                content = self._receiver.get(True, 1)
                if not content:
                    continue
                response_mail = Mail.from_json(content)
                if not response_mail:
                    continue
                get_requestid = response_mail.requestid
                
                try:
                    response = extract_data_from_mail(response_mail)
                except Exception as err:
                    import traceback
                    logger.debug("Error when parse output response: %s" % traceback.format_exc())
                    response = err
                if response:
                    logger.debug("Output response for request[%s]" % requestid)
                
                # 拿到已经超时的请求, 就不会放入request_cache， 因为已经被clear了.
                if self.request_cache.has_request(get_requestid):
                    self.request_cache.add_response(get_requestid, response)

                if get_requestid == requestid:
                    # 同一个service同时两个请求，拿到对方的返回, 自己继续
                    # 只有当拿到一个指定requestid的返回的时候才正常退出
                    break
            except self.Empty:
                if not self.request_cache.has_request(requestid):
                    logger.debug("request id[%s] fetch job timeout, because request cache cleared." % requestid)
                    return
                if time.time() - st >= timeout:
                    logger.debug("request id[%s] fetch job timeout." % requestid)
                    return
                logger.debug("request id[%s] continue." % requestid)
            except gevent.GreenletExit:
                logger.debug("request id[%s] timeout,job been killed" % requestid)
                return
        
    def process_mails(self):
        if not self._receiver:
            return

        while self.running:
            try:
                content = self._receiver.get(True, 1)
                self.process_mail(content)
            except self.Empty:
                pass

        self._receiver.close()
#        for content in self._receiver.dump_cache():
#            self.process_mail(content)

    def process_mail(self, mail_injson):
        if not mail_injson:
            return
#        logger.debug("Input: %s" % mail_injson)
        response_mail = Mail.from_json(mail_injson)
#        logger.debug("parse mail out: %s" % response_mail)
        requestid = response_mail.requestid
        try:
            response = extract_data_from_mail(response_mail)
        except Exception as err:
            import traceback
            logger.debug("Error when parse output response: %s" % traceback.format_exc())
            response = err
        if response:
            logger.debug("Output response for request[%s]" % requestid)
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

    def rpc(self, request, key="", block=True, timeout=10, expire=None):
        assert self.service_meta.callmode == "rpc"
        return self.send(request, key, block, timeout, expire)

    def mrpc(self, request, key="", block=True, timeout=10, expire=None):
        assert self.service_meta.callmode == "polling"
        return self.send(request, key, block, timeout, expire)



