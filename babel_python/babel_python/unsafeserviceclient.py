#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
import Queue

from .mailutil import populate_data_into_mail, extract_data_from_mail
from .mail import Mail
from .util import gen_uuid, millis_now
from .babelrabbitmq import RabbitmqClientSender, RabbitmqClientReceiver
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


class UnsafeServiceClient(object):

    def __init__(self, service_meta, client_id=None):
        self.service_meta = service_meta
        self.client_id = client_id
        if not self.client_id:
            self.client_id = gen_uuid()

        if service_meta.serverimpl != "rabbitmq":
            raise RuntimeError("serverimpl {} not implemented yet".format(service_meta.serverimpl))

        self._sender = RabbitmqClientSender(service_meta)
        if service_meta.callmode != "notify":
            # need a queue to receive response
            self._receiver = RabbitmqClientReceiver(self.client_id)
        else:
            self._receiver = None

        if service_meta.coder != "mail":
            raise RuntimeError("coder {} is not supported yet".format(service_meta.coder))

        self.requestid_base = AtomicLong(0)
        self.running = True

    def gen_next_requestid(self):
        while True:
            result = self.requestid_base.value
            if not self.requestid_base.compare_and_set(result, result + 1):
                continue
            return "{}_{}".format(self.client_id, result)

    def start(self):
        if self._receiver:
            self._receiver.start_consuming()

    def close(self):
        self.running = False

        if self._sender:
            self._sender.close()
            self._sender = None

        if self._receiver:
            self._receiver.close()
            self._receiver = None

    def send(self, request, key, block=True, timeout=10):
        if not self.running:
            raise RuntimeError("the service client is closed")
        requestid = self.gen_next_requestid()
        request_mail = Mail.new_mail(self.client_id, self.service_meta.name, requestid)
        populate_data_into_mail(request_mail, request)

        expire = millis_now() + int(timeout * 1000)
        request_mail.add_header("expire", str(expire+5000))

        countOfReplies = 0
        if self._receiver:
            countOfReplies = self.service_meta.options.get("servercardinality", 1)

        try:
            self._sender.send(request_mail.get_json(), key, block, timeout)
        except Queue.Full:
            # it's too busy in the send queue
            raise

        if not countOfReplies:
            return True, None

        receivedEvents = list()
        while self.running and countOfReplies > len(receivedEvents) and millis_now() < expire:
            try:
                content = self._receiver.get(True, 1)
                e = self.process_mail(content)
                receivedEvents.append(e)
            except Queue.Empty:
                time.sleep(0.1)
                continue

        if self.service_meta.callmode == "rpc":
            # special treatment for one response
            # no response
            if len(receivedEvents) == 0:
                return False, None

            # remote exception
            response = receivedEvents[0]
            if isinstance(response, Exception):
                raise response

            # success
            return True, response
        else:
            return len(receivedEvents) == countOfReplies, receivedEvents

    def process_mails(self):
        if not self._receiver:
            return

        while self.running:
            try:
                content = self._receiver.get(True, 1)
                self.process_mail(content)
            except Queue.Empty:
                pass

        self._receiver.close()
        for content in self._receiver.dump_cache():
            self.process_mail(content)

    def process_mail(self, mail_injson):
        if not mail_injson:
            return
        response_mail = Mail.from_json(mail_injson)
        try:
            response = extract_data_from_mail(response_mail)
        except Exception as err:
            response = err

        return response


    # useful handlers
    def notify(self, request, key="", block=True, timeout=10):
        assert self.service_meta.callmode == "notify"
        self.send(request, key, block, timeout)

    def rpc(self, request, key="", block=True, timeout=10):
        assert self.service_meta.callmode == "rpc"
        return self.send(request, key, block, timeout)

    def mrpc(self, request, key="", block=True, timeout=10):
        assert self.service_meta.callmode == "mrpc"
        return self.send(request, key, block ,timeout)



