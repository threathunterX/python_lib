#!/usr/bin/env python
# -*- coding: utf-8 -*-

from babel_python.haighamq import *

__author__ = 'lw'


class TestHaighamqk:

    def setup_method(self, method):
        self.queue = "test"
        self.exchange = "test"
        self.exchange_type = "direct"
        self.routing_key = "test"
        self.max_send = 100
        self.max_recv = 10
        self.recver = HaighaQueueReceiver("sh", self.exchange, self.exchange_type, self.queue, self.routing_key,
                                          exchange_durable=False, queue_durable=False, max_cache_size=10, self_delete=True)
        self.sender = HaighaQueueSender("sh", self.exchange, self.exchange_type, exchange_durable=False, queue_durable=False,
                                        self_delete=True, amqp_url="amqp://guest:guest@localhost:5672/")
        self.recver.start_consuming()

    def teardown_method(self, method):
        self.recver.stop_consuming()
        self.sender.close()
        self.recver.close()

    def test_simple_send_recv(self):
        msg = "test_message"
        self.sender.put(msg, self.routing_key, block=False)
        received = self.recver.get(True, 2)
        assert msg == received

    def test_multiple_recv(self):
        msg = "test_message"
        for i in xrange(self.max_send):
            self.sender.put(msg, self.routing_key, block=False)



