#!/usr/bin/env python
# -*- coding: utf-8 -*-

from babel_python.rabbitmq import *

__author__ = 'lw'


class TestRabbitmq:

    def setup_method(self, method):
        self.queue = "test"
        self.exchange = "test"
        self.exchange_type = "direct"
        self.routing_key = "test"
        self.max_send = 100
        self.max_recv = 10
        self.sender = PikaQueueSender("sh", self.exchange, self.exchange_type, exchange_durable=False,
                                      queue_durable=False, max_queue_size=self.max_send, lazy_limit=True, self_delete=True)
        self.recver = PikaQueueReceiver("sh", self.exchange, self.exchange_type, self.queue, self.routing_key, exchange_durable=False,
                                        queue_durable=False, max_cache_size=self.max_recv, self_delete=True)
        self.recver.start_consuming()

    def teardown_method(self, method):
        self.recver.stop_consuming()
        self.sender.close()
        self.recver.close()

    def test_simple_send_recv(self):
        msg = "test_message"
        self.sender.put(msg, self.routing_key)
        assert msg == self.recver.get(True, 5)

    def test_multiple_recv(self):
        msg = "test_message"
        for i in xrange(self.max_send):
            self.sender.put(msg, self.routing_key)

        import time
        time.sleep(2)

        # assert self.recver.get_errors_due_to_queue_full() == self.max_send - self.max_recv



