#!/usr/bin/env python
# -*- coding: utf-8 -*-

from babel_python.redis import *

__author__ = "nebula"


class TestRabbitmq:

    def setup_method(self, method):
        RedisCtx.get_instance().host = "127.0.0.1"
        RedisCtx.get_instance().port = 6379
        self.list_name = "listtest"
        self.list_sender = RedisListSender(self.list_name)
        self.list_recver = RedisListReceiver(self.list_name)
        self.list_recver.start_consuming()

        self.pubsub_name = "pubsubtest"
        self.pubsub_sender = RedisPubSubSender(self.pubsub_name)
        self.pubsub_recver = RedisPubSubReceiver(self.pubsub_name)
        self.pubsub_recver.start_consuming()

        self.max_send = 100

    def teardown_method(self, method):
        self.list_recver.stop_consuming()
        self.list_recver.close()
        self.list_sender.close()

        self.pubsub_recver.stop_consuming()
        self.pubsub_recver.close()
        self.pubsub_sender.close()

    def test_list_simple_send_recv(self):
        msg = "test_message"
        self.list_sender.put(msg, "")
        assert msg == self.list_recver.get(True, 5)

    def test_pubsub_simple_send_recv(self):
        msg = "test_message"
        self.pubsub_sender.put(msg, "")
        assert msg == self.pubsub_recver.get(True, 5)

    def test_multiple_recv(self):
        msg = "test_message"
        for i in xrange(self.max_send):
            self.list_sender.put(msg, "")
            self.pubsub_sender.put(msg, "")

        import time
        time.sleep(2)

