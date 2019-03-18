#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from babel_python.servicemeta import ServiceMeta
from babel_python.babelrabbitmq import *
from babel_python.util import millis_now

__author__ = "nebula"


def sleep(timeout):
    time.sleep(timeout)


class TestBabelRabbitmq:

    def test_queue(self):
        queue_service = ServiceMeta.from_json("""
        {
            "name": "test",
            "callmode": "notify",
            "delivermode": "queue",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "sdc": "sh",
                "cdc": "sh"
            }
        }
        """)

        sender = get_client_sender_for_queue(queue_service)
        recver = get_server_receiver_for_queue(queue_service)
        recver.start_consuming()

        sender.put("test_msg", queue_service.name)
        assert recver.get(timeout=2) == "test_msg"

        sender.close()
        recver.close()

    def test_multiple_queue(self):
        queue_service = ServiceMeta.from_json("""
        {
            "name": "test",
            "callmode": "notify",
            "delivermode": "queue",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "sdc": "sh",
                "cdc": "sh"
            }
        }
        """)

        sender = get_client_sender_for_queue(queue_service)
        recver = get_server_receiver_for_queue(queue_service, max_cache_size=20000)
        recver.start_consuming()

        round = 1000
        start = millis_now()
        for i in xrange(round):
            sender.put("test_msg", queue_service.name, block=False)
        sleep(5)
        while recver.get_cached_number() < round:
            a = (recver.get_cached_number())
            sleep(0.2)
        end = millis_now()
        dure = end - start
        dure /= 1000.0
        print "{} records spend {} seconds, {} per second".format(round, dure, round/dure)

        sender.close()
        recver.close()

    def test_topic(self):
        topic_service1 = ServiceMeta.from_json("""
        {
            "name": "test",
            "callmode": "notify",
            "delivermode": "topic",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "sdc": "sh",
                "cdc": "sh",
                "serversubname": "consumer1"
            }
        }
        """)

        topic_service2 = ServiceMeta.from_json("""
        {
            "name": "test",
            "callmode": "notify",
            "delivermode": "topic",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "sdc": "sh",
                "cdc": "sh",
                "serversubname": "consumer2"
            }
        }
        """)

        sender = get_client_sender_for_topic(topic_service1)
        recver1 = get_server_receiver_for_topic(topic_service1)
        recver2 = get_server_receiver_for_topic(topic_service2)
        recver1.start_consuming()
        recver2.start_consuming()

        sender.put("test_msg", topic_service1.name)
        assert recver1.get(timeout=1) == "test_msg"
        assert recver2.get(timeout=1) == "test_msg"

        sender.close()
        recver1.close()
        recver2.close()

    def test_sharding(self):
        # build services
        services = map(lambda x:
            ServiceMeta.from_dict(
            {
                "name": "test",
                "callmode": "notify",
                "delivermode": "sharding",
                "serverimpl": "rabbitmq",
                "coder": "mail",
                "options": {
                    "sdc": "sh",
                    "cdc": "sh",
                    "serverseq": str(x)
                }
            }
        ), [1, 2, 3, 4])

        # start queue and receivers
        sender = get_client_sender_for_sharding(services[0])
        receivers = map(
            lambda service: get_server_receiver_for_sharding(service, max_cache_size=1000),  services)
        for receiver in receivers:
            receiver.start_consuming()

        # first round, send 100 keys
        for i in range(100):
            key = str(i)
            sender.put(key, key, block=False)

        sleep(1)

        counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
        print counters_first

        for c in counters_first:
            assert c > 0

        # dropping cache
        for receiver in receivers:
            receiver.dump_cache()

        # second round ,send 400 keys in range(0, 100)
        for i in range(400):
            key = str(i % 100)
            sender.put(key, key, block=False)

        sleep(5)
        counters_second = map(lambda receiver: receiver.get_cached_number(), receivers)
        print counters_second

        for counter_first, counter_second in zip(counters_first, counters_second):
            assert counter_first * 4 == counter_second

        sender.close()
        for receiver in receivers:
            receiver.close()

    def test_shuffle(self):
        # build services
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "test",
                               "callmode": "notify",
                               "delivermode": "shuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "sdc": "sh",
                                   "cdc": "sh"
                               }
                           }
                       ), [1, 2, 3, 4])

        # start queue and receivers
        sender = get_client_sender_for_shuffle(services[0])
        receivers = map(
            lambda service: get_server_receiver_for_shuffle(service, max_cache_size=1000),  services)
        for receiver in receivers:
            receiver.start_consuming()

        # first round, send 100 keys
        for i in range(100):
            key = str(i)
            sender.put(key, "test")

        sleep(1)

        counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
        print counters_first

        for c in counters_first:
            assert c > 0

        sender.close()
        for receiver in receivers:
            receiver.close()

    def test_topic_shuffle(self):
        # build services
        # 2 services as a group, 2 groups
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "test",
                               "callmode": "notify",
                               "delivermode": "topicshuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "sdc": "sh",
                                   "cdc": "sh",
                                   "serversubname": "consumer1"
                               }
                           }
                       ), [1, 2])
        services += map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "test",
                               "callmode": "notify",
                               "delivermode": "topicshuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "sdc": "sh",
                                   "cdc": "sh",
                                   "serversubname": "consumer2"
                               }
                           }
                       ), [1, 2])

        # start queue and receivers
        sender = get_client_sender_for_topic_shuffle(services[0])
        receivers = map(
            lambda service: get_server_receiver_for_topic_shuffle(service, max_cache_size=1000),  services)
        for receiver in receivers:
            receiver.start_consuming()

        # first round, send 100 keys
        for i in range(100):
            key = str(i)
            sender.put(key, "test")

        sleep(1)

        counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
        print counters_first

        for c in counters_first:
            assert c > 0

        sender.close()
        for receiver in receivers:
            receiver.close()


    def test_topic_sharding(self):
        # build services
        # 2 services as a group, 2 groups
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "test",
                               "callmode": "notify",
                               "delivermode": "topicsharding",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "sdc": "sh",
                                   "cdc": "sh",
                                   "serversubname": "consumer1",
                                   "serverseq": str(x)
                               }
                           }
                       ), [1, 2])
        services += map(lambda x:
                        ServiceMeta.from_dict(
                            {
                                "name": "test",
                                "callmode": "notify",
                                "delivermode": "topicsharding",
                                "serverimpl": "rabbitmq",
                                "coder": "mail",
                                "options": {
                                    "sdc": "sh",
                                    "cdc": "sh",
                                    "serversubname": "consumer2",
                                    "serverseq": str(x)
                                }
                            }
                        ), [1, 2, 3])

        # start queue and receivers
        sender = get_client_sender_for_topic_sharding(services[0])
        receivers = map(
            lambda service: get_server_receiver_for_topic_sharding(service, max_cache_size=1000),  services)
        for receiver in receivers:
            receiver.start_consuming()

        # first round, send 100 keys
        for i in range(100):
            key = str(i)
            sender.put(key, "test." + key)

        sleep(1)

        counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
        print counters_first

        for c in counters_first:
            assert c > 0

        sender.close()
        for receiver in receivers:
            receiver.close()
