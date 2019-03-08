#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import gevent
from babel_python.servicemeta import ServiceMeta
#from babel_python.babelrabbitmq_async import *
from babel_python import babelrabbitmq_async
from babel_python.util import millis_now
from babel_python.kombu import connection_closed 


from threathunter_common.rabbitmq.rmqctx import RabbitmqCtx

RabbitmqCtx.get_instance().amqp_url = 'pyamqp://%s:%s@%s:%s/' % ("admin", "threathunter.com.com", "172.16.10.77", 5672)
#RabbitmqCtx.get_instance().amqp_url = 'librabbitmq://%s:%s@%s:%s/' % ("admin", "threathunter.com.com", "172.16.10.77", 5672)

import os

os.environ.update({'KOMBU_LOG_CONNECTION': "True"})
import logging
logging.basicConfig(level=logging.DEBUG)
__author__ = 'lw'


def sleep(timeout):
    gevent.sleep(timeout)


class TestBabelRabbitmq:
    @classmethod
    def teardown_class(self):
        logging.error("enter tearDown....")
        connection_closed()

    def test_queue(self):
        queue_service = ServiceMeta.from_json("""
        {
            "name": "testqueue",
            "callmode": "notify",
            "delivermode": "queue",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "cdc": "testsh",
                "sdc": "testsh",
                "durable": true
            }
        }
        """)

        sender = babelrabbitmq_async.get_client_sender_for_queue(queue_service)
        recver = babelrabbitmq_async.get_server_receiver_for_queue(queue_service)
        try:
            recver.start_consuming()
            sender.put("test_msg", queue_service.name)
            print "queue size: ", recver.get_cached_number()
            msg = recver.get(timeout=2)
            assert msg == "test_msg"
        finally:
            sender.close()
            recver.close()

    def test_multiple_queue(self):
        queue_service = ServiceMeta.from_json("""
        {
            "name": "testmulqueue",
            "callmode": "notify",
            "delivermode": "queue",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "cdc": "testsh",
                "sdc": "testsh"
            }
        }
        """)

        sender = babelrabbitmq_async.get_client_sender_for_queue(queue_service)
        recver = babelrabbitmq_async.get_server_receiver_for_queue(queue_service, max_cache_size=20000)
        try:
            recver.start_consuming()
    
            round = 10000
            start = millis_now()
            for i in xrange(round):
                sender.put("test_msg", queue_service.name, block=False)
            while recver.get_cached_number() < round:
                a = (recver.get_cached_number())
                sleep(0.2)
            end = millis_now()
            dure = end - start
            dure /= 1000.0
            print "{} records spend {} seconds, {} per second".format(round, dure, round/dure)
    
        finally:
            sender.close()
            recver.close()
            assert False

    def test_topic_only(self):
        topic_service1 = ServiceMeta.from_json("""
        {
            "name": "testtopic",
            "callmode": "notify",
            "delivermode": "topic",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "cdc": "testsh",
                "sdc": "testsh",
                "serversubname": "consumer1"
            }
        }
        """)

        topic_service2 = ServiceMeta.from_json("""
        {
            "name": "testtopic",
            "callmode": "notify",
            "delivermode": "topic",
            "serverimpl": "rabbitmq",
            "coder": "mail",
            "options": {
                "cdc": "testsh",
                "sdc": "testsh",
                "serversubname": "consumer2"
            }
        }
        """)

        sender = babelrabbitmq_async.get_client_sender_for_topic(topic_service1)
        recver1 = babelrabbitmq_async.get_server_receiver_for_topic(topic_service1)
        recver2 = babelrabbitmq_async.get_server_receiver_for_topic(topic_service2)
        try:
            recver1.start_consuming()
            recver2.start_consuming()
    
            sender.put("test_msg", topic_service1.name)
            m1 = recver1.get(timeout=1)
            m2 = recver2.get(timeout=1)
            assert m1 == "test_msg"
            assert m2 == "test_msg"
        finally:
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
                    "cdc": "testsh",
                    "sdc": "testsh",
                    "serverseq": str(x)
                }
            }
        ), [1, 2, 3, 4])

        # start queue and receivers
        sender = babelrabbitmq_async.get_client_sender_for_sharding(services[0])
        receivers = map(
            lambda service: babelrabbitmq_async.get_server_receiver_for_sharding(service, max_cache_size=1000),  services)
        try:
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
        finally:
            sender.close()
            for receiver in receivers:
                receiver.close()

    def test_shuffle(self):
        # build services
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "testshuffle",
                               "callmode": "notify",
                               "delivermode": "shuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "cdc": "testsh",
                                   "sdc": "testsh"
                               }
                           }
                       ), [1, 2, 3, 4])

        # start queue and receivers
        sender = babelrabbitmq_async.get_client_sender_for_shuffle(services[0])
        receivers = map(
            lambda service: babelrabbitmq_async.get_server_receiver_for_shuffle(service, max_cache_size=1000),  services)
            
        try:
            for receiver in receivers:
                receiver.start_consuming()
            # first round, send 100 keys
            for i in range(100):
                key = str(i)
                sender.put(key, "testshuffle")
    
            sleep(1)
    
            counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
            print counters_first
    
            for c in counters_first:
                assert c > 0
        finally:
            sender.close()
            for receiver in receivers:
                receiver.close()

    def test_topic_shuffle(self):
        # build services
        # 2 services as a group, 2 groups
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "testtopicshuffle",
                               "callmode": "notify",
                               "delivermode": "topicshuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "cdc": "testsh",
                                   "sdc": "testsh",
                                   "serversubname": "consumer1"
                               }
                           }
                       ), [1, 2])
        services += map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "testtopicshuffle",
                               "callmode": "notify",
                               "delivermode": "topicshuffle",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "cdc": "testsh",
                                   "sdc": "testsh",
                                   "serversubname": "consumer2"
                               }
                           }
                       ), [1, 2])

        # start queue and receivers
        sender = babelrabbitmq_async.get_client_sender_for_topic_shuffle(services[0])
        receivers = map(
            lambda service: babelrabbitmq_async.get_server_receiver_for_topic_shuffle(service, max_cache_size=1000),  services)

        try:
            for receiver in receivers:
                receiver.start_consuming()
            
            # first round, send 100 keys
            for i in range(100):
                key = str(i)
                sender.put(key, "testtopicshuffle")
    
            sleep(1)
    
            counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
            print counters_first
    
            for c in counters_first:
                assert c > 0
        finally:
            sender.close()
            for receiver in receivers:
                receiver.close()


    def test_topic_sharding(self):
        # build services
        # 2 services as a group, 2 groups
        services = map(lambda x:
                       ServiceMeta.from_dict(
                           {
                               "name": "testtopicsharding",
                               "callmode": "notify",
                               "delivermode": "topicsharding",
                               "serverimpl": "rabbitmq",
                               "coder": "mail",
                               "options": {
                                   "cdc": "testsh",
                                   "sdc": "testsh",
                                   "serversubname": "consumer1",
                                   "serverseq": str(x)
                               }
                           }
                       ), [1, 2])
        services += map(lambda x:
                        ServiceMeta.from_dict(
                            {
                                "name": "testtopicsharding",
                                "callmode": "notify",
                                "delivermode": "topicsharding",
                                "serverimpl": "rabbitmq",
                                "coder": "mail",
                                "options": {
                                    "cdc": "testsh",
                                    "sdc": "testsh",
                                    "serversubname": "consumer2",
                                    "serverseq": str(x)
                                }
                            }
                        ), [1, 2, 3])

        # start queue and receivers
        sender = babelrabbitmq_async.get_client_sender_for_topic_sharding(services[0])
        receivers = map(
            lambda service: babelrabbitmq_async.get_server_receiver_for_topic_sharding(service, max_cache_size=1000),  services)

        try:
            for receiver in receivers:
                receiver.start_consuming()
            # first round, send 100 keys
            for i in range(100):
                key = str(i)
                sender.put(key, "testtopicsharding." + key)
    
            sleep(1)
    
            counters_first = map(lambda receiver: receiver.get_cached_number(), receivers)
            print counters_first
    
            for c in counters_first:
                assert c > 0
        finally:
            logging.error("Test is Closing !!!!!!!!")
            sender.close()
            try:
                for receiver in receivers:
                    receiver.close()
            except Exception:
                import traceback
                logging.error( traceback.format_exc())
