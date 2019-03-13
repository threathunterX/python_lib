#!/usr/bin/env python
# -*- encoding: utf-8 -*#
# referencing code in pyspider
from __future__ import absolute_import
import time, logging
import threading, Queue
try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

import pika
import pika.exceptions
from threathunter_common.rabbitmq.rmqctx import RabbitmqCtx
from threathunter_common.util import run_in_thread

__author__ = 'lw'

catch_error = 0
def catch_error(func):
    """Catch errors of rabbitmq then reconnect"""
    def wrap(self, *args, **kwargs):
        global catch_error
        try:
            func(self, *args, **kwargs)
        except Exception as e:
            catch_error += 1
            if catch_error <= 10 or catch_error % 1000 == 0:
                logging.error('RabbitMQ error: %r, reconnect.', e)
            self.reconnect()
            return func(self, *args, **kwargs)
        else:
            catch_error = 0
    return wrap


class PikaQueue(object):
    """
    A Queue like rabbitmq connector
    """

    Empty = Queue.Empty
    Full = Queue.Full

    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
        """
        Constructor for a PikaQueue.

        Not works with python 3. Default for python 2.

        amqp_url:   https://www.rabbitmq.com/uri-spec.html
        self_delete: remove the queue while stopped
        """
        if not isinstance(dc, list):
            dc = [dc]
        self.dc = dc
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.exchange_durable = kwargs.get("exchange_durable", True)
        self.queue = queue
        self.binding_key = binding_key
        self.queue_durable = kwargs.get("queue_durable", False)
        self.amqp_url = kwargs.get("amqp_url")
        if not self.amqp_url:
            self.amqp_url = RabbitmqCtx.get_instance().amqp_url
        self.self_delete = kwargs.get("self_delete", False)

        if not self.exchange:
            raise RuntimeError("exchange is required")

        self.lock = threading.RLock()
        self.consume_task = None
        self.reconnect()
        
    def _reconnect(self):
        """Reconnect to rabbitmq server"""
        self.connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self.channel = self.connection.channel()

        if self.queue:
            self.channel.queue_declare(queue=self.queue, durable=self.queue_durable, auto_delete=(not self.queue_durable))
            for dc in self.dc:
                full_ex = "{}.{}".format(dc, self.exchange)
                self.channel.exchange_declare(exchange=full_ex, type=self.exchange_type, durable=self.exchange_durable,
                                              auto_delete=(not self.exchange_durable))
                self.channel.queue_bind(queue=self.queue, exchange=full_ex, routing_key=self.binding_key)

        # reconnect successfully

    def reconnect(self):
        with self.lock:
            try:
                self._reconnect()
            except pika.exceptions.ChannelClosed:
                # try once more
                self._reconnect()

    @catch_error
    def qsize(self):
        with self.lock:
            if self.queue:
                ret = self.channel.queue_declare(self.queue, passive=True)
            else:
                return 0
        return ret.method.message_count

    def empty(self):
        if self.qsize() == 0:
            return True
        else:
            return False

    def full(self):
        if self.queue and self.max_queue_size and self.qsize() >= self.max_queue_size:
            return True
        else:
            return False

    @catch_error
    def close(self):
        with self.lock:
            if self.self_delete and self.queue:
                self.channel.queue_delete(queue=self.queue)
                self.queue = None
            if self.channel:
                self.channel.close()
                self.channel = None


class PikaQueueSender(PikaQueue):

    def __init__(self, dc, exchange, exchange_type, **kwargs):
        PikaQueue.__init__(self, dc, exchange, exchange_type, None, None, **kwargs)
        self.max_queue_size = kwargs.get("max_queue_size")
        self.lazy_limit = kwargs.get("lazy_limit")

        if self.queue and self.lazy_limit and self.max_queue_size:
            self.qsize_diff_limit = int(self.max_queue_size * 0.1)
        else:
            self.qsize_diff_limit = 0
        self.qsize_diff = 0
        self.local2server = Queue.Queue() #add by wxt 2015-12-16 解耦
        run_in_thread(self.put_to_server) # add by wxt 2015-12-16
        
    @catch_error
    def put(self, obj, routing_key, durable=False, block=True, timeout=None):
        if not block:
            return self.put_nowait(obj, routing_key)

        start_time = time.time()
        while True:
            try:
                return self.put_nowait(obj, routing_key, durable)
            except Queue.Full:
                if timeout:
                    lasted = time.time() - start_time
                    if timeout > lasted:
                        time.sleep(min(self.max_timeout, timeout - lasted))
                    else:
                        raise
                else:
                    time.sleep(self.max_timeout)

    @catch_error
    def put_nowait(self, obj, routing_key, durable=False):
        
        qsize = self.local2server.qsize()
        if qsize > 10000:  #add by wxt 2015-12-16
            if qsize % 500 == 1:
                logging.error('babel local2server queue size too large %s',qsize)
        else:
            self.local2server.put((obj,routing_key,durable,))


    def put_to_server(self):

        while True:
            try:
                data = self.local2server.get(timeout=1)
                obj = data[0]
                routing_key = data[1]
                durable = data[2]        
                with self.lock:
                    self.qsize_diff += 1
                    for dc in self.dc:
                        full_ex = "{}.{}".format(dc, self.exchange)

                        if durable:
                            self.channel.basic_publish(exchange=full_ex, routing_key=routing_key, body=obj,
                                                              properties=pika.BasicProperties(delivery_mode=2)) # make message persistent)
                        else:
                            self.channel.basic_publish(exchange=full_ex, routing_key=routing_key, body=obj)
            except Queue.Empty:
                continue
            except Exception,e:
                while 1:
                    try:
                        self.reconnect()
                        break
                    except Exception,e:
                        time.sleep(5)
                


class PikaQueueReceiver(PikaQueue):

    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
        PikaQueue.__init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs)
        self.max_cache_size = kwargs.get("max_cache_size", 1000)
        self.cache = Queue.Queue(maxsize=self.max_cache_size)
        self.full_errors = 0
        self.consumer_tag = None
        self.consume_task = None
        if not self.queue:
            raise RuntimeError("queue is required")
        self.running = False

    def backend_consuming(self):
        while self.running:
            try:
                while len(self.channel._consumers) and self.running:
                    self.channel.connection.process_data_events()
            except Exception as ignore:
                print ignore
                if self.running:
                    self._restore_consuming_during_server_outage()
        print 111

    def _restore_consuming_during_server_outage(self):
        while self.running:
            try:
                self._reconnect()
                if self.connection and not self.connection.is_closed:
                    self.consumer_tag = self.channel.basic_consume(self.on_message, queue=self.queue)
                    return
            except Exception as err:
                pass
            finally:
                time.sleep(1)

    @catch_error
    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received

        """
        # self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.running = True
        self.consumer_tag = self.channel.basic_consume(self.on_message, queue=self.queue)
        self.consume_task = run_in_thread(self.backend_consuming)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        with self.lock:
            self.running = False
            if self.consume_task:
                self.consume_task.join(timeout=2)
                self.consume_task = None
            if self.channel and self.consumer_tag:
                self.channel.basic_cancel(self.consumer_tag)
                self.consumer_tag = None

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        pass
        # self.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        try:
            self.cache.put(body)
            self.channel.basic_ack(basic_deliver.delivery_tag)
        except Queue.Full:
            self.full_errors += 1

    def get_errors_due_to_queue_full(self):
        return self.full_errors

    def get_nowait(self):
        return self.cache.get_nowait()

    def get(self, block=True, timeout=None):
        return self.cache.get(block, timeout)

    def get_cached_number(self):
        return self.cache.qsize()

    def dump_cache(self):
        old_cache = self.cache
        self.cache = Queue.Queue(maxsize=self.max_cache_size)
        result = list()
        for _ in range(old_cache.qsize()):
            try:
                result.append(old_cache.get_nowait())
            except Queue.Empty:
                continue
        return result

    def close(self):
        self.stop_consuming()
        PikaQueue.close(self)


class PikaTwoLayerQueueReceiver(PikaQueueReceiver):
    """
    For the exchange to exchange relaying routing
    """

    def __init__(self, dc, first_exchange, first_exchange_type, first_binding_key, second_exchange, second_exchange_type,
                 queue, binding_key, **kwargs):
        if not isinstance(dc, list):
            dc = [dc]
        self.first_exchange_dc = dc
        self.first_exchange = first_exchange
        self.first_exchange_type = first_exchange_type
        self.first_binding_key = first_binding_key
        self.first_exchange_durable = kwargs.get("first_exchange_durable", True)
        PikaQueueReceiver.__init__(self, [], second_exchange, second_exchange_type, queue, binding_key, **kwargs)

    def _reconnect(self):
        PikaQueue._reconnect(self)
        # ugly here,
        if self.queue:
            self.channel.queue_declare(queue=self.queue, durable=self.queue_durable, auto_delete=(not self.queue_durable))
            self.channel.exchange_declare(exchange=self.exchange, type=self.exchange_type, durable=self.exchange_durable,
                                          auto_delete=(not self.exchange_durable))
            self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.binding_key)
            for dc in self.first_exchange_dc:
                full_ex = "{}.{}".format(dc, self.first_exchange)
                self.channel.exchange_declare(exchange=full_ex, type=self.first_exchange_type,
                                              durable=self.first_exchange_durable, auto_delete=(not self.first_exchange_durable))
                self.channel.exchange_bind(source=full_ex, destination=self.exchange,
                                           routing_key=self.first_binding_key)
