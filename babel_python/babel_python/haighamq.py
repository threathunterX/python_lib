#!/usr/bin/env python
# -*- encoding: utf-8 -*#
# referencing code in pyspider
from __future__ import absolute_import

import socket, logging
import traceback

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

import gevent
from gevent import monkey
from gevent.event import Event
from gevent.queue import Queue

from haigha.connections.rabbit_connection import RabbitConnection
from haigha.message import Message
monkey.patch_all()

from .util import gen_uuid
from threathunter_common.rabbitmq.rmqctx import RabbitmqCtx

__author__ = "nebula"

logger = logging.getLogger('babel_python.haigha')

def catch_error(func):
    """Catch errors of rabbitmq then reconnect"""
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logging.error('RabbitMQ error: %r, reconnect.', e)
            self.reconnect()
            return func(self, *args, **kwargs)
    return wrap


connection = None
connection_task = None
connection_consumers = dict()


def frame_loop():
    global connection
    while True:
      connection.read_frames()
      gevent.sleep(0)


def connection_closed():
    global connection, connection_task
    connection = None
    if connection_task:
        connection_task.kill()
        connection_task = None


def get_connection(amqp_url):
    global connection
    if connection and not connection.closed:
        return connection

    parse = urlparse.urlparse(amqp_url)
    sock_opts = {
        (socket.IPPROTO_TCP, socket.TCP_NODELAY): 1
        }
    connection = RabbitConnection(logger=logger, debug=logging.WARN, user=parse.username, password=parse.password,
                                   vhost=parse.path, host=parse.hostname, hearbeat=None, port=parse.port,
                                   close_cb=connection_closed, sock_opts=sock_opts, transport="gevent")

    global connection_task
    connection_task = gevent.spawn(frame_loop)

    global connection_consumers
    if connection_consumers:
        for c in connection_consumers.itervalues():
            c._reconnect()
            c.start_consuming()
    return connection


class HaighaQueue(object):
    """
    A queue based on haigha
    """

    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
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
        self.channel = None

        if not self.exchange:
            raise RuntimeError("exchange is required")

        self.reconnect()

    def _reconnect(self):
        """Reconnect to rabbitmq server"""
        if self.channel and self.channel.connection and not self.channel.connection.closed and not self.channel.closed:
            return

        connection = get_connection(self.amqp_url)
        self.channel = connection.channel()
        self.channel.add_close_listener(self._channel_closed_cb)

        if self.queue:
            self.channel.queue.declare(self.queue, durable=self.queue_durable, auto_delete=(not self.queue_durable))
            for dc in self.dc:
                full_ex = "{}.{}".format(dc, self.exchange)
                self.channel.exchange.declare(full_ex, self.exchange_type, durable=self.exchange_durable,
                                              auto_delete=(not self.exchange_durable), cb=None)
                self.channel.queue.bind(self.queue, full_ex, routing_key=self.binding_key)

    def _channel_closed_cb(self, ch):
        logging.warn("channelo closed {}".format(ch))

    def reconnect(self):
        try:
            self._reconnect()
        except Exception as e:
            # try once more
            logging.warn("fail to reconnect %s", e)
            self._reconnect()

    @catch_error
    def close(self):
        if not self.channel or self.channel.closed:
            self.channel = None
            return
        if self.self_delete and self.queue:
            self.channel.queue.delete(queue=self.queue, if_unused=True)

        if self.channel:
            self.channel.close()
            self.channel = None


class HaighaQueueSender(HaighaQueue):

    def __init__(self, dc, exchange, exchange_type, **kwargs):
        HaighaQueue.__init__(self, dc, exchange, exchange_type, None, None, **kwargs)

    @catch_error
    def put(self, obj, routing_key, durable=False, block=True, timeout=None):
        if not block:
            return self.put_nowait(obj, routing_key, durable)

        if durable:
            msg = Message(body=obj, delivery_mode=2)
        else:
            msg = Message(body=obj)
        for dc in self.dc:
            full_ex = "{}.{}".format(dc, self.exchange)
            ev = Event()
            self.channel.publish_synchronous(msg, exchange=full_ex, routing_key=routing_key, cb=ev.set())
            ev.wait(timeout)

    @catch_error
    def put_nowait(self, obj, routing_key, durable=False):
        if durable:
            msg = Message(body=obj, delivery_mode=2)
        else:
            msg = Message(body=obj)
        for dc in self.dc:
            full_ex = "{}.{}".format(dc, self.exchange)
            self.channel.publish(msg, exchange=full_ex, routing_key=routing_key)


class HaighaQueueReceiver(HaighaQueue):

    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
        HaighaQueue.__init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs)
        self.max_cache_size = kwargs.get("max_cache_size", 1000)
        self.cache = Queue(maxsize=self.max_cache_size)
        self.full_errors = 0
        self.recvd = 0
        if not self.queue:
            raise RuntimeError("queue is required")
        self.uuid = gen_uuid()

    @catch_error
    def start_consuming(self):
        if self.channel and self.channel.connection and not self.channel.connection.closed and not self.channel.closed:
            self.channel.basic.consume(self.queue, consumer=self.on_message)
            global connection_consumers
            connection_consumers[self.uuid] = self
            gevent.sleep(1)

    def stop_consuming(self):
        global connection_consumers
        if self.uuid in connection_consumers:
            del connection_consumers[self.uuid]
        if self.channel and self.channel.connection and not self.channel.connection.closed and not self.channel.closed:
            self.channel.basic.cancel(consumer=self.on_message)

    def on_message(self, msg):
        try:
            self.recvd += 1
            if self.recvd % 100 == 0:
                gevent.sleep()
            self.cache.put(msg.body)
            # self.channel.basic.ack(msg.delivery_info["delivery_tag"])
        except gevent.queue.Full:
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
        self.cache = Queue(maxsize=self.max_cache_size)
        result = list()
        for _ in range(old_cache.qsize()):
            try:
                result.append(old_cache.get_nowait())
            except Queue.Empty:
                continue
        return result

    def close(self):
        self.stop_consuming()
        HaighaQueue.close(self)


class HaighaTwoLayerQueueReceiver(HaighaQueueReceiver):
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
        HaighaQueueReceiver.__init__(self, dc, second_exchange, second_exchange_type, queue, binding_key, **kwargs)

    def _reconnect(self):
        HaighaQueue._reconnect(self)
        if self.queue:
            self.channel.queue.declare(self.queue, durable=self.queue_durable, auto_delete=(not self.queue_durable))
            self.channel.exchange.declare(self.exchange, self.exchange_type, durable=self.exchange_durable,
                                          auto_delete=(not self.exchange_durable), cb=None)
            self.channel.queue.bind(self.queue, self.exchange, routing_key=self.binding_key)
            for dc in self.first_exchange_dc:
                full_ex = "{}.{}".format(dc, self.first_exchange)
                self.channel.exchange.declare(full_ex, self.first_exchange_type,
                                              durable=self.first_exchange_durable,
                                              auto_delete=(not self.first_exchange_durable))
                self.channel.exchange.bind(self.exchange, full_ex, routing_key=self.first_binding_key)
