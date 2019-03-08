#!/usr/bin/env python
# -*- encoding: utf-8 -*#
from __future__ import absolute_import

import logging, sys, traceback
from pprint import pformat

from kombu import Connection, Exchange, Queue, Producer, Message
from kombu.simple import SimpleQueue, Empty
from kombu.transport import supports_librabbitmq
from kombu.exceptions import ConnectionError, ChannelError
import gevent

from .util import gen_uuid
from threathunter_common.rabbitmq.rmqctx import RabbitmqCtx

logger = logging.getLogger('babel_python.kombu')

def recover_connect(func):
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except ConnectionError as e:
            logger.error("Pro")
            logging.error('RabbitMQ error: %r, reconnect.', e)
            return func(self, *args, **kwargs)
    return wrap

connection = None
connection_task = None
connection_consumers = dict()

LOCAL_DEBUG = False
def connection_ensure_fail(exc, interval):
    logger.error("ensure connection fail.. Exception: %s,\
                 message: %s, will sleep for %s", exc, exc.message, interval)

def debug_message(msg):
    if not isinstance(msg, Message):
        return
    logger.debug('Received message: %r dir: %s',msg, dir(msg))
    logger.debug('  properties:\n %s', pretty(msg.properties))
    logger.debug('  delivery_info:\n %s', pretty(msg.delivery_info))
    logger.debug("  body:\n %s", pretty(msg.body if isinstance(msg.body, buffer) else str(msg.body)))
    logger.debug("  payload:\n %s", pretty(msg.payload if isinstance(msg.payload, buffer) else str(msg.payload)))
    
def connection_closed():
    global connection
    if connection.connected:
        logger.info("Trigger connection to close!!!!!")
        connection.close()
    else:
        logger.info("Connection is not connected, pass.")

def get_connection(amqp_url, heartbeat=60.0):
    global connection
    if connection:
        if connection.connected:
            return connection
        else:
            # block whole process?
            # connectied first then close rabbitmq, then test if block, then start rabbitmq, then test if recover
            return connection.ensure_connection(
                errback=connection_ensure_fail,
                max_retries=3)

    if LOCAL_DEBUG:
        connection = Connection(amqp_url, heartbeat=heartbeat, transport="memory")
    else:
        connection = Connection(amqp_url, heartbeat=heartbeat)#transport_options=dict(heartbeat=60.0))
    if logger.level <= logging.DEBUG:
        connection._logger = True
    
    connection.connect()
    logger.debug("Babel connection Info: %s" % connection.info())
    if connection.transport_cls == "amqp" and supports_librabbitmq():
        logger.debug("Babel transport way is %s", "librabbitmq")
    else:
        logger.debug("Babel transport way is %s", connection.transport_cls)
    logger.info("""Babel connection support heartbeat? %s ,server heartbeat: %s,
                client heartbeat:%s, final heartbeat: %s, heartbeat interval: %s""" %(
                    connection.supports_heartbeats,
                    getattr(connection.connection,"server_heartbeat",None),
                    getattr(connection.connection, "client_heartbeat", None),
                    getattr(connection.connection, "heartbeat", None),
                    connection.get_heartbeat_interval()))

#    global connection_consumers
#    if connection_consumers:
#        for c in connection_consumers.itervalues():
#            c._reconnect()
#            c.start_consuming()

    return connection

def pretty(obj):
    return pformat(obj, indent=4)

class KombuMessageQueue(object):
    """
    A queue based on kombu
    """
    class_name = None
    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
        if not isinstance(dc, list):
            dc = [dc]
        self.dc = dc
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.exchange_durable = kwargs.get("exchange_durable", True)
        self.exchange_opts = dict(durable=self.exchange_durable,
                                  auto_delete=not self.exchange_durable,
                                  type=self.exchange_type)
        self.queue = queue
        self.binding_key = binding_key
        self.queue_durable = kwargs.get("queue_durable", False)
        self.queue_opts = dict(durable=self.queue_durable,
                               auto_delete=not self.queue_durable,
                               routing_key=self.binding_key)
        self.amqp_url = kwargs.get("amqp_url")
        if not self.amqp_url:
            self.amqp_url = RabbitmqCtx.get_instance().amqp_url
        self.self_delete = kwargs.get("self_delete", False)
        self.channel = None
        self.queue_obj = None
        self.closed = True
        if not self.exchange:
            raise RuntimeError("exchange is required")
        self.Exchange_objs = set()
        self.Exchange_dict = dict()

        self.connect()

    def _reconnect(self):
        """Reconnect to rabbitmq server"""
        if self.channel and self.channel.connection and not self.channel.connection.connected:# and not self.channel.closed:
            return
        global connection
        if connection.connected:
            connection.close()
        get_connection()

    def connect(self):
        self._connect()

    def _connect(self):
        connection = get_connection(self.amqp_url)
        self.channel = connection.channel()
            # nowait, useful?
            # arguments ..
        logger.debug("{class_name}[{id:#x}] is build Communicate framework..".format(class_name=self.class_name, id=id(self)))
        for dc in self.dc:
            full_ex = "{}.{}".format(dc, self.exchange)
            ex = Exchange(full_ex, channel=self.channel, **self.exchange_opts)
            # abstract.py line 67, __call__ will do bind
            ex(self.channel).declare()


            if not ex.is_bound:
                logger.error("Exchange[{id:#x}] {exchange} is not bound to chan {chan_id}".format(id=id(ex), exchange=ex, chan_id=self.channel.channel_id))
            logger.debug("{class_name}[{id:#x}] create Exchange[{eid:#x}]".format(
                class_name=self.class_name, id=id(self),
                eid=id(ex)))
            logger.debug("Exchange[{id:#x}] build opts: {e_opts}".format(
                id=id(ex), e_opts=self.exchange_opts))
            logger.debug("Exchange[{id:#x}] opts: {e_opts}".format(
                id=id(ex), e_opts=dict(durable=ex.durable,
                                       type=ex.type,
                                       auto_delete=ex.auto_delete))
                     )

            self.Exchange_objs.add(ex)
            self.Exchange_dict[full_ex] = ex
        if self.queue:
            self.Queue_obj = Queue(self.queue, self.Exchange_objs,channel=self.channel,
                                   **self.queue_opts)
            self.Queue_obj.declare()
            for ex in self.Exchange_objs:
                self.Queue_obj.bind_to(ex, routing_key=self.binding_key)
                
            logger.debug("{class_name}[{id:#x}] create Queue obj[{qid:#x}]: {q}".format(
                class_name=self.class_name, id=id(self),
                qid=id(self.Queue_obj), q=self.Queue_obj))
            logger.debug("Queue obj[{id:#x}] build opts: {q_opts}".format(
                id=id(self.Queue_obj), q_opts=self.queue_opts))
            logger.debug("Queue obj[{id:#x}] opts: {q_opts}".format(
                id=id(self.Queue_obj), q_opts=dict(durable=self.Queue_obj.durable,
                                                   routing_key=self.Queue_obj.routing_key,
                                                   auto_delete=self.Queue_obj.auto_delete))
                     )
            
        self.closed = False
        
    def reconnect(self):
        try:
            self._reconnect()
        except Exception:
            # try once more
            logger.warn("----- Babel fail to reconnect : \n%s", sys.exc_info())
            self._reconnect()
    
    def __del__(self):
        logger.debug("{cn} default del method".format(cn=self.class_name))
        if not self.closed:
            logger.debug("=============== {class_name}[{id:#x}] Enter closing... ".format(
                class_name=self.class_name,id=id(self)))
            self.close()

    def close(self):
        logger.debug("{cn}[{id:#x}] is calling KombuMessageQueue close method".format(
            cn=self.class_name, id=id(self)))
        try:
            for ex in self.Exchange_objs:
                if not ex.durable:
                    r = ex.delete()
                    logger.debug("{cn}[{id:#x}] delete {exchange}[{eid:#x}], return {r}".format(cn=self.class_name, id=id(self), exchange=ex, eid=id(ex), r=r))
            if self.self_delete and self.queue:
                r = self.channel.queue_delete(queue=self.queue, if_unused=True)
                logger.debug("delete queue {q} return {r}".format(q=self.queue, r=r))
            
            global connection
            if self.channel and connection and connection.connected:
                self.channel.close()
                self.channel = None
        except Exception:
            logger.error("!!! Babel Error when close {class_name}[{id:#x}]:\n{exc_info}".format(
                class_name=self.class_name, id=id(self), exc_info=traceback.format_exc()))
        finally:
            self.closed = True

class KombuQueueSender(KombuMessageQueue):

    def __init__(self, dc, exchange, exchange_type, **kwargs):
        self.class_name = "KombuQueueSender"
        KombuMessageQueue.__init__(self, dc, exchange, exchange_type, None, None, **kwargs)
        self.producers = list()
        for ex in self.Exchange_objs:
            self.producers.append( Producer(self.channel, ex))
            
        # debug Sender's producers' info
        for p in self.producers:
            logger.debug("""{class_name}[{id:#x}](chan {chan_id}) have producer[{pid:#x}]:
                         (exchange :{exchange} bound to chan {chan_id} ? {bound}""".format(
                             class_name=self.class_name, id=id(self), 
                             pid=id(p), exchange=p.exchange,chan_id=p.channel.channel_id,
                             bound=p.exchange.is_bound))

    def on_return(self, exception, exchange, routing_key, message):
        logger.error("""Publish raise Exception when {exchange}[eid:#x] use
                     routing_key: {rk} to send message: {msg}, Exception:\n{info}""".format(
                         eid=id(exchange), exchange=exchange, rk=routing_key, msg=message,
                         info=exception))

    def put(self, obj, routing_key, durable=False, block=True, timeout=None):
        # @totest
#        immediate = False
#        if not block:
#            immediate = True
        for producer in self.producers:
            try:
                # retry_policy @todo
                if durable:
                    re = producer.publish(obj, routing_key=routing_key,delivery_mode=2,
                                          timeout=timeout, retry=True, on_return=self.on_return)
                else:
                    re = producer.publish(obj, routing_key=routing_key,
                                          timeout=timeout, retry=True, on_return=self.on_return)
                # publish always return None?
#                logger.debug("publish return :%s", re)

            except Exception:
                logger.error("Producer publish exception:%s, routing_key:%s , cause: %s" % (producer, routing_key, traceback.format_exc()))

    def put_nowait(self, obj, routing_key, durable=False):
        self.put(obj, routing_key, durable, block=False)

class KombuQueueReceiver(KombuMessageQueue):

    def __init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs):
        if self.class_name is None:
            self.class_name = "KombuQueueReceiver"
        KombuMessageQueue.__init__(self, dc, exchange, exchange_type, queue, binding_key, **kwargs)
        self.queue_obj = SimpleQueue(self.channel, self.Queue_obj,)

        self.uuid = gen_uuid()
        if not self.queue:
            raise RuntimeError("queue is required")
        
    def start_consuming(self):
        pass

    def dump_cache(self):
        self.queue_obj.clear()

    def stop_consuming(self):
        self.queue_obj.close()

    def get_cached_number(self):
        return self.queue_obj.qsize()

    def get_nowait(self):
        try:
            log_message = self.queue_obj.get_nowait()
        except Empty:
            return None
        log_message.ack()
        p = log_message.payload # deserialized data.        
        if isinstance(p, buffer):
            p = unicode(p)
        return p

    def get(self, block=True, timeout=None):
        logger.debug("Enter Message get callback method..")
        try:
            log_message = self.queue_obj.get(block=True, timeout=1)
#            logger.debug("get return : %s, dir: %s, type: %s", log_message, dir(log_message), type(log_message))
            debug_message(log_message)
        except Empty:
            logger.error("Empty error when get @todo infos..")
            return
        p = log_message.payload # deserialized data.
        log_message.ack() # remove message from queue        
        if isinstance(p, buffer):
            p = unicode(p)
        return p

    def __del__(self):
        if not self.closed:
            logger.debug("=============== {class_name}[{id:#x}] Enter closing... ".format(
                class_name=self.class_name,id=id(self)))
            self.close()

    def close(self):
        logger.debug("{cn}[{id:#x}] is calling KombuQueueReceiver close method".format(
            cn=self.class_name, id=id(self)))
        try:
            logger.debug("{cn}[{id:#x}] is about to closing.. Queue size: {qsize}".format(
                cn=self.class_name, id=id(self), qsize=self.get_cached_number()))
            # clean Rabbitmq Queue
            self.dump_cache()
            logger.debug("{cn}[{id:#x}] is cleared. Queue size: {qsize}".format(
                cn=self.class_name, id=id(self), qsize=self.get_cached_number()))
            # stop all active consumers
            self.stop_consuming()
            # how to measure stop all consumers @todo
            # delete Rabbitmq Queue
            r = self.Queue_obj.delete()
            logger.debug("{cn}[{id:#x}] delete Queue[{qid:#x}] return: {r}".format(
                cn=self.class_name, id=id(self), qid=id(self.Queue_obj), r=r))
        except ChannelError as e:
            if '404' not in e.message:
                logger.error("Error when {cn}[{id:#x}] close: {msg}".format(
                    cn=self.class_name, id=id(self), msg=traceback.format_exc()))
        KombuMessageQueue.close(self)

class KombuTwoLayerQueueReceiver(KombuQueueReceiver):
    """
    For the exchange to exchange relaying routing
    """

    def __init__(self, dc, first_exchange, first_exchange_type, first_binding_key,
                 second_exchange, second_exchange_type,
                 queue, binding_key, **kwargs):
        if not isinstance(dc, list):
            dc = [dc]
        self.class_name = "KombuTwoLayerQueueReceiver"
        self.first_exchange_dc = dc
        self.first_exchange = first_exchange
        self.first_exchange_type = first_exchange_type
        self.first_binding_key = first_binding_key
        self.first_exchange_durable = kwargs.get("first_exchange_durable", True)
        self.first_exchange_opts = dict(durable=self.first_exchange_durable,
                                        auto_delete=not self.first_exchange_durable,
                                        type=self.first_exchange_type,
                                        routing_key=self.first_binding_key
                                    )
        self.first_Exchange_objs = set()
        self.first_Exchange_dict = dict()
        KombuQueueReceiver.__init__(self, dc, second_exchange, second_exchange_type, queue, binding_key, **kwargs)

    def _connect(self):
        KombuMessageQueue._connect(self)
        if self.queue:
            for dc in self.first_exchange_dc:
                full_ex = "{}.{}".format(dc, self.first_exchange)
                ex = Exchange(full_ex, channel=self.channel, **self.first_exchange_opts)
                ex(self.channel).declare()
                logger.debug("{cn}[{id:#x}] create first layer Exchange[{eid:#x}]: {ex}".format(cn=self.class_name, id=id(self), eid=id(ex), ex=ex))
                logger.debug("First layer Exchange[{eid:#x}] build opts: {e_opts}".format(eid=id(ex), e_opts=self.first_exchange_opts))
                logger.debug("First layer Exchange[{id:#x}] opts: {e_opts}".format(
                    id=id(ex), e_opts=dict(durable=ex.durable,
                                           type=ex.type,
                                           auto_delete=ex.auto_delete))
                         )
                
                self.first_Exchange_objs.add(ex)
                self.first_Exchange_dict[full_ex] = ex
                self.Queue_obj.bind_to(ex, routing_key=self.binding_key)
                if not ex.is_bound:
                    logger.error("Exchange[{eid:#x}] {ex} is not bound to chan {chan_id}".format(eid=id(ex), ex=ex, chan_id=self.channel.channel_id))
    def __del__(self):
        if not self.closed:
            logger.debug("=============== {class_name}[{id:#x}] Enter closing... ".format(
                class_name=self.class_name,id=id(self)))
            self.close()
    def close(self):
        logger.debug("{cn} is closing..".format(cn=self.class_name))
        KombuQueueReceiver.close(self)
