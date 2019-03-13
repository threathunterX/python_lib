#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import hashlib
import mmh3

from .rabbitmq import PikaQueueSender, PikaQueueReceiver, PikaTwoLayerQueueReceiver

__author__ = 'lw'


"""
Helper method on rabbitmq for babel.

Each types of deliver mode has different senders and receivers:
queue	          _queue	            direct	       $sname	        $sname
topic             _topic	            topic	       $sname.$sub	    $sname
sharding	      _sharding.$sname	    conhash	       $sname.$seq	    $key
shuffle	          _shuffle	            direct         $sname	        $sname
topic+shuffle	  _toffle	            topic	       $sname.$sub      $sname
topic+sharding	  _toshard.$sname+$sname.$sub	fanout+conhash  $sname.$sub.$seq	$key(bucket number)

each rpc client has a specific queue
                  _client	            direct	       $cliname	        $cliname
pls see $wiki/babel/rabbitmq for latest information
"""


SHARD_DATA = [60, 29, 84, 22, 17,
              63, 18, 20, 24, 11,
              64, 68, 96, 12, 62,
              92, 59, 56, 14, 15]


def get_hash(raw):
    if not raw:
        return ""

    md5 = hashlib.md5()
    md5.update(raw)
    d = md5.digest()
    return (ord(d[3]) << 24) + (ord(d[2]) << 16) + (ord(d[1]) << 8) + ord(d[0])


def decide_durable(service_meta, kwargs):
    durable = service_meta.options.get("durable", False)
    if durable:
        kwargs["exchange_durable"] = True
        kwargs["first_exchange_durable"] = True
        kwargs["queue_durable"] = True
        kwargs["self_delete"] = False
    return durable


def get_sdc(service_meta):
    return service_meta.options["sdc"].split(",")


def get_cdc(service_meta):
    return service_meta.options["cdc"].split(",")


def get_client_sender_for_queue(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)

    ex = "_queue"
    return PikaQueueSender(get_sdc(service_meta), ex, "direct", **kwargs)


def get_server_receiver_for_queue(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)

    ex = "_queue"
    return PikaQueueReceiver(get_sdc(service_meta), ex, "direct", service_meta.name, service_meta.name, **kwargs)


def get_client_sender_for_topic(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_topic"
    return PikaQueueSender(get_sdc(service_meta), ex, "topic", **kwargs)


def get_server_receiver_for_topic(service_meta, **kwargs):
    subname = service_meta.options.get("serversubname")
    if not subname:
        raise RuntimeError("the topic service provider should have subname")

    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_topic"
    return PikaQueueReceiver(get_sdc(service_meta), ex, "topic", "{}.{}".format(service_meta.name, subname), service_meta.name, **kwargs)


def get_client_sender_for_sharding(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", False)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)

    ex = "_sharding.{}".format(service_meta.name)
    return PikaQueueSender(get_sdc(service_meta), ex, "x-consistent-hash", **kwargs)


def get_server_receiver_for_sharding(service_meta, **kwargs):
    seq = service_meta.options.get("serverseq")
    if not seq:
        raise RuntimeError("the sharding service provider should have sequence")

    kwargs.setdefault("exchange_durable", False)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_sharding.{}".format(service_meta.name)
    return PikaQueueReceiver(get_sdc(service_meta), ex, "x-consistent-hash",
                             "{}.{}".format(service_meta.name, seq), "100", **kwargs)


def get_client_sender_for_shuffle(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_shuffle"
    return PikaQueueSender(get_sdc(service_meta), ex, "direct", **kwargs)


def get_server_receiver_for_shuffle(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_shuffle"
    return PikaQueueReceiver(get_sdc(service_meta), ex, "direct", service_meta.name, service_meta.name, **kwargs)


def get_client_sender_for_topic_shuffle(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_toffle"
    return PikaQueueSender(get_sdc(service_meta), ex, "topic", **kwargs)


def get_server_receiver_for_topic_shuffle(service_meta, **kwargs):
    subname = service_meta.options.get("serversubname")
    if not subname:
        raise RuntimeError("the topic shuffle service provider should have subname")

    kwargs.setdefault("exchange_durable", True)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_toffle"
    return PikaQueueReceiver(get_sdc(service_meta), ex, "topic", "{}.{}".format(service_meta.name, subname), service_meta.name, **kwargs)


def get_client_sender_for_topic_sharding(service_meta, **kwargs):
    kwargs.setdefault("exchange_durable", False)
    kwargs.setdefault("first_exchange_durable", False)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_toshard.{}".format(service_meta.name)
    return PikaQueueSender(get_sdc(service_meta), ex, "fanout", **kwargs)


def get_server_receiver_for_topic_sharding(service_meta, **kwargs):
    name = service_meta.name
    subname = service_meta.options.get("serversubname")
    if not subname:
        raise RuntimeError("the topic shuffle service provider should have subname")
    seq = service_meta.options.get("serverseq")
    if not seq:
        raise RuntimeError("the sharding service provider should have sequence")

    kwargs.setdefault("exchange_durable", False)
    kwargs.setdefault("first_exchange_durable", False)
    kwargs.setdefault("queue_durable", False)
    decide_durable(service_meta, kwargs)
    ex = "_toshard.{}".format(service_meta.name)
    return PikaTwoLayerQueueReceiver(get_sdc(service_meta), ex, "fanout", "*",
                                     "{}.{}".format(name, subname), "x-consistent-hash", "{}.{}.{}".format(name, subname, seq),
                                     "100", **kwargs)


def get_client_receiver_for_rpc_client(cdc, clientid, **kwargs):
    kwargs["exchange_durable"] = True
    kwargs["queue_durable"] = False
    return PikaQueueReceiver([cdc], "_client", "direct", clientid, clientid, **kwargs)


def get_server_sender_for_rpc_server(cdc, clientid, **kwargs):
    kwargs["queue_durable"] = False
    return PikaQueueSender([cdc], "_client", "direct", **kwargs)


####################################################


def get_client_sender(service_meta, **kwargs):
    return RabbitmqClientSender(service_meta, **kwargs)


class RabbitmqClientSender(object):
    """
    Used to send request by client.

    """

    def __init__(self, service_meta, **kwargs):
        self.service_meta = service_meta
        self.durable = self.service_meta.options.get("durable", False)
        if service_meta.delivermode == "queue":
            self._sender = get_client_sender_for_queue(service_meta, **kwargs)
        elif service_meta.delivermode == "topic":
            self._sender = get_client_sender_for_topic(service_meta, **kwargs)
        elif service_meta.delivermode == "sharding":
            self._sender = get_client_sender_for_sharding(service_meta, **kwargs)
        elif service_meta.delivermode == "shuffle":
            self._sender = get_client_sender_for_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "topicshuffle":
            self._sender = get_client_sender_for_topic_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "topicsharding":
            self._sender = get_client_sender_for_topic_sharding(service_meta, **kwargs)
        else:
            raise RuntimeError("invalid queue deliver mode")

    def send(self, content, key, blocking=True, timeout=5):
        routing_key = None
        if self.service_meta.delivermode in {"queue", "topic", "shuffle", "topicshuffle"}:
            routing_key = self.service_meta.name
        elif self.service_meta.delivermode == "sharding":
            routing_key = key
        elif self.service_meta.delivermode == "topicsharding":
            routing_key = "{}.{}".format(self.service_meta.name, key)

        self._sender.put(content, routing_key, self.durable, blocking, timeout)

    def get_sharding_key(self, key):
        return str(mmh3.hash(key))

    def close(self):
        if self._sender:
            self._sender.close()
            self._sender = None


def get_server_receiver(service_meta, **kwargs):
    return RabbitmqServerReceiver(service_meta, **kwargs)


class RabbitmqServerReceiver(object):
    """
    Used to receive request from client.

    """

    def __init__(self, service_meta, **kwargs):
        self.service_meta = service_meta
        if service_meta.delivermode == "queue":
            self._receiver = get_server_receiver_for_queue(service_meta, **kwargs)
        elif service_meta.delivermode == "topic":
            self._receiver = get_server_receiver_for_topic(service_meta, **kwargs)
        elif service_meta.delivermode == "sharding":
            self._receiver = get_server_receiver_for_sharding(service_meta, **kwargs)
        elif service_meta.delivermode == "shuffle":
            self._receiver = get_server_receiver_for_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "topicshuffle":
            self._receiver = get_server_receiver_for_topic_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "topicsharding":
            self._receiver = get_server_receiver_for_topic_sharding(service_meta, **kwargs)
        else:
            raise RuntimeError("invalid queue deliver mode")

    def start_consuming(self):
        return self._receiver.start_consuming()

    def stop_consuming(self):
        return self._receiver.stop_consuming()

    def get_errors_due_to_queue_full(self):
        return self._receiver.get_errors_due_to_queue_full()

    def get_nowait(self):
        return self._receiver.get_nowait()

    def get(self, block=True, timeout=None):
        return self._receiver.get(block, timeout)

    def get_cached_number(self):
        return self._receiver.get_cached_number()

    def dump_cache(self):
        return self._receiver.dump_cache()

    def close(self):
        return self._receiver.close()


def get_client_receiver(service_meta, clientid, **kwargs):
    return RabbitmqClientReceiver(service_meta, clientid, **kwargs)


class RabbitmqClientReceiver(object):
    """
    Used to receive response from server.

    """

    def __init__(self, service_meta, clientid, **kwargs):
        kwargs["queue_durable"] = False
        self.service_meta = service_meta
        self._receiver = get_client_receiver_for_rpc_client(service_meta.options["cdc"], clientid, **kwargs)

    def start_consuming(self):
        return self._receiver.start_consuming()

    def stop_consuming(self):
        return self._receiver.stop_consuming()

    def get_errors_due_to_queue_full(self):
        return self._receiver.get_errors_due_to_queue_full()

    def get_nowait(self):
        return self._receiver.get_nowait()

    def get(self, block=True, timeout=None):
        return self._receiver.get(block, timeout)

    def get_cached_number(self):
        return self._receiver.get_cached_number()

    def dump_cache(self):
        return self._receiver.dump_cache()

    def close(self):
        return self._receiver.close()


def get_server_sender(service_meta, clientid, **kwargs):
    return RabbitmqServerSender(service_meta, clientid, **kwargs)


class RabbitmqServerSender(object):
    """
    Used to send response by server.

    """

    def __init__(self, service_meta, clientid, **kwargs):
        self._sender = get_server_sender_for_rpc_server(service_meta, clientid, **kwargs)
        self.clientid = clientid

    def send(self, content, key, blocking=True, timeout=5):
        self._sender.put(content, self.clientid, False, blocking, timeout)

    def get_sharding_key(self, key):
        # response doesn't need to recalculate the key
        return key

    def close(self):
        return self._sender.close()


def build_rmq_binding(first_ex, first_ex_type, second_ex, second_ex_type):
    pass
