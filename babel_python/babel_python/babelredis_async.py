#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .redis_async import (RedisListSender, RedisListReceiver, RedisPubSubSender,
                          RedisPubSubReceiver, RedisMultipleListSender)

__author__ = "nebula"


"""
Helper method on redis for babel.

Each types of deliver mode has different senders and receivers:
queue	          list      $sname
topic             pubsub    $sname

Redis implementation doesn't support sharding/shuffle related delivery mode for now

each rpc client has a specific queue
client            list      $clientid
pls see $wiki/babel/redis for latest information
"""


def get_client_sender_for_queue(service_meta, **kwargs):
    return RedisListSender(service_meta.name, **kwargs)


def get_server_receiver_for_queue(service_meta, **kwargs):
    return RedisListReceiver(service_meta.name, **kwargs)


def get_client_sender_for_topic(service_meta, **kwargs):
    return RedisPubSubSender(service_meta.name, **kwargs)


def get_server_receiver_for_topic(service_meta, **kwargs):
    return RedisPubSubReceiver(service_meta.name, **kwargs)


def get_client_sender_for_shuffle(service_meta, **kwargs):
    return RedisListSender(service_meta.name, **kwargs)


def get_server_receiver_for_shuffle(service_meta, **kwargs):
    return RedisListReceiver(service_meta.name, **kwargs)


def get_client_sender_for_sharding(service_meta, **kwargs):
    cardinality = service_meta.options.get("servercardinality")
    if not cardinality:
        raise RuntimeError("the sharding service provider should have cardinality")
    cardinality = int(cardinality)
    names = ["{}.{}".format(service_meta.name, _) for _ in range(1, cardinality+1)]
    return RedisMultipleListSender(names, **kwargs)


def get_server_receiver_for_sharding(service_meta, **kwargs):
    seq = service_meta.options.get("serverseq")
    if not seq:
        raise RuntimeError("the sharding service provider should have sequence")
    return RedisListReceiver("{}.{}".format(service_meta.name, seq), **kwargs)


def get_client_receiver_for_rpc_client(service_meta, clientid, **kwargs):
    return RedisListReceiver(clientid, **kwargs)


def get_server_sender_for_rpc_server(service_meta, clientid, **kwargs):
    return RedisListSender(clientid, **kwargs)


###########################################################


def get_client_sender(service_meta, **kwargs):
    return RedisClientSender(service_meta, **kwargs)


class RedisClientSender(object):
    """
    Used to send request by client.

    """

    def __init__(self, service_meta, **kwargs):
        self.service_meta = service_meta
        if service_meta.delivermode == "queue":
            self._sender = get_client_sender_for_queue(service_meta, **kwargs)
        elif service_meta.delivermode == "topic":
            self._sender = get_client_sender_for_topic(service_meta, **kwargs)
        elif service_meta.delivermode == "shuffle":
            self._sender = get_client_sender_for_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "sharding":
            self._sender = get_client_sender_for_sharding(service_meta, **kwargs)
        elif service_meta.delivermode in {"topicshuffle", "topicsharding"}:
            raise RuntimeError("not support in redis")
        else:
            raise RuntimeError("invalid queue deliver mode")

    def send(self, content, key, blocking=True, timeout=5):
        self._sender.put(content, "", False, blocking, timeout)

    def get_sharding_key(self, key):
            return key

    def close(self):
        if self._sender:
            self._sender.close()
            self._sender = None


def get_server_receiver(service_meta, **kwargs):
    return RedisServerReceiver(service_meta, **kwargs)


class RedisServerReceiver(object):
    """
    Used to receive request from client.

    """

    def __init__(self, service_meta, **kwargs):
        self.service_meta = service_meta
        if service_meta.delivermode == "queue":
            self._receiver = get_server_receiver_for_queue(service_meta, **kwargs)
        elif service_meta.delivermode == "topic":
            self._receiver = get_server_receiver_for_topic(service_meta, **kwargs)
        elif service_meta.delivermode == "shuffle":
            self._receiver = get_server_receiver_for_shuffle(service_meta, **kwargs)
        elif service_meta.delivermode == "sharding":
            self._receiver = get_server_receiver_for_sharding(service_meta, **kwargs)
        elif service_meta.delivermode in {"sharding", "topicshuffle", "topicsharding"}:
            raise RuntimeError("not support in redis")
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
    return RedisClientReceiver(service_meta, clientid, **kwargs)


class RedisClientReceiver(object):
    """
    Used to receive response from server.

    """

    def __init__(self, service_meta, clientid, **kwargs):
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


def get_server_sender(cdc, clientid, **kwargs):
    return RedisServerSender(cdc, clientid, **kwargs)


class RedisServerSender(object):
    """
    Used to send response by server.

    """

    def __init__(self, cdc, clientid, **kwargs):
        self._sender = get_server_sender_for_rpc_server(cdc, clientid, **kwargs)
        self.clientid = clientid

    def send(self, content, key, blocking=True, timeout=5):
        self._sender.put(content, "", False, blocking, timeout)

    def get_sharding_key(self, key):
        # response doesn't need to recalculate the key
        return key

    def close(self):
        return self._sender.close()
