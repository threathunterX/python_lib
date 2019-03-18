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

from .semaphore import CountDownLatch

from threathunter_common.util import run_in_thread
from threathunter_common.redis.redisctx import RedisCtx

__author__ = "nebula"

def catch_error(func):
    """Catch errors of redis and do a second try"""
    def wrap(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logging.error('redis operation error, try once more, error: %s', e)
            return func(self, *args, **kwargs)
    return wrap


class RedisList(object):

    def __init__(self, name, **kwargs):
        self.name = name
        self.lock = threading.RLock()

    @catch_error
    def qsize(self):
        return RedisCtx.get_instance().redis.llen(self.name)

    def empty(self):
        if self.qsize() == 0:
            return True
        else:
            return False

    def full(self):
        if self.name and self.max_queue_size and self.qsize() >= self.max_queue_size:
            return True
        else:
            return False

    def close(self):
        pass


class RedisListSender(RedisList):

    def __init__(self, name, **kwargs):
        RedisList.__init__(self, name, **kwargs)
        self.max_queue_size = kwargs.get("max_queue_size")
        self.lazy_limit = kwargs.get("lazy_limit")

        if self.name and self.lazy_limit and self.max_queue_size:
            self.qsize_diff_limit = int(self.max_queue_size * 0.1)
        else:
            self.qsize_diff_limit = 0
        self.qsize_diff = 0

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
        if self.lazy_limit and self.qsize_diff < self.qsize_diff_limit:
            pass
        elif self.full():
            raise Queue.Full
        else:
            self.qsize_diff = 0

        with self.lock:
            self.qsize_diff += 1
            RedisCtx.get_instance().redis.rpush(self.name, str(obj))


class RedisListReceiver(RedisList):
    def __init__(self, name, **kwargs):
        RedisList.__init__(self, name, **kwargs)
        self.max_cache_size = kwargs.get("max_cache_size", 1000)
        self.cache = Queue.Queue(maxsize=self.max_cache_size)
        self.full_errors = 0
        self.running = False

    def start_consuming(self):
        self.running = True
        self.consume_task = run_in_thread(self.consume_task)

    def stop_consuming(self):
        self.running = False
        self.consume_task.join()

    def consume_task(self):
        while self.running:
            try:
                msg = RedisCtx.get_instance().redis.blpop([self.name], timeout=1)

                if not msg:
                    continue

                self.cache.put(msg[1], timeout=10)
            except Queue.Full:
                self.full_errors += 1
            except Exception as err:
                print err
                time.sleep(1)

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


class RedisPubSub(object):

    def __init__(self, name, **kwargs):
        self.name = name
        self.lock = threading.RLock()

    def qsize(self):
        raise RuntimeError("not support in pubsub mode")

    def empty(self):
        raise RuntimeError("not support in pubsub mode")

    def full(self):
        raise RuntimeError("not support in pubsub mode")

    def close(self):
        pass


class RedisPubSubSender(RedisPubSub):

    def __init__(self, name, **kwargs):
        RedisPubSub.__init__(self, name, **kwargs)

    def put(self, obj, routing_key, durable=False, block=True, timeout=None):
        return self.put_nowait(obj, routing_key)

    @catch_error
    def put_nowait(self, obj, routing_key, durable=False):
        RedisCtx.get_instance().redis.publish(self.name, str(obj))


class RedisPubSubReceiver(RedisPubSub):
    def __init__(self, name, **kwargs):
        RedisPubSub.__init__(self, name, **kwargs)
        self.max_cache_size = kwargs.get("max_cache_size", 1000)
        self.cache = Queue.Queue(maxsize=self.max_cache_size)
        self.full_errors = 0
        self.running = False
        self.pubsub = None

    def start_consuming(self):
        self.running = True
        latch = CountDownLatch()
        self.consume_task = run_in_thread(self.consume_task, latch)
        latch.wait()

    def stop_consuming(self):
        self.running = False
        if self.pubsub:
            self.pubsub.unsubscribe(self.name)
            self.pubsub = None
        self.consume_task.join()

    def consume_task(self, latch):
        while self.running:
            try:
                r = RedisCtx.get_instance().redis
                self.pubsub = r.pubsub()
                self.pubsub.subscribe(self.name)
                for item in self.pubsub.listen():
                    if item["type"] == "subscribe":
                        latch.countdown()
                    elif item["type"] == "message":
                        try:
                            self.cache.put(item["data"], timeout=10)
                        except Queue.Full:
                            self.full_errors += 1
            except Exception as err:
                print err

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


class RedisMultipleListSender(object):
    """
    Used to send to multiple redis lists, messages are sent to the list according to the routing key
    """

    def __init__(self, names, **kwargs):
        self.names = names
        self.count = len(names)

    def put(self, obj, routing_key, durable=False, block=True, timeout=None):
        return self.put_nowait(obj, routing_key)

    @catch_error
    def put_nowait(self, obj, routing_key, durable=False):
        idx = hash(routing_key) % self.count
        name = self.names[idx]
        RedisCtx.get_instance().redis.rpush(name, str(obj))

    def close(self):
        pass
