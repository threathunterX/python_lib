#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
from rediscluster import StrictRedisCluster

__author__ = 'lw'


class RedisCtx(object):
    _instance = None

    def __init__(self):
        self._host = None
        self._port = None
        self._redis = None
        self._password = None
        self._nodes = None  # for redis cluster, ex. [dict(host="127.0.0.1", port=6379),]

    @property
    def nodes(self):
        return self._nodes

    @nodes.setter
    def nodes(self, nodes):
        self._nodes = nodes
        self.update_redis()

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        self._host = host
        self.update_redis()

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        self._port = port
        self.update_redis()

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = password
        self.update_redis()

    @property
    def redis(self):
        if self._redis:
            return self._redis
        if not self._port or not self._host:
            if not self._nodes:
                raise RuntimeError(
                    "invalid redis config, host({}), port({}), nodes({})".format(self._host, self._port, self._nodes))
        if not self._nodes:
            r = redis.Redis(host=self._host, port=self._port)
        else:
            r = StrictRedisCluster(host=self._host, port=self._port,
                                   password=self._password, startup_nodes=self._nodes)
        self._redis = r
        return r

    def update_redis(self):
        """
        expire the old redis instance
        """
        self._redis = None  # expire the old one

    @staticmethod
    def get_instance():
        if not RedisCtx._instance:
            RedisCtx._instance = RedisCtx()
        return RedisCtx._instance

    def __str__(self):
        return "RedisCtx[host={}, port={}, nodes={}]".format(self._host, self._port, self._nodes)
