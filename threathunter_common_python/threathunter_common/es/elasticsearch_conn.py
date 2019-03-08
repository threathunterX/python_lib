#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import

import logging

from ..third_libs.pyes_0_99_5.connection_http import update_connection_pool
from ..third_libs.pyes_0_99_5 import ES

logger = logging.getLogger('threathunter_common.es.conn')

__author__ = 'tsli'

class ESConnection():
    """
    ES连接类
    通过将该类的实例化对象传给各个Service从而实现连接的复用
    同时，在多线程中，可以控制一个线程worker使用一个连接从而做到连接的线程安全

    连接配置都写在配置文件中，不需要用户去传入
    """
    def __init__(self, created_for="threathunter",conn_pool_size=1,buffer_size=400,host="",port=""):
        self.conn = None
        self.created_for = created_for
        self.host = host
        self.port = str(port)
        self.buffer_size = buffer_size
        self.conn_pool_size = conn_pool_size

    def __connect(self):
        """
        update_connection_pool(maxsize=1):
        Update the global connection pool manager parameters.

        maxsize: Number of connections to save that can be reused (default=1).
                 More than 1 is useful in multithreaded situations.
        """
        if "threathunter" == self.created_for:
            self.connection_pool_size = int(self.conn_pool_size)
            self.bulk_size = int(self.buffer_size)
            self.es_url = self.host + ":" + self.port
        else:
            raise Exception("no create_for")

        update_connection_pool(self.connection_pool_size)
        try:
            self.conn = ES(self.es_url, bulk_size=self.bulk_size) # Use HTTP
        except Exception, e:
            logger.debug("Failed to connect to elastic search server")
            return False
        return True

    def get_connection(self):
        """
        返回连接以复用
        """
        if self.__connect():
            return self.conn
        else:
            return None

