#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import
import abc

from ..third_libs.pyes_0_99_5.connection_http import update_connection_pool
from ..third_libs.pyes_0_99_5 import ES
from ..third_libs.pyes_0_99_5.exceptions import NoServerAvailable


class ESDao():
    """
    Dao层只做一些ES API的封装，跟业务逻辑无关
    """
    def __init__(self, conn=None):
        """
        connection对象是由外部传入的，这样使得connection的复用更灵活
        同时，适用多线程场景
        """
        self.conn = conn

    def add_records_in_bulk(self, index_name, type_name, record, id=None):
        """
        批量添加数据到ES的buffer中，默认批量大小为400个doc，这个大小在配置文件中设置
        如果record/doc数量超过了阈值（默认400），将会提交前400个doc到ES server
        所以buffer中doc的数量永远少于阈值，最后的一批数据需要通过调用self.flush_bulk()去强制提交到ES server

        :param index_name: 索引名称（数据库名称）
        :param type_name: 索引类型名称（表名）
        :param record: 数据（记录）
        :param id: 如果不传id，那么在插入数据时，ES会随机一个hash作为id；如果传入id，则将record的id设为该值
        :return: 返回批量插入后的插入结果
        """
        response = None
        try:
            if id:
                response = self.conn.index(record, index_name, type_name, id, bulk=True)
            else:
                response = self.conn.index(record, index_name, type_name, bulk=True)
        except NoServerAvailable, e:
            raise Exception("add record in bulk failed")
        except Exception, e:
            raise Exception("add record in bulk failed")

        return response

    def flush_bulk(self):
        """
        强制提交缓冲区中数据到ES server
        :return: a dict，包含了errors字段，如果errors字段为false则flush成功，如果errors字段为true，则flush不成功。
                不成功时的报错信息存在dict["items"]["create"]["error"]
        """
        return self.conn.flush_bulk(forced=True)


class ESConnection():
    """
    ES连接类
    通过将该类的实例化对象传给各个Service从而实现连接的复用
    同时，在多线程中，可以控制一个线程worker使用一个连接从而做到连接的线程安全

    连接配置都写在配置文件中，不需要用户去传入
    """
    def __init__(self, created_for="athena"):
        self.conn = None
        self.created_for = created_for

    def __connect(self):
        """
        update_connection_pool(maxsize=1):
        Update the global connection pool manager parameters.

        maxsize: Number of connections to save that can be reused (default=1).
                 More than 1 is useful in multithreaded situations.
        """
        if "athena" == self.created_for:
            self.connection_pool_size = 1
            self.bulk_size = 400
            self.es_url = "192.168.79.204:9200"

        update_connection_pool(self.connection_pool_size)
        try:
            self.conn = ES(self.es_url, bulk_size=self.bulk_size) # Use HTTP
        except Exception, e:
            print "Failed to connect to elastic search server", e
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

# 仅为单线程设计
es_athena_conn = ESConnection(created_for="athena").get_connection()


class BaseESService():
    """
    抽象基类
    供BaseESServiceForAthena, BaseESServiceForOPS, BaseESServiceForBigSecurity继承
    """
    __metaclass__ = abc.ABCMeta

    _es_dao = None

    @abc.abstractmethod
    def __init__(self, service_type="athena", thread_mode="single"):
        conn = None
        if "athena" == service_type:
            if "single" == thread_mode:
                conn = es_athena_conn
            elif "multi" == thread_mode:
                conn = ESConnection(created_for="athena").get_connection()
            else:
                raise Exception("thread mode is either single or multi, other's invalid")
        else:
            raise Exception("service type error, choose from athena/ops/threathunterurity")
        self._es_dao = ESDao(conn)


class BaseESServiceForAthena(BaseESService):
    """
    这个类作为基类，提供给Handler和后台使用的ES Service继承
    """
    def __init__(self, thread_mode="single"):
        super(BaseESServiceForAthena, self).__init__(service_type="athena", thread_mode=thread_mode)


class ESServiceForLogs(BaseESServiceForAthena):
    """
    继承自BaseESServiceForAthena，专为Log的录入和查询服务
    """
    def add_records_in_buffer(self, index, typee, record, id=None):
        if id:
            ret = self._es_dao.add_records_in_bulk(index, typee, record, id)
        else:
            ret = self._es_dao.add_records_in_bulk(index, typee, record)
        if not ret:
            return True
        elif ret["errors"]:
            return False
        return True

    def flush_bulk(self):
        ret = self._es_dao.flush_bulk()
        if not ret:
            # ret_dict也有可能为None，那就是当buffer中的数量正好达到了400的整数倍的时候，数据自动提交了，这时候去flush bulk就会返回None
            return True
        elif ret["errors"]:
            return False
        return True