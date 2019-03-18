#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import
import datetime
import abc

from .elasticsearch_conn import ESConnection
from .elasticsearch_dao import ESDao

__author__ = "nebula"

class BaseESService():
    """
    抽象基类
    供BaseESServiceForAthena, BaseESServiceForOPS, BaseESServiceForBigSecurity继承
    """
    __metaclass__ = abc.ABCMeta

    _es_dao = None

    @abc.abstractmethod
    def __init__(self, service_type="threathunter", thread_mode="single",host="",port=""):
        conn = None
        if "threathunter" == service_type:
            if "single" == thread_mode:
                conn = ESConnection(created_for="threathunter",port=port,host=host).get_connection()
            else:
                raise Exception("thread mode is either single or multi, other's invalid")
        else:
            raise Exception("service type error, choose from threathunter")
        self._es_dao = ESDao(conn)

    def __get_days_list(self, begin_datetime, end_datetime):
        days = (end_datetime.date() - begin_datetime.date()).days
        n_days_list = []

        for i in range(0, days+1):
            n_days_list.append((begin_datetime + datetime.timedelta(days=i)).date())
        return n_days_list

    def get_existing_indices(self, index_prefix=None, index_suffix=None, starttime=None, endtime=None, connection_para=None, limit=None):
        """
        :limit:返回索引的个数，从endtime往前取N个索引
        """
        start_date = datetime.datetime.fromtimestamp(starttime)
        end_date = datetime.datetime.fromtimestamp(endtime)
        days_list = self.__get_days_list(start_date, end_date)
        indices = []
        for day in days_list:
            index = day.strftime('%Y-%m-%d')
            if index_prefix:
                index = "%s_%s" % (index_prefix, index)
            if index_suffix:
                index = "%s_%s" % (index, index_suffix)
            indices.append(index)
        exists_indices = self._es_dao.get_existing_indices(indices=indices, connection_para=connection_para)
        if limit:
            exists_indices = exists_indices[limit*-1:]
        return exists_indices


class BaseESServiceForThreathunter(BaseESService):
    """
    这个类作为基类，提供给Handler和后台使用的ES Service继承
    """
    def __init__(self, thread_mode="single",port="",host=""):
        super(BaseESServiceForThreathunter, self).__init__(service_type="threathunter", thread_mode=thread_mode,port=port,host=host)



