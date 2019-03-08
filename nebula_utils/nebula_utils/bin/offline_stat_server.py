#!/home/threathunter/nebula/nebula_web/venv/bin/python
# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import tornado
from tornado.ioloop import PeriodicCallback

from babel_python.servicemeta import ServiceMeta
from babel_python.serviceserver_async import ServiceServer
from threathunter_common.redis.redisctx import RedisCtx
from threathunter_common.metrics.metricsagent import MetricsAgent
from threathunter_common.util import millis_now
from threathunter_common.event import Event

from nebula_utils.persist_compute.cache import get_statistic, get_all_statistic
from nebula_utils import settings

logging.basicConfig(
    level=logging.DEBUG)

logger = logging.getLogger('nebula_utils.offline_stat.server')

DEBUG_PREFIX = '==============='

def get_server(redis_conf, rmq_conf):
    conf = rmq_conf
    if settings.Babel_Mode == "redis":
        conf = redis_conf

    meta = ServiceMeta.from_json(conf)
    server = ServiceServer(meta)
    return server

def get_offline_stat_server():
    return get_server(settings.OfflineStatService_redis,
                      settings.OfflineStatService_rmq)

def anwser(event):
    logger.debug(DEBUG_PREFIX+"事件 %s 接收的时间: %s", event, datetime.now())
    if not event:
        # 什么时候rpc的event为空?
        return 

    key = event.key
    if_all_key = event.property_values['if_all_key']
    key_type = event.property_values['key_type']
    fromtime = event.property_values['fromtime']
    endtime = event.property_values['endtime']
    var_names = event.property_values['var_names']
    
    if if_all_key:
        ret = get_all_statistic(key_type, fromtime, endtime, var_names)
    else:
        ret = get_statistic(key, key_type, fromtime, endtime, var_names)
        
    logger.debug(DEBUG_PREFIX+u"获取返回的数据是:%s", ret)
    temp_dict = dict()
    for var_name, stat in ret.iteritems():
        if isinstance(stat, set):
            temp_dict[var_name] = list(stat)
        else:
            temp_dict[var_name] = stat
    response = Event("__all__", "offline_stat_query_response", key, millis_now(), {'result':temp_dict})
    
    logger.debug(DEBUG_PREFIX+u"rpc server 返回的数据是:%s", response)
    return response

class OfflineStatRPCServer(object):
    def __init__(self):
        self.server = get_offline_stat_server()
        self.bg_task = None

    @staticmethod
    def running_task():
        # 留给一些定时任务的
        logger.debug(DEBUG_PREFIX+"定时任务运行的时间: %s", datetime.now())
        return

    def start(self):
        self.server.start(func=anwser)
        
        timeout = 30 * 60 * 1000
        pc = PeriodicCallback(self.running_task, timeout)
        pc.start()
    
def init_server_runtime():
    RedisCtx.get_instance().host = settings.Redis_Host
    RedisCtx.get_instance().port = settings.Redis_Port
    logger.debug(u"成功初始化redis: {}".format(RedisCtx.get_instance()))
    
    MetricsAgent.get_instance().initialize(settings.Metrics_Conf_FN)
    logger.debug(u"成功初始化metrics服务: {}.".format(MetricsAgent.get_instance().m))

def main():
    init_server_runtime()
    
    offline_server = OfflineStatRPCServer()
    offline_server.start()
    
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == '__main__':
    main()
    
