# -*- coding: utf-8 -*-

"""
测试详情页的散点图、每30s访问、日志列表的正确性
详情页的散点图、每30s访问、日志列表的一致性， 和上面柱状图的数量匹配
首先完整的日志列表的总数和每30s访问的总数应该一致
1. 确定功能函数正确性
2. 视图函数?
3. 前端参数?

"""
import logging
import unittest, os, json
from datetime import datetime

from settings import settings
from nebula.views import data_bus as dbus
from nebula.views import incident_stat as ins
from nebula.tests.utils import wsgi_safe, WebTestCase, test_env_prepare, get_current_hour_interval, get_last_hour_interval

from nebula_utils.persist_utils import query_visit_stream, query_clicks_period, get_request_log

#logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger('nebula.test.incident_stat')


@wsgi_safe
class DataBusTest(WebTestCase):
    def get_handlers(self):
        return [
            (dbus.OfflineSerialDataHandler.REST_URL,
             dbus.OfflineSerialDataHandler),
            (ins.ClickListPeriodHandler.REST_URL,
             ins.ClickListPeriodHandler),
            (ins.ClickListHandler.REST_URL,
             ins.ClickListHandler),
            (ins.VisitStreamHandler.REST_URL,
             ins.VisitStreamHandler)
        ]
    def get_app_kwargs(self):
        return settings.Tornado_Setting
        
    def test_detail_page(self):
        # 1. 完整的散点图、每三十秒访问量、点击列表的总数 应与 当前小时的总量一致
        # 2. 散点图、每三十秒访问量、点击列表的数量 应相符
        # 情况1. API 出来有差别, 但是直接调接口出来的也是一模一样的差别的话,那就去看接口的查询,a, 找到多少条, b.索引难道拿到的不一样?
        url = '%s?var_list=%s&from_time=%s&end_time=%s&key_type=%s&key=%s' % (dbus.OfflineSerialDataHandler.REST_URL, var_list, int(from_time), int(from_time)+1, key_type, key)
        res = self.fetch(url)
        jvalue = json.loads(res.body)
#        print jvalue["values"]
        print "柱状图总量:", jvalue["values"][from_time][var_list]
        
        url = '%s?from_time=%s&end_time=%s&key_type=%s&key=%s' % (ins.ClickListPeriodHandler.REST_URL, int(from_time), int(end_time), key_type, key)
        res = self.fetch(url)
        jvalue = json.loads(res.body)
        print "每三十秒API返回总量:", reduce(lambda x,y: x+y, [int(v["count"]) for k,v in jvalue["values"].iteritems()])
        
        url = '%s?from_time=%s&end_time=%s&key_type=%s&key=%s' % (ins.VisitStreamHandler.REST_URL, int(from_time), int(end_time), key_type, key)
        res = self.fetch(url)
        jvalue = json.loads(res.body)
        print "散点图API返回总量:", len(jvalue["values"])

        url = '%s?from_time=%s&end_time=%s&key_type=%s&key=%s&size=%s' % (ins.ClickListHandler.REST_URL, int(from_time), int(end_time), key_type, key, 10000)
        res = self.fetch(url)
        jvalue = json.loads(res.body)
        print "点击列表API返回总量:", len(jvalue["values"])
        
    def test_unify(self):
        clicks_period_r = query_clicks_period(key, key_type, from_ts, end_ts)
        
        #print clicks_period_r
        
        cp_total = 0
        for k,v in clicks_period_r.iteritems():
            cp_total += v['count']
        
        print "\n每30s的总量:",cp_total
        
        visit_stream_r = query_visit_stream(key, key_type, from_ts, end_ts)
        
        #print visit_stream_r
        
        print "散点图的总量:",len(visit_stream_r)
        
        clicks_r, err = get_request_log(key, from_ts, key_type, end=end_ts, limit=10000)
        
        print "日志总量:", len(clicks_r)

def env_prepare():
    # metrics 初始化配置
    metrics_dict = {
    "app": "nebula_web",
    "redis": {
        "type": "redis",
        "host": settings.Redis_Host,
        "port": settings.Redis_Port
    },
    "influxdb": {
        "type": "influxdb",
        "url": settings.Influxdb_Url,
        "username": "test",
        "password": "test"
    },
    "server": settings.Metrics_Server
    }
    from threathunter_common.redis.redisctx import RedisCtx
    RedisCtx.get_instance().host = settings.Redis_Host
    RedisCtx.get_instance().port = settings.Redis_Port
    
    from threathunter_common.metrics.metricsagent import MetricsAgent
    MetricsAgent.get_instance().initialize_by_dict(metrics_dict)
    
    # load取cache们
    from nebula.dao.cache import Cache_Init_Functions, init_caches

    # 策略权重的缓存
    from nebula.dao.strategy_dao import init_strategy_weigh
    Cache_Init_Functions.append(init_strategy_weigh)
    init_caches()
    
if __name__ == '__main__':
    env_prepare()

    global key, key_type, from_time, end_time, from_ts, end_ts, var_list
    key = "121.42.183.159"
    key_type = 'ip'
    from_time = "1487113200000"
    end_time = "1487116799999"
    var_list = "ip__visit__dynamic_count__1h__slot"
    
    from_ts = int(from_time) / 1000.0
    end_ts = int(end_time) / 1000.0

    print(u"测试的条件, key: %s, key_type: %s, from_ts: %s , type:%s, %s,end_ts: %s , type:%s, %s, var_list: %s, type:%s" % (key, key_type, from_ts, type(from_ts), datetime.fromtimestamp(from_ts), end_ts, type(end_ts), datetime.fromtimestamp(end_ts), var_list, type(var_list)))
    unittest.main()