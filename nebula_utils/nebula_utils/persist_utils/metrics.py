#!/usr/bin/env python
# -*- coding:utf-8 -*-
from threathunter_common.util import millis_now
from threathunter_common.metrics.metricsagent import get_latency_str_for_millisecond
from threathunter_common.metrics.redismetrics import RedisMetrics

from nebula_utils import settings


# metrics参数
metrics_agent = RedisMetrics(
    host=settings.Redis_Host, port=settings.Redis_Port)
db = 'default'
expire_seconds = 86400 * 60
interval = 60 * 60 * 1000
latency_metrics = 'nebula.cronjob'


def catch_latency(remark):
    """
    记录函数调用耗费时间、计算结果，记录metrics
    """
    def decorator(func):
        def func_wrapper(*args, **kwargs):
            tags = dict(process=func.__name__, remark=remark)
            start = millis_now()
            timestamp = int(settings.Working_TS) * 1000
            try:
                ret = func(*args, **kwargs)
                tags['status'] = 'success'
                return ret
            except Exception as err:
                tags['status'] = 'fail'
                tags['err'] = err.message.encode('utf-8')
            finally:
                end = millis_now()
                tags['range'] = get_latency_str_for_millisecond(end-start)
                metrics_agent.add_metrics(
                    db, latency_metrics, tags, 1, expire_seconds, timestamp)
        return func_wrapper
    return decorator


@catch_latency("聚合小时metrics")
def merge_history_metrics(db, metrics_name, aggregation_type, filter_tags=dict(), group_tags=list()):
    """
    聚合前一个小时metrics,每一小时一个点
    """
    timestamp = int(settings.Working_TS) * 1000
    time_end = timestamp + interval

    # 聚合前一个小时数据
    metrics = metrics_agent.query(db, metrics_name, aggregation_type, timestamp,
                                  time_end, interval, filter_tags, group_tags)

    if metrics:
        notice_metrics = metrics.values()[0]

        # 获取前一个小时数据
        r = metrics_agent.redisctx.redis
        redis_key = '{}.{}'.format(db, metrics_name)
        entries = r.zrangebyscore(redis_key, timestamp, time_end - 1)

        for legend, value in notice_metrics.items():
            notice = list(legend)
            tags = {group_tags[i]: notice[i] for i in range(len(group_tags))}
            metrics_agent.add_metrics(
                db, metrics_name, tags, value, expire_seconds, timestamp=timestamp)

        # 聚合的数据重新写入后,删除原有数据
        p = r.pipeline()
        p.delete(*entries)
        p.execute()
