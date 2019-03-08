#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
from tornado import gen
import tornado_mysql

from threathunter_common.util import millis_now, utf8

from nebula_utils import settings
from nebula_utils.persist_utils import utils
from nebula_utils.persist_utils.metrics import catch_latency

logger = logging.getLogger('nebula_utils.persist_compute.notice')


config = {
    'host': settings.MySQL_Host,
    'port': settings.MySQL_Port,
    'user': settings.MySQL_User,
    'passwd': settings.MySQL_Passwd,
    'db': settings.Nebula_Data_DB,
    'charset': 'utf8'
}
QUERY_NOTICE_PARAMS = ['`key`', 'check_type', 'strategy_name',
                       'scene_name', 'decision', 'test', 'geo_city', 'uri_stem']
INSERT_NOTICE_PARAMS = ['`key`', 'check_type', 'strategy_name', 'scene_name', 'decision',
                        'test', 'geo_city', 'uri_stem', 'tag', 'count', 'timestamp', 'last_modified']
NOTICE_QUERY_STRING = 'SELECT %s, COUNT(*) FROM notice WHERE timestamp >= %s and timestamp < %s GROUP BY %s'
NOTICE_INSERT_STRING = 'INSERT INTO notice_stat (%s) VALUES(%s)' % (
    ','.join(INSERT_NOTICE_PARAMS), '%s,' * (len(INSERT_NOTICE_PARAMS) - 1) + '%s')
NOTICE_DELETE_STRING = 'DELETE FROM notice_stat WHERE timestamp = %s'


def parser_notice(*args):
    notice = {}

    for k, v in zip(QUERY_NOTICE_PARAMS, list(args)):
        notice[k] = utf8(v)
    notice['count'] = args[-1]
    return notice


@gen.coroutine
@catch_latency('统计风险名单数据库')
def gen_notice_statistics():
    """
    查询历史notice，并且统计命中tag，存入notice_stat数据库表，生成风险名单报表
    """
    logger.info('开始统计风险名单')
    start_time = int(settings.Working_TS) * 1000
    end_time = start_time + 60 * 60 * 1000
    strategies_weigh = utils.Strategies_Weigh

    try:
        # 初始化数据库连接
        conn = yield tornado_mysql.connect(**config)
        cursor = conn.cursor()
        query_params = ','.join(QUERY_NOTICE_PARAMS)
        insert_values = []
        yield cursor.execute(NOTICE_QUERY_STRING % (query_params, start_time, end_time, query_params))

        for _ in cursor:
            # 将查询结果解析为notice dict，并且根据命中策略名查询命中tag
            notice = parser_notice(*_)
            notice['timestamp'] = start_time
            notice['last_modified'] = millis_now()

            if notice['strategy_name'] in strategies_weigh:
                tags = strategies_weigh.get(
                    notice['strategy_name'], {}).get('tags', [])

                # 将每一个命中tag和统计后的notice组合存入数据库
                for tag in tags:
                    notice['tag'] = utf8(tag)
                    insert_values.append([notice[p]
                                          for p in INSERT_NOTICE_PARAMS])

        # 避免重复插入数据，需要先删除该时段数据，重新插入
        yield cursor.execute(NOTICE_DELETE_STRING % start_time)
        yield cursor.executemany(NOTICE_INSERT_STRING, insert_values)

        # 提交，不然无法保存新建数据
        conn.commit()
        # 关闭游标
        cursor.close()
        # 关闭连接
        conn.close()
        logger.info('风险名单统计完成')
    except Exception as e:
        logger.error(e)
        logger.error('风险名单统计失败')
