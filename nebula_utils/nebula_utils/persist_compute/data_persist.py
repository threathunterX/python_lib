# -*- coding: utf-8 -*-

"""
将数据持久化
"""
import sys
import cPickle as pickle
import traceback
import logging

from leveldb import LevelDBError

from nebula_utils.persist_utils.db import db_context, get_db
from .cache import Stat_Dict, Hook_Functions, get_click_stat_key
from nebula_utils.persist_utils.metrics import catch_latency

from threathunter_common.redis.redisctx import RedisCtx

logger = logging.getLogger('nebula_utils.persist_compute.data_persist')
DEBUG_PREFIX = '==============='


@catch_latency("持久化数据")
def write_statistic_data(db_path):
    if Hook_Functions:
        # 执行钩子函数, 比如双变量计算的, 榜单计算的 @todo 应该提出去，不然调试钩子函数的计算结果都不方便
        for func in Hook_Functions:
            func(Stat_Dict)

#    logger.debug(DEBUG_PREFIX+'执行钩子函数后生成的统计数据是:%s', Stat_Dict)
    try:
        db = get_db(db_path)
    except LevelDBError:
        logger.error(u'不能从 %s 获取leveldb 连接', db_path)
        sys.exit(2)
    try:
        for dim,v in Stat_Dict.iteritems():
            if dim is None:
                # 大部分情况是uid, did,获取不到的情况
                dim = ''
            for k,stat in v.iteritems():
                if k is None:
                    continue
                if k == 'all':
#                    logger.debug(DEBUG_PREFIX+"维度:%s生成的all的总的统计里面的变量们有:%s, 数据是:%s", dim, stat.keys(), stat)
                    logger.debug(DEBUG_PREFIX+u"维度:%s生成的all的总的统计里面的变量们有:%s", dim, stat.keys())
                key = get_click_stat_key(k,dim)
                if key:
                    db.Put(key, pickle.dumps(stat))
#                db.Put(get_click_stat_key(k,dim), pickle.dumps(stat))
    finally:
        if locals().has_key('db'):
            del db

def write_click_counts(counter, date):
    """
    向redis记录访问次数记录
    counter: {key: count}
    date: 使用的持久化数据的文件夹的时间，精确到小时
    """
    r = RedisCtx.get_instance().redis
    with r.pipeline() as pipe:
        pipe.multi()
        for k, v in counter.iteritems():
            pipe.hset(k, date, v)
        pipe.execute()
