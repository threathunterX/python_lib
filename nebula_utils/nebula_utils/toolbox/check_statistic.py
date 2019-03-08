# -*- coding: utf-8 -*-
import logging, sys
import cPickle as pickle

from nebula_utils.persist_compute import cache
from nebula_utils.persist_utils.db import db_context, scan_keys, get_db

DEBUG_PREFIX = '==============='

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('toolbox.check_statistic')

def check_keys(key, key_type, db_path=None, timestamp=None, var_names=None):
    # 找到数据库
    if db_path is None:
        if timestamp is None:
            logger.error('没有指定统计数据库路径或者查询的时间戳, 无法找到统计数据库路径, 错误退出')
            sys.exit(-2)
        db_path = cache.get_stat_db_path(timestamp)

    # 生成统计存储的key
    if key == '':
        prefix = cache.get_stat_key_prefix(key_type)
        try:
            db = get_db(db_path)
            stat_keys = scan_keys(prefix, db, include_value=False)
        finally:
            del db
    elif key == 'all':
        stat_keys = [cache.get_total_stat_key(key_type), ]
    else:
        stat_keys = [cache.get_click_stat_key(key, key_type), ]
    logger.debug(DEBUG_PREFIX+"传入获取统计数据库的key是 %s", stat_keys)
    
    # 获取持久化的统计数据
    stat = None
    for k in stat_keys:
        try:
            with db_context(db_path) as db:
                stat = cache.get_key_stat(k, db, set(var_names or []))
        except KeyError:
            logger.info("db:%s 没有key为 %s 的统计信息", db_path, (k,))
            return
            
        logger.info("db:%s,key为 %s 的统计为:%s", db_path, (k,) , stat)
    
def check_db(db_path=None, timestamp=None, print_detail=False, print_keys=False):
    """
    检查数据库是否有数据, 别强求性能了..
    """

    # 找到数据库
    if db_path is None:
        if timestamp is None:
            logger.error('没有指定统计数据库路径或者查询的时间戳, 无法找到统计数据库路径, 错误退出')
            sys.exit(-2)
        db_path = cache.get_stat_db_path(timestamp)
        
    db = None
    try:
        db = get_db(db_path)
        stats = [(k,pickle.loads(v)) for k,v in db.RangeIter(include_value=True)]
        logger.info('数据库:%s, 是否有数据? %s', db_path, len(stats)>0)
    finally:
        del db
        
    if print_detail:
        for k, v in stats:
            logger.info('key:%s, 统计的数据是:%s', (k,), v)
    if print_keys:
        logger.info( '数据库:%s, 的所有key是:%s', db_path, list(k for k, v in stats))
            

if __name__ == '__main__':
    # 检查ip的总的统计
    check_keys('all', 'ip',timestamp=1467259200.0)
    # 检查一个类型的key的存储上的变量们
    ip = '109.60.108.114'
#    check_keys(ip, 'ip',timestamp=1466755200.0)
#    check_keys(ip, 'ip',timestamp=1466758800.0)
#    check_keys('', 'ip',timestamp=1466755200.0)
#    check_db(timestamp=1466755200.0, print_keys=True)
#    check_db(timestamp=1466758800.0, print_keys=True)
