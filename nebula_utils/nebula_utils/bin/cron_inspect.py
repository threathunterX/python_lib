#!/home/threathunter/nebula/nebula_web/venv/bin/python
# -*- coding: utf-8 -*-

import sys, logging
from datetime import timedelta, datetime
from time import sleep
from os import listdir
from os import path as opath

from nebula_utils.persist_compute.main import compute_statistic
from nebula_utils.persist_compute import cache
from nebula_utils.persist_utils.settings import STAT_DB_TMP_PATH, STAT_DB_PATH
from nebula_utils import settings
#from nebula_utils.persist_utils import utils

#DB_ROOT_PATH = '/home/threathunter/nebula/nebula_web/nebula/tests/persist_test/'
DB_ROOT_PATH = settings.DB_ROOT_PATH#'/home/threathunter/nebula/persistent/'
#STAT_DB_TMP_PATH, STAT_DB_PATH = 'data_tmp', 'data'

logger = logging.getLogger('nebula_utils.cron_inspect')

doc = """
Usage:
  cron_inspect.py [options] [args]
Options:
  --check_history [duration]: 检查时间范围内的数据统计的正确性,
     ex. --check_history 1d 检查今天(当前小时之前)的统计是否运行没错
         --check_history 1m 检查今天之前一个月的统计是否运行没错
         --check_history 1w 检查今天之前一个星期的统计是否运行没错
         --check_history 5h 检查之前5个小时的统计是否运行没错
         --check_history 检查上个小时的统计
  --rerun_history [duration]: 检查时间范围内的数据统计的正确性,并重新统计有问题的目录, 如果客户流量较大或者检查时间范围较大，可能会耗时较久
  --rerun [db_path db_path1 ...] : 默认检查上个小时的统计，如果有错就重新跑； 也可以指定数据文件地址
"""
def get_last_hour(f='%Y%m%d%H'):
    """
    获取上个小时的时间字符串, ex. 2015040915 
    """
    n = datetime.now()
    td = timedelta(seconds=3600)
    last_hour_n = n - td
    return last_hour_n.strftime(f)

Last_Hour_Path = [ opath.join(DB_ROOT_PATH, get_last_hour()), ]

def is_stat_running(db_path):
    # data_tmp 文件夹存在
    if opath.exists(opath.join(db_path, STAT_DB_TMP_PATH)):
        return True
    return False

def is_stat_success(db_path):
    # 统计成功条件: data 目录存在, data目录 MANIFEST打头的文件至少有一个
    data_path = opath.join(db_path, STAT_DB_PATH)
    if not opath.exists(data_path):
        return False
        
    if any( f.startswith("MANIFEST") for f in listdir(data_path)):
        return True
    return False

def get_db_paths_from_duration(duration, f='%Y%m%d%H'):
    if duration is None:
        return Last_Hour_Path
    days = None
    seconds = None
    start_time = None
    now = datetime.now()
    
    unit = duration[-1]
    avail_units = ('h', 'd', 'w', 'm')
    if unit not in avail_units:
        logger.error( '未知的时间单位: %s' % unit )
        sys.exit(1)
        
    if unit == 'h':
        seconds = int(duration[:-1]) * 3600
    elif unit == 'd':
        days = int(duration[:-1])
    elif unit == 'w':
        days = int(duration[:-1]) * 7
    elif unit == 'm':
        days = int(duration[:-1]) * 30
        
    if seconds:
        start_time = now - timedelta(seconds=seconds)
    elif days:
        start_time = now - timedelta(days=days)
        
    if not start_time:
        logger.error('计算不出开始时间, 请检查输入的时间间隔: %s', duration)
        sys.exit(2)
        
    db_paths = []
    hour_interval = timedelta(seconds=3600)
    while now >= start_time:
        now = now - hour_interval
        db_paths.append(opath.join(DB_ROOT_PATH, now.strftime(f)))
        
    return db_paths
        
def rerun_stat_history(duration):
    issue_db_paths = check_stat_history(duration)
    logger.debug( u'将会重新统计的目录: %s', issue_db_paths)
    rerun_stat(issue_db_paths)
    
def check_stat_history(duration):
    db_paths = get_db_paths_from_duration(duration)
    running = []
    incomplete = []
    for db_path in db_paths:
        if not opath.exists(db_path):
            # @todo, 打印没有产生的小时日志，虽然可能当时java并没有跑，没有办法追踪了。
            continue

        if is_stat_running(db_path):
            running.append(db_path)
            
        if not is_stat_success(db_path):
            incomplete.append(db_path)
            
        logger.info( u' %s 文件夹没有问题', db_path)
            
    if running:
        print('以下目录应该还在跑统计(存在data_tmp目录):')
        for _ in running:
            print(_)
            
    if incomplete:
        print('以下目录统计出错(data目录没有MANIFEST文件):')
        for _ in incomplete:
            print(_)
    
    running.extend(incomplete)
    
    return running
    
def rerun_stat(db_paths):
    if not db_paths:
        # 之前小时
        db_paths = Last_Hour_Path

    logger.info('将会检查状态并重新统计的db_paths: %s', db_paths)
    for db_path in db_paths:
        while is_stat_running(db_path):
            logger.info('统计目录%s 还存在data_tmp目录，说明还在跑统计脚本,即将睡眠 5min，再重新检查...', db_path)
            sleep(300)
        
        if not is_stat_success(db_path):
            logger.info('将会重新跑统计 path: %s' % db_path)
            compute_statistic(db_path)
            cache.Stat_Dict = dict()
            cache.Hook_Functions = []
        else:
            logger.info(u'path %s访问没有问题', db_path)
        

if __name__ == '__main__':
    if len(sys.argv) == 1 or sys.argv[1] in ('-h', '--help'):
        print(doc)
    elif sys.argv[1] == '--check_history':
        # 检查一定时间内的数据统计的正确性
        duration = sys.argv[2] if len(sys.argv) == 3 else None
        check_stat_history(duration)
    elif sys.argv[1] == '--rerun_history':
        # 检查一定时间内的数据统计的正确性, 并且重新统计有问题的目录
        duration = sys.argv[2] if len(sys.argv) == 3 else None
        rerun_stat_history(duration)
    elif sys.argv[1] == '--rerun':
        # 检查上个小时的统计
        db_paths = sys.argv[2:]
        logger.info('db_paths: %s', db_paths)
        rerun_stat(db_paths)