# -*- coding: utf-8 -*-
import os, shutil, sys, logging, time
from struct import calcsize, pack
from os import path as opath
from datetime import datetime
from tornado import ioloop

from threathunter_common.redis.redisctx import RedisCtx
from threathunter_common.util import millis_now
from threathunter_common.metrics.metricsagent import MetricsAgent

from nebula_utils import persist_utils
from nebula_utils import settings
from nebula_utils.persist_utils import utils
from nebula_utils.persist_utils import settings as usettings
from nebula_utils.persist_utils.log import Record, Index
from nebula_utils.persist_utils.db import get_db
from nebula_utils.persist_utils.metrics import merge_history_metrics, catch_latency

from .data_persist import write_statistic_data
from .utils import dict_merge, Group_Key_To_Dimension
from .process import ComputeDAG, ComputeVariableHandler
from .cache import Hook_Functions, gen_click_counter, gen_risk_incidents, gen_related_counter
from .profile import gen_visit_profile
from .notice_persist import gen_notice_statistics


DEBUG_PREFIX = '==============='
logger = logging.getLogger('nebula_utils.persist_compute.main')
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

    
def get_stat_db_path(db_path):
    """
    获取统计数据所放的临时和正式数据库地址, 
    
    新建db_path内的临时、正式数据库目录, 删掉之前存在的目录(为了重新统计、避免错误退出临时目录遗留等问题)
    """

    stat_db_tmp_path = opath.join(db_path, usettings.STAT_DB_TMP_PATH)
    stat_db_path = opath.join(db_path, usettings.STAT_DB_PATH)
    # 删除掉已经存在的统计数据目录, 不然shutil.move会将data_tmp放到已有的data下面, 而不是替换掉原来的data目录
    if opath.exists(stat_db_path):
        try:
            shutil.rmtree(stat_db_path)
        except:
            logger.error('不能删除已经存在的data目录: %s', stat_db_path)
            sys.exit(-2)

    # 删除掉已经存在的临时统计目录, 避免可能受到上次错误统计退出的影响
    if opath.exists(stat_db_tmp_path):
        try:
            shutil.rmtree(stat_db_tmp_path)
        except:
            logger.error('不能删除已经存在的data目录: %s', stat_db_path)
            sys.exit(-2)
            
    if not opath.exists(stat_db_tmp_path):
        try:
            os.mkdir(stat_db_tmp_path)
        except OSError:
            logger.error('%s不能被创建, 可能没有对应小时的日志目录.', stat_db_tmp_path)
            sys.exit(-3)

    return stat_db_tmp_path, stat_db_path

    
def add_extra_dimension_hook(Stat_Dict):
    var_prefixs = ["geo_count_top_by", "click_count_top_by"]
    # 统计生成中间结果
    # dimension: {var_name1: dict, var_name2:dict}
    temp_dict = dict()
    for dim,v in Stat_Dict.iteritems():
        if not temp_dict.has_key(dim):
            temp_dict[dim] = dict()

        for key, stat_dict in v.iteritems():
            if isinstance(stat_dict, list):
                # did, click_count_top_bypage_did 
                logger.debug(DEBUG_PREFIX+u'意外发生: 维度:%s, key:%s, stat_dict:%s', dim, key, stat_dict)
                continue
            for var_name, var_dict in stat_dict.iteritems():
                if any(var_name.startswith(_) for _ in var_prefixs):
                    temp_stat = temp_dict[dim]
                    if temp_stat.has_key(var_name) and temp_stat[var_name]:
                        temp_stat[var_name] = dict_merge(temp_stat[var_name], var_dict)
                    else:
                        temp_stat[var_name] = var_dict
                        
    for dim, stat in temp_dict.iteritems():
        for var_name, var_dict in stat.iteritems():
            if Stat_Dict[dim].has_key('all') and Stat_Dict[dim]['all'].has_key(var_name):
                logger.error('维度: %s, 要统计的"all"key下面的变量 %s已经存在了, 原来:%s, 回调函数生成:%s', dim, var_name, Stat_Dict[dim]['all'][var_name], var_dict)
            else:
                if not Stat_Dict[dim].has_key('all'):
                    Stat_Dict[dim]['all'] = dict()
                Stat_Dict[dim]['all'][var_name] = var_dict


@catch_latency("DAG 计算")
def compute_dag(compute_variables, logs_path):
    # 初始化计算有向无环图
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("=============== %s 开始统计计算 ===============", run_time)
    import newprocess
    dag = newprocess.DAG()
    dag.add_variables(compute_variables)

    # 然后开始不断获取log, 扔进 计算流里面
    last = millis_now()
    for lp in logs_path:
        logger.info("日志文件 %s 正在统计...", lp)
        for record, context in Record.record_generator(lp):
            dag.send_to_compute_flow(record)

#            # 补充索引
#            for col, dim in Group_Key_To_Dimension.iteritems():
#                val = record.get(col, None)
#                if not val:
#                    continue

        now = millis_now()
        logger.info("时间消耗：{}ms".format(now - last))
        last = now

def compute_statistic(specify_db_path=None):
    """
    
    """
    # 初始化redis实例
    RedisCtx.get_instance().host = settings.Redis_Host
    RedisCtx.get_instance().port = settings.Redis_Port
    MetricsAgent.get_instance().initialize_by_dict(metrics_dict)

    # 获取日志文件们所在的路径
    db_path, logs_path = Index.get_log_paths(specify_db_path)
    work_hour = db_path.rsplit('/', 1)[-1]
    t = datetime.strptime(work_hour, settings.LogPath_Format)
    settings.Working_TS = time.mktime((t.year, t.month, t.day, t.hour, t.minute, t.second, 0, 0, 0))
    settings.Working_DAY = int(time.mktime((t.year, t.month, t.day, 0, 0, 0, 0, 0, 0)))
    logger.debug(DEBUG_PREFIX+ 'working_hour:%s working_ts:%s, len:%s', work_hour, settings.Working_TS, len(str(settings.Working_TS)))

    # 从每个小时日志文件夹中获取 record schema && record header
    utils.load_record_settings(db_path)

    # 重新生成索引python_index目录
    Index.regen_index(db_path)
    
    # 获取所有策略权重
    utils.get_strategies_weigh()

    # 获取compute variables
    compute_variables = utils.fetch_compute_variables()
    logger.debug(DEBUG_PREFIX+'获得的计算变量们是:%s', [ _['name'] for _ in compute_variables if _['name']])

    # 新增变量时的调试本地的变量文件, 文件里面就可以只有单独的变量树来避免等很久
    # import json
    # with open('/home/threathunter/nebula/nebula_web/venv/lib/python2.7/site-packages/nebula_utils/unittests/offline.json', 'r') as f:
    # compute_variables = json.load(f)
    # cvs = [ ComputeVariableHandler.get_compute_variable(**_) for _ in compute_variables]
    # dag.add_nodes(cvs)

    # 遍历日志离线统计
    compute_dag(compute_variables, logs_path)

    # 注册生成风险事件的回调函数
    Hook_Functions.append(gen_risk_incidents)

    # 统计点击量的counter
    Hook_Functions.append(gen_click_counter)
#    Hook_Functions.append(gen_related_counter)

    # 注册统计user profile的回调函数
    Hook_Functions.append(gen_visit_profile)

    # 统计notices过去一个小时的数据
    ioloop.IOLoop.current().run_sync(gen_notice_statistics)

    # 聚合notices过去一个小时的metrics
    last = millis_now()
    logger.info("开始merge history metrics")
    merge_history_metrics('default', 'web.notices', 'sum',
                          group_tags=['test', 'strategy', 'location', 'url'])
    now = millis_now()
    logger.info("时间消耗：{}ms".format(now - last))
    last = now

    # 清理统计数据目录
    logger.info("开始清理统计数据目录")
    stat_db_tmp_path, stat_db_path = get_stat_db_path(db_path)
    now = millis_now()
    logger.info("时间消耗：{}ms".format(now - last))
    last = now

    # 持久化统计数据
    logger.info("开始持久化统计数据")
    write_statistic_data(stat_db_tmp_path)
    now = millis_now()
    logger.info("时间消耗：{}ms".format(now - last))
    last = now

    # 统计完成, 临时统计目录改名为正式, 提供查询服务
    logger.info("开始移动目录")
    shutil.move(stat_db_tmp_path, stat_db_path)
    now = millis_now()
    logger.info("时间消耗：{}ms".format(now - last))
    last = now

    # 定时脚本统计完成后，调用web API清除缓存，刷新数据
    utils.clean_cache()
    now = millis_now()
    logger.info("时间消耗：{}ms".format(now - last))
