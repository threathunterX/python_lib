# -*- coding: utf-8 -*-
import time
import logging, socket, traceback
from os import path as opath
from struct import pack
import cPickle as pickle

import aerospike
from aerospike.exception import RecordNotFound, AerospikeError
from leveldb import LevelDBError
from threathunter_common.util import ip_match
from . import utils
from nebula_utils import settings
from nebula_utils.persist_utils import utils as putils
from nebula_utils.persist_utils.db import scan_keys, get_db
from nebula_utils.persist_utils import settings as psettings
from nebula_utils.persist_utils.metrics import catch_latency

location = 'nebula_utils.compute.cache'
logger = logging.getLogger(location)
DEBUG_PREFIX = '==============='
# 每个维度一个统计dict
Stat_Dict = dict() # dimension : dict( 各维度的统计)
Hook_Functions = []

def get_stat_db_path(timestamp):
    """
    从时间戳 获取 报表数据存放位置
    """
    db_path = putils.get_path(timestamp, settings.DB_ROOT_PATH)
    return opath.join(db_path, psettings.STAT_DB_PATH)

def get_total_stat_key(key_type):
    """
    获取当前小时，key_type下面对所有key做top的 leveldb的key (key_type[1byte]'all')
    
    """
    return get_stat_key_prefix(key_type) + 'all'

def get_click_stat_key(key, dimension):
    """
    替代原来的generate_stat_key函数
    
    根据key生成 对应的统计的leveldb里面存储的键
    统计的key的格式 type(1 byte)|key(len(key) bytes)
    dimension: 统计的key的维度: enum: ip, ipc, did, uid, page
    key: 生成统计的key, ex. ip, ipc, device id, 特殊值:"all"
    """
    stat_type = utils.Dimension_Stat_Prefix.get(dimension, None)
    if stat_type is None:
        logging.error('invalid dimension type:%s , can not generate stat key', dimension)
        return None

    if stat_type == utils.IPC_Stat_Type:
        if key == 'all':
            res = pack('>B', stat_type) + key
        else:
            ip_segs = key.split('.')
            res = pack('>BBBB', stat_type, int(ip_segs[0]), int(ip_segs[1]), int(ip_segs[2]))
    elif stat_type == utils.IP_Stat_Type:
        if key == 'all':
            key_hex = key
        else:
            if key.find(':') != -1:
                # in case: port in c_ip column
                key = key.split(':')[0]
            try:
                key_hex = socket.inet_aton(key)
            except Exception:
                logger.error(u'key %s不能直接解析', key)
                return None

        t_hex = pack('>B', stat_type)
        res = t_hex + key_hex
    elif stat_type in (utils.UID_Stat_Type, utils.DID_Stat_Type, utils.PAGE_Stat_Type):
        t_hex = pack('>B', stat_type)
        res = t_hex + key
    
    return res


@catch_latency("统计点击数")
def gen_click_counter(Stat_Dict):
    """
    将各个维度统计的click数量存入aerospike
    /platform/stats/offline_serial 数据源
    连续小时各维度、已经总览的数据
    """
    ContinuousDB.get_db()
    
    work_ts = settings.Working_TS
    work_day = settings.Working_DAY
    related_vars = dict(
        did=['did__visit__dynamic_distinct_ip__1h__slot', #did 关联ip数
             'did__visit__dynamic_distinct_user__1h__slot',# did 关联user数
             'did__visit__dynamic_distinct_page__1h__slot',# did 关联page数 
             'did__visit__incident_count__1h__slot'],#did 风险事件数
        user=['user__visit__dynamic_distinct_ip__1h__slot',# user 关联ip数
             'user__visit__dynamic_distinct_did__1h__slot',# user 关联did数
             'user__visit__dynamic_distinct_page__1h__slot',# user 关联page数
             'user__visit__incident_count__1h__slot'],# user 风险事件数
        ip=['ip__visit__dynamic_distinct_did__1h__slot',# ip 关联did数
             'ip__visit__dynamic_distinct_user__1h__slot',# ip 关联user数
             'ip__visit__dynamic_distinct_page__1h__slot',# ip 关联page数
             'ip__visit__incident_count__1h__slot'],# ip 风险事件数
        page=['page__visit__dynamic_distinct_ip__1h__slot',# page 关联ip数
             'page__visit__dynamic_distinct_user__1h__slot',# page 关联user数
             'page__visit__dynamic_distinct_did__1h__slot',# page 关联did数
             'page__visit__incident_count__1h__slot'],)# page 风险事件数
    # 收集各维度相关联的variable数据,存入aerospike
    for dim, var_name in utils.Click_Variable_Names.iteritems():
        dim_stat_dict = Stat_Dict.get(dim, None)
        # 将每个维度关联的ip、page、did、user数量作为tag存入metrics,当前维度的tag则为维度的key
        # ex. {'ip': '172.16.0.1', 'user': 3, 'did': 7, 'page': 10, 'incident': 9}
        if dim_stat_dict is None:
            logger.info('维度:%s的统计字典为空', dim)
            continue
        logger.debug('维度%s的key是否都为空? %s' , dim, all(map(lambda x: x is None, dim_stat_dict.iterkeys())))

        dim_vars = related_vars[dim]
        for key, var_dict in dim_stat_dict.iteritems():
            if key == 'all':
                continue

            record = dict()
            record[var_name] = var_dict.get(var_name, 0)
            for var in dim_vars:
                a = var_dict.get(var, None)
                if isinstance(a, (list,set)):
                    record[var] = len(a)
                elif isinstance(a, (int,float)):
                    record[var] = a
                elif a is None:
                    record[var] = 0
            ContinuousDB.add(key, dim, work_day, work_ts, record)
    
    # 收集没有维度的count, distinctcount统计数据,存入aerospike
    dim_stat_dict = Stat_Dict.pop('total', {})
    var_dict = dim_stat_dict.get('all', {})
    total_vars = [
        'total__visit__dynamic_distinct_ip__1h__slot',  # ip数
        'total__visit__dynamic_distinct_did__1h__slot',  # did 数
        'total__visit__dynamic_distinct_user__1h__slot',  # user 数
        'total__visit__incident_distinct_user__1h__slot',  # 风险用户数
        'total__visit__incident_count__1h__slot',  # 风险事件数
        'total__visit__dynamic_count__1h__slot',  # 策略管理页面右上角 总点击数
        'total__visit__visitor_incident_count__1h__slot',  # 策略管理，访客风险数
        'total__visit__account_incident_count__1h__slot',  # 策略管理，账号风险数
        'total__visit__order_incident_count__1h__slot',   # 策略管理，订单风险数
        'total__visit__transaction_incident_count__1h__slot',  # 策略管理，支付风险数
        'total__visit__marketing_incident_count__1h__slot',  # 策略管理，营销风险数
        'total__visit__other_incident_count__1h__slot',  # 策略管理，其他风险数
        'total__transaction__sum__1h__slot',
        'total__transaction__count__1h__slot',
     ]
    total_dict = {}
    for var in total_vars:
        var_value = var_dict.get(var, 0)
        if isinstance(var_value, (int,float)):
            total_dict[var] = var_value
        elif isinstance(var_value, set):
            total_dict[var] = len(var_value)
            
    ContinuousDB.add('all', 'total', work_day, work_ts, total_dict)


def gen_related_counter(Stat_Dict):
    """
    continuous_top_related_statistic接口数据源,IP维度存入关联用户名称,其他维度存入关联IP

    页面: 风险分析 ip维度、user维度、did维度点击流页面
    ip维度， 关联的user的访问次数
    user维度, 关联ip的访问次数
    
    查询ip维度的某个关联用户访问次数时，应该查询user维度的key， 否则从ip维度查该变量统计的数据类型就是dict了,
    暂时弃用
    """
#    work_ts = None
#    continuous_db = ContinuousDB.get_db()
##    timestamp = putils.get_last_hour_timestamp()
#
#    related_vars = dict( ip='ip__visit__user_dynamic_count__1h__slot',
#                         user='user__visit__ip_dynamic_count__1h__slot',
#                         did='did__visit__ip_dynamic_count__1h__slot')
#    for dim in utils.Click_Variable_Names.keys():
#        dim_stat_dict = Stat_Dict.get(dim, None)
#
#        if dim_stat_dict is None:
#            logger.info('维度:%s的统计字典为空', dim)
#            continue
#
#        var_name = related_vars.get(dim ,None)
#        if var_name is None:
#            # page 维度没有看跨小时的ip访问的需求
#            continue
#        # 反过来查询的key_type
#        if dim == 'ip':
#            query_key_type = 'user'
#        else:
#            query_key_type = 'ip'
#
#        for key, var_dict in dim_stat_dict.iteritems():
#            if key == 'all':
#                continue
#            
#            related_values = var_dict.get(var_name, {})
#            for related_key, value in related_values.iteritems():
#                # 有可能重复的key, 直接add?
#                continuous_db.add(related_key, query_key_type, work_ts, dict(var_name=value))

def get_stat_key_prefix(key_type):
    stat_type = utils.Dimension_Stat_Prefix.get(key_type, None)
    return pack('>B', stat_type)
    
def get_statistic(key, key_type, fromtime, endtime, var_names):
    """
    根据key和时间戳 来获取对应小时统计的报表数据
    Paramters:
     key: 
     key_type: ip, ipc, page, user, did
     timestamp:
     t: 生成统计key 对应的type段, 现在默认为None是因为暂时查询key只有ip,ipc 类型的, @todo 视图函数里面扔进来
    Return:
     if key is None:
      { key(统计leveldb的索引中除了开头type的部分): {var_name1: , var_name2:} }
     else:
      {var_name1:, var_name2:}
    """
    var_names_set = set(var_names)
    logger.debug(DEBUG_PREFIX+ 'in get_statistic...')
    try:
        db_path = get_stat_db_path(fromtime)
#        logger.debug(DEBUG_PREFIX+"传入的fromtime:%s, 获取的对应统计的数据库地址是: %s", fromtime, db_path)
    except Exception as e:
        return None
    
    if key:
        logger.debug(DEBUG_PREFIX+" 有指定特定的key")
        logger.debug(DEBUG_PREFIX+"传入获取统计数据库的key的参数key:%s, key_type:%s", str(key), str(key_type))
        key = get_click_stat_key(key, key_type)
        if key is None:
            return None
        logger.debug(DEBUG_PREFIX+"传入获取统计数据库的key是 %s", (key,))
        try:
            db = get_db(db_path)
            return get_key_stat(key, db, var_names_set)
        except KeyError:
            logger.error("db:%s don't have key: %s", db_path, key)
            return None
        except LevelDBError:
            logger.error("db:%s 统计结果不正确", db_path)
            return None
        finally:
            if locals().has_key('db'):
                del db
    else:
        logger.debug(DEBUG_PREFIX+"会遍历所有的key")
        # url: {var_name1: , var_name2:}
        ret = dict()
        # 当传入的key为空时， 来遍历所有的page维度的key, 从里面load所有的var_names
        # 目前只有page维度会传入空的key
        prefix = get_stat_key_prefix(key_type)
        try:
            db = get_db(db_path)
            keys = scan_keys(prefix, db, include_value=False)
#            logger.debug(DEBUG_PREFIX+"将会遍历的统计key_type:%s, prefix:%s 扫到的keys: %s", key_type, (prefix,), keys)
            for key in keys:
                key_stat = get_key_stat(key, db, var_names_set)
#                logger.debug(DEBUG_PREFIX+"key: %s, key in ret? %s ,查询到的数据是:%s",(key,), key in ret.keys(), key_stat)
                ret[key[1:]] = key_stat
        except LevelDBError:
            logger.error("db:%s 统计结果不正确", db_path)
            return None
        except Exception as e:
            logger.error(e)
            return None
        finally:
            if locals().has_key('db'):
                del db
        return ret
    return None
    
def get_all_statistic(key_type, fromtime, endtime, var_names):
    """
    return: {var_name: , var_name:}
    """

#    logger.debug(DEBUG_PREFIX+ 'in get_all_statistic... , key_type is %s', key_type)
    total_key = get_total_stat_key(key_type)
#    logger.debug(DEBUG_PREFIX+ '拿到对应维度特殊的总统计的统计key为: %s', (total_key, ))
    var_names_set = set(var_names)
    
    try:
        db_path = get_stat_db_path(fromtime)
    except Exception as e:
        logger.error("Error when get %s type's total statistic", key_type)
        traceback.print_exc()
        return None

    if not opath.exists(db_path):
        return None
    try:
        db = get_db(db_path)
        return get_key_stat(total_key, db, var_names_set)
    except KeyError:
        logger.error("db:%s don't have key: %s", db_path, (total_key,))
        return None
    except LevelDBError:
        logger.error("db:%s 统计结果不正确", db_path)
        return None
    finally:
        if locals().has_key('db'):
            del db

def get_key_stat(key, db, var_names_set):
    """
    获取db数据库中对应key下面var_names_set的下统计数据
    """
#    logger.debug(DEBUG_PREFIX+"in 获取key: %s对应的统计数据", (key,))
    value = db.Get(key)
#    logger.debug(DEBUG_PREFIX+"获取key对应的统计原始数据: %s", (value,))
    jvalue = pickle.loads(value)
#    logger.debug(DEBUG_PREFIX+"获取key对应的统计数据是: %s, 过滤的变量名是 %s", jvalue, var_names_set)
    return dict( (k,v) for k,v in jvalue.iteritems() if var_names_set and k in var_names_set)


@catch_latency("统计风险事件")
def gen_risk_incidents(stat_dict):
    # 风险事件使用IP关联
    ip_dimension = stat_dict.get('ip', {})
    risk_incident = list()

    for ip, variables in ip_dimension.items():
        # ip incident事件数
        ip_incident_count = variables.get('ip__visit__incident_count__1h__slot', 0)

        if not ip_incident_count or ip == 'all':
            continue
        # 获取incident事件notice统计数据
        incident = dict()
        incident['ip'] = ip
        incident['associated_events'] = list()  # 风险事件关联事件id set
        incident['start_time'] = variables.pop('ip__visit__incident_min_timestamp__1h__slot', 0)
        incident['strategies'] = variables.pop('ip__visit__scene_incident_count_strategy__1h__slot', {})
        incident['hit_tags'] = variables.pop('ip__visit__tag_incident_count__1h__slot', {})
        incident['risk_score'] = compute_risk_score(incident['strategies'], ip_incident_count)
        uri_stems = sorted(variables.pop('ip__visit__page_incident_count__1h__slot', {}).items(),
                           lambda x, y: cmp(x[1], y[1]), reverse=True)[:10]
        incident['uri_stems'] = {uri: value for uri, value in uri_stems}
        incident['hosts'] = dict()
        for uri, count in incident['uri_stems'].items():
            if uri:
                host, _ = putils.parse_host_url_path(uri)
            else:
                host = uri
            if incident['hosts'].get(host, None):
                incident['hosts'][host] += count
            else:
                incident['hosts'][host] = count
        incident['most_visited'] = sorted(incident['uri_stems'].items(),
                                          lambda x, y: cmp(x[1], y[1]), reverse=True)[0][0] if incident['uri_stems'] else ''
        incident['peak'] = variables.pop('ip__visit__incident_max_rate__1h__slot', {}).get('max_count', 0)
        incident['dids'] = variables.pop('ip__visit__did_incident_count__1h__slot', {})
        incident['associated_users'] = variables.pop('ip__visit__user_incident_count__1h__slot', {})
        incident['users_count'] = len(variables.pop('ip__visit__incident_distinct_user__1h__slot', []))
        incident['associated_orders'] = dict()
        incident['status'] = 0

        risk_incident.append(incident)

    from tornado.httpclient import HTTPClient, HTTPError
    from tornado.escape import json_encode, json_decode

    auth_code = putils.get_auth_code()
    incident_url = 'http://{}:{}/platform/risk_incidents?auth={}'.format(
        settings.WebUI_Address, settings.WebUI_Port, auth_code)
    client = HTTPClient()
    try:
        _ = client.fetch(incident_url, method='POST', body=json_encode(risk_incident))
        res = json_decode(_.body)
        if res.get('msg', '') != 'ok':
            logger.error('新增风险事件失败,返回: {}'.format(res))
    except HTTPError:
        logger.error(u'很有可能插入风险事件超时')

def compute_risk_score(strategies, incident_count):
    """
    根据策略权重计算风险值
    每个场景下的所有策略计算平均值
    风险值为所有场景中的最大平均值
    """
    risk_scores = list()

    for category, category_strategies in strategies.items():
        category_score = 0

        # 根据权重计算场景总分
        for strategy, count in category_strategies.items():
            strategy_score = putils.Strategies_Weigh.get(strategy, {}).get('score', 0)
            category_score += strategy_score * count

        risk_scores.append(int(category_score/incident_count))

    return max(risk_scores) if risk_scores else 0


class ContinuousDB(object):
    db = None
    db_name = settings.AeroSpike_DB_Name #'offline'
    ttl = settings.AeroSpike_DB_Expire * 3600 # 数据过期时间 单位: 秒
        
    @classmethod
    def get_db(cls):
        if cls.db is None:
            cls.db = aerospike.client(settings.ContinuousDB_Config).connect()
        return cls.db
        
    @classmethod
    def add(cls, key, key_type, day, timestamp, vals):
        if key is None:
            return
        # key加上当天日期时间戳，防止不过期
        key = '%s_%s' % (key, day)
        db_key = (cls.db_name, key_type, key)
#        logger.info('insert key: %s, key_type:%s', key, key_type)
        try:
            cls.db.put(db_key, {str(timestamp):vals}, meta={'ttl':cls.ttl})
        except AerospikeError as e:
            logger.error('Aerospike Error: %s %s', e.msg, e.code)
        
    @classmethod
    def query(cls, key, key_type, timestamp, var_list):
        """
        Return:
        {var1:, var2:}
        """

        db_key = (cls.db_name, key_type, key)
        try:
            (key, meta, bins) = cls.db.select(db_key, timestamp)
            if bins:
                d = bins[timestamp]
                result = dict( (var, d.get(var, 0)) for var in var_list )
        except RecordNotFound:
            return None
        except AerospikeError as e:
            logger.error('Aerospike Error: %s %s', e.msg, e.code)
            return None
        return result
    
    @classmethod
    def query_many(cls, key, key_type, timestamps, var_list):
        """
        查询多个时间点的统计信息， 输出保留传入的时间戳的顺序
        Return:
        [ (timestamp, statistic_dict), ...]
        """
        # 兼容以前离线统计key未加上timestamp
        db_keys = [(cls.db_name, key_type, key), ]
        # key加上开始时间到结束时间的日期时间戳
        day = 86400
        from_day = int(float(timestamps[0]) - (float(timestamps[0]) % day) + time.timezone)
        end_day = int(float(timestamps[-1]) - (float(timestamps[-1]) % day) + time.timezone)
        for d in range(from_day, end_day + day, day):
            db_keys.append((cls.db_name, key_type, '%s_%s' % (key, d)))
        try:
            logger.debug(DEBUG_PREFIX+u'query key:%s, type is %s, timestamps:%s', db_keys, type(db_keys), timestamps)
            result = cls.db.select_many(db_keys, timestamps)
            logger.debug(DEBUG_PREFIX+u"查询返回的数据是:%s", result)
            # result: key, meta, bins
            bins = (_[2] for _ in result if _[2] is not None)
            records = dict()
            for bb in bins:
                for key, b in bb.iteritems():
                    r = records[key] = dict()
                    for var in var_list:
                        r[var] = b.get(var, 0)
        except RecordNotFound:
            return None
        except AerospikeError as e:
            logger.error('query_many Aerospike Error: %s %s', e.msg, e.code)
            return None
        return records
