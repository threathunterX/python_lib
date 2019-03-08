# -*- coding: utf-8 -*-

import traceback, logging
from os import path as opath
from functools import partial
import csv
import codecs
from datetime import datetime, timedelta

from .db import db_context, get_db
from . import utils
from . import settings
#from .stats import get_statistic # api xinterface @todo
from .log import Record, Index, RecordNotMatch

from nebula_utils import settings as global_settings#Temp_Query_Path, LogPath_Format # @todo

logger = logging.getLogger('nebula_utils.main')
DEBUG_PREFIX = '==============='
FORCUS_PREFIX = '$$$$$$$$$$$$$$$'

def get_log_path(log_path, timestamp):
    """
    生成持久化存储日志path
      优先 specify_db_path
      次之 从时间戳获取 到小时的数据文件夹
    Output:
     - db_path ex. /path/to/persistent/2015040112/
     - log_path ex. /path/to/persistent/2015040112/log
    """
    if log_path:
        db_path = log_path
    else:
        db_path = utils.get_db_path(timestamp)
    log_path = opath.join(db_path, settings.LOG_PATH)
    return db_path, log_path

def get_interval_timestamps(fromtime, endtime, interval):
    """
    获取从fromtime到endtime, 每隔interval秒的时间戳, [fromtime, endtime) 包括fromtime， 不包括endtime
    fromtime: timestamp (float)
    endtime: timestamp (float)
    interval: seconds (int)
    """
    if fromtime >= endtime:
        return []
    ts = []
    while fromtime < endtime:
        ts.append(fromtime)
        fromtime = fromtime + interval

    if ts and ts[-1] + interval < endtime:
        ts.append(endtime)
    return ts

def get_half_min_strs_fromtimestamp(fromtime, endtime ):
    """
    返回从fromtime到endtime 每30s的时间戳
    """
    return get_interval_timestamps(fromtime, endtime, 30)

def get_timestamp_belong_half_min(timestamp):
    """
    找到时间戳所属的间隔为半分钟的时间戳
    >>> t = 1471586420.0
    >>> get_timestamp_belong_half_min(t)
    1471586400.0
    >>> t = 1471586450.0
    >>> get_timestamp_belong_half_min(t)
    1471586430.0
    """
    seconds = timestamp % 60
    if seconds >= 30:
        tmp = timestamp - seconds
        timestamp = tmp + 30
    else:
        timestamp -= seconds
    return timestamp


def query_visit_stream(key, key_type, fromtime, endtime, specify_db_path=None, limit=None):
    """
    获取一个小时内一段时间范围内每条记录的 user, 时间戳, 是否有报警
    Paramter:
    - key: (str)维度的值
    - key_type: (str)维度, ex. ip, page, user, did ,ipc    
    - fromtime: 时间戳 unix(float)    
    - endtime: 时间戳 unix(float)
    - specify_db_path:(str)
      指定的持久化数据库目录, 而非从timestamp中获取
    - limit:(int) 限制返回的个数
    Return
    [{user:(ip维度, 其他维度是ip), timestamp:, if_notice:}, ... ]
    """
    if limit is None:
        # 默认只返回2000个
        limit = 2000
    col_mappings = {
        'ip': 'c_ip',
        'did': 'did',
        'user': 'uid',
        'page': 'page',
        'sid': 'sid'
    }

    records, err = get_request_log(key, fromtime, key_type, specify_db_path, limit=limit, end=endtime)
    if records:
        notice_statistic = dict()
        http_count = 0
        traverse_count = 0
        result = []
        for record in records:
            if record:
                traverse_count += 1
                if record.name == 'HTTP_DYNAMIC':
                    http_count += 1

                    col_val = {show_col: record.get(get_col, '') for show_col, get_col in col_mappings.iteritems()
                               if show_col != key_type}
                    if_notice = True if record.notices or notice_statistic.get(record.id, False) else False
                    col_val['timestamp'] = record.timestamp
                    col_val['if_notice'] = if_notice
                    result.append(col_val)
                if traverse_count >= limit:
                    break

        logger.debug( DEBUG_PREFIX+'遍历过的非空的record 个数:%s', traverse_count)
        logger.debug( DEBUG_PREFIX+'遍历过的http_dynamic 个数:%s', http_count)
        return result
    else:
        logger.error("散点图获得日志出错:", err)
        return

def query_clicks_period(key, key_type, fromtime, endtime, specify_db_path=None):
    """
    每30s的DYNAMIC日志统计
    - key: (str)维度的值
    - key_type: (str)维度, ex. ip, page, user, did ,ipc    
    - fromtime: 时间戳 unix(float)    
    - endtime: 时间戳 unix(float)
    - specify_db_path:(str)
      指定的持久化数据库目录, 而非从timestamp中获取
    Return:
    {
      timestamp1: {count:, if_notice:True},
      ...
    }
    """
    limit = 10000000 # 1kw means no limit
    records, err = get_request_log(key, fromtime, key_type, specify_db_path, limit=limit, end=endtime)
    if records:
        result = dict( (ts, {'count':0, 'if_notice':False})
                       for ts in get_half_min_strs_fromtimestamp(fromtime, endtime))
        #    logger.debug(DEBUG_PREFIX+u"查询范围内的ts: %s", result.keys())
        tmp_records = dict() # {id: {if_notice , timestamp}}
        http_count = 0
        traverse_count = 0
        for record in records:
            if record:
                traverse_count += 1
                if record.name == 'HTTP_DYNAMIC':
                    http_count += 1
                    tmp_records[record.id] = dict(if_notice=bool(record.notices), timestamp=record.timestamp)
                elif record.pid in tmp_records and \
                     not tmp_records[record.pid]['if_notice'] and record.notices:
                    # 当子事件的父事件没有报警时，会补充报警信息
                    tmp_records[record.pid]['if_notice'] = True

        for k, r in tmp_records.iteritems():
            ts = get_timestamp_belong_half_min(r['timestamp']/ 1000.0)
#            logger.debug(DEBUG_PREFIX+u"时间戳%s, 查到的ts:%s", r['timestamp'], ts)
            if not result.has_key(ts):
                logger.debug(DEBUG_PREFIX+u"不是查询范围内的时间戳 :%s", ts)
                continue
            result[ts]['count'] += 1
            if not result[ts]['if_notice']:
                # 30s内任一条有报警，既标记
                result[ts]['if_notice'] = r['if_notice']

        logger.debug( DEBUG_PREFIX+'遍历过的非空的record 个数:%s', traverse_count)
        logger.debug( DEBUG_PREFIX+'遍历过的http_dynamic 个数:%s', http_count)
        return result
    else:
        logger.error("散点图获得日志出错:", err)
        return
    
def find_next_log_db_path(log_path, start_time_ts, end_time_ts):
    """
    从log_path出发 由早到近查找 start_time_ts (datetime obj) 和end_time_ts(datetime obj)之间存在的日志文件夹
    log_path ex. /path/to/persistent/2015040112/log
    Return:
    db_path ex. /path/to/persistent/2015040112/
    """

    persist_prefix, current_hour = log_path.rsplit('/', 2)[:-1]
    last_hour = datetime.strptime(current_hour, global_settings.LogPath_Format) + timedelta(hours=1)
    db_path = None
    while db_path is None:
        if last_hour > end_time_ts or last_hour < start_time_ts:
            break
        
        tmp_db_path = opath.join(persist_prefix, last_hour.strftime(global_settings.LogPath_Format))
        if opath.exists(tmp_db_path):
            db_path = tmp_db_path
            break
        last_hour = last_hour + timedelta(hours=1)
    return db_path

def query_log(start_time, end_time, query_terms, **kwargs):
    """
    日志查询
    start_time: timestamp
    end_time: timestamp
    query_terms:
    show_cols:
    size:
    specify_file_path:
    request_id:
    """

    from nebula_utils.persist_compute.condition import eval_statement
    shards = range(settings.Shard_Size)
    size = kwargs.get('size', 20)
    specify_file_path = kwargs.get('specify_file_path', None)
    offset = kwargs.get('offset', None)
    write_to_file= kwargs.get('write_to_file', False)
    show_cols = kwargs.get('show_cols', []) # api层保证不为空
    request_id = kwargs.get('request_id', None)

    start_time_ts = datetime.fromtimestamp(start_time)
    end_time_ts = datetime.fromtimestamp(end_time)
    logger.debug(DEBUG_PREFIX+"查询的参数: start_time: %s, endtime: %s, args:%s", start_time_ts, end_time_ts, kwargs)
    if not show_cols or not request_id:
        # 没有需要展示的字段, 可区别的放查询日志的文件名那还查询什么呢
        logger.warn(u'日志查询没有 展示字段和请求id参数')
        return

    def query_record(record, query_conds):
        if record is None:
            return
        if all( eval_statement(conds.get('op'),
                               record.get( conds.get('left'), None),
                               conds.get('right', None))
                for conds in query_conds):
            return record
        return

    # 生成查询的回调函数
    callbacks = []
    callbacks.append( partial(query_record, query_conds=query_terms) )

    if specify_file_path is None:
        # 第一次查询时，从start_time获取对应小时的日志的目录, 然后从0分片开始查找, offset 为默认的None
        try:
            db_path, log_path = get_log_path(None, start_time)
            filepath = opath.join(log_path, '0')
        except ValueError:
            logger.critical(u'timestamp: %s can not gain log path.', start_time)
            traceback.print_exc()
            return None
    else:
        # 第二次继续查询时, 从上次的文件继续, offset 一般不为空
        filepath = specify_file_path
        db_path = specify_file_path.rsplit('/', 2)[0]

    logger.debug(DEBUG_PREFIX+ u'查询的日志目录是:%s', filepath)
    # load查询小时内schema和header key的version
    logger.debug(DEBUG_PREFIX+ 'load 当前小时的配置: %s', db_path)
    if not opath.exists(db_path):
        db_path = find_next_log_db_path(log_path, start_time_ts, end_time_ts)
    if db_path is None:
        # 查询时间范围内没有日志文件夹
        return None #dict(info=u'查询时间范围内没有日志文件') @todo 查询结果信息
    else:
        filepath = opath.join(db_path, settings.LOG_PATH , '0')
        
    logger.debug(DEBUG_PREFIX+ u'查询的日志目录是:%s', filepath)
    # load查询小时内schema和header key的version
    logger.debug(DEBUG_PREFIX+ 'load 当前小时的配置: %s', db_path)
        
    utils.load_schema(db_path)
    utils.load_header_version(db_path)

    records = []
    result = dict()
    temp_records_file = None
    has_record_offset = False
    try:
        if write_to_file:
            # 只有第一次查询的时候会写文件，和统计总数
            result['temp_log_path'] = 'log_query_%s.csv' % request_id # just name
            temp_records_file_path = opath.join(global_settings.Temp_Query_Path, result['temp_log_path'])
            temp_records_file = open(temp_records_file_path, 'a') # @todo settings
            temp_records_file.write(codecs.BOM_UTF8)

            csv_writer = csv.writer(temp_records_file)
            csv_writer.writerow(show_cols)
            # 统计扫出来的日志总数
            result['total'] = 0
        while True:

            for record, context in Record.record_generator(filepath, offset, end_time):

                # callback不能放在record里计算，避免报错不继续查询
                if callbacks:
                    for func in callbacks:
                        record = func(record)

                if record:
                    if len(records) < size:
                        # 无论是否是第一次查询, 查询不到size都会填充
                        records.append(record)
                    elif len(records) == size:
                        if not has_record_offset:
                            result['logs'] = [
                                dict( (col,r.get(col, None)) for col in show_cols)
                                for r in records]
                            result['last_file'] = context['filepath']
                            result['last_offset'] = context['offset']
                            has_record_offset = True
#                        break
                    if write_to_file:
                        # 第一次查询将需要显示的字段写入临时的查询日志文件
                        csv_writer.writerow([ record.get(col , None) for col in show_cols])
                        # 统计日志数量
                        result['total'] += 1
                else:
                    continue

            # 如果不是第一次查询遍历全部，且已经找满了下一页的日志，就直接退出
            if has_record_offset and not write_to_file:
                break
            # 如果遍历完一个文件,找到的数量不够,然后就去下一个shard 如果当前小时都补不齐还要去拿下一个小时的文件夹来继续
            log_path, shard_name = filepath.rsplit('/', 1)
            next_filename = int(shard_name) + 1
            # 翻页查询时, 继续上次查询的offset之后，别的文件遍历需要清掉offset
            offset = 0
            if next_filename < settings.Shard_Size: #@todo
                filepath = opath.join(log_path, str(next_filename))
                logger.info(u'继续查找文件:%s', filepath)
            else:
                db_path = find_next_log_db_path(log_path, start_time_ts, end_time_ts)

                if db_path is None:
                    if not result.has_key('logs'):
                        # 最后一页如果找不满, 且超出了查找范围, 需要将结果返回，但是已经穷尽, 就没有last_file 和 last_offset了, 
                        result['logs'] = [
                            dict( (col,r.get(col, None)) for col in show_cols)
                            for r in records]
                    break
                    
                filepath = opath.join(db_path, settings.LOG_PATH , '0')
                # load查询小时内schema和header key的version
                logger.debug(DEBUG_PREFIX+ 'load 当前小时的配置: %s', db_path)
                utils.load_schema(db_path)
                utils.load_header_version(db_path)

    finally:
        if temp_records_file is not None:
            # 当第一次查询的时候会将查询到的日志写入到临时文件
            temp_records_file.close()

    return result

def get_request_log(key, timestamp, key_type, specify_db_path=None, eid=None, query=None, limit=None, end=None):
    """
    查询单条日志详情或者查询日志列表的工具函数

    Paramter:
    - key: (str)维度的值
    - key_type: (str)维度, ex. ip, page, user, did ,ipc
    - timestamp: 时间戳 unix(float) 起始时间戳

    - specify_db_path:(str)
      指定的持久化数据库目录, 而非从timestamp中获取

    - eid:(ObjectId)
      查询页面详情的时候需要的event id(come from bson ObjectId).

    - query:(str)
      过滤record的查询参数
    - limit:(int) 限制返回的个数
    - end: 时间戳 unix(float) 结束时间戳
    leveldb key pattern: 4bytes of ip, 4bytes of timestamp(分钟秒毫秒)
    """
    # a.获得持久化存储日志path, ex.
    # - db_path ex. /path/to/persistent/2015040112/index
    # - log_path ex. /path/to/persistent/2015040112/log
    from nebula_utils.persist_compute.condition import eval_statement
    try:
        db_path, log_path = get_log_path(specify_db_path, timestamp)
    except ValueError as e:
        logger.critical(u'key: %s, key_type: %s, specify_db_path: %s, timestamp: %s can not gain log path.', key, key_type, specify_db_path, timestamp)
        traceback.print_exc()
        return None, 'key:%s, %s' % (key, e.message)

    # 查询返回数量的限制, 默认20
    if limit is None:
        limit = 20

    # load查询小时内schema和header key的version
    utils.load_schema(db_path)
    utils.load_header_version(db_path)

    db_path = opath.join(db_path, settings.INDEX_PATH)

    # a. checkpoint db_path
    logger.debug( DEBUG_PREFIX+ '查询的db_path is: %s', db_path)
    logger.debug( DEBUG_PREFIX+ '查询的log_path is: %s', log_path)

    # 匹配 event id, 各种维度值的过滤函数
    # @todo 有event id匹配的，只匹配到一个就丢出来了
    filters = []
    filters.append( partial(Record.filter_header_keys, key=key, key_type=key_type) )

    if eid:
        filters.append( partial(Record.filter_event_id, event_id=eid) )
    
    def filter_timestamp(record, fromtime, endtime):
        if record is None:
            return None
        if fromtime and record.timestamp < fromtime * 1000:
            return None
        if endtime and record.timestamp > endtime * 1000:
            return None
        return record

    def query_record(record, query):
        if record is None:
            return None
        if all([any(eval_statement(cond.get('op'), record.get(cond.get('left'), None), cond.get('right', None))
                    for cond in conds) for conds in query]):
            return record
        return None

    # 查询、格式化record字段的毁掉函数注册
    callbacks = list()
    callbacks.append(partial(filter_timestamp, fromtime=timestamp, endtime=end))

    try:
        db = get_db(db_path)
        if not db:
            return
        if eid:
            log_offsets = Index.get_offsets(key, key_type, db, timestamp)
        else:
            log_offsets = Index.get_offsets(key, key_type, db, timestamp, end=end)

        # [(logname, offset, tens), ..]
        logger.debug(DEBUG_PREFIX+'获取的索引值们是:%s', log_offsets)
        records = []
        error_infos = []
        http_count = 0
        traverse_count = 0
        for index, _ in enumerate(log_offsets):
            
            logname, offset, tens = _
            if index + 1 < len(log_offsets):
                # 不是最后一个索引时
                # 如果当前索引的时间和下一个索引一样就需要读取下一个索引的日志, 即使达到size限制.
                need_read_next_index = (tens == log_offsets[index+1])
            else:
                need_read_next_index = False

            filepath = opath.join(log_path, str(logname))
            logger.debug(FORCUS_PREFIX+'当前使用的日志文件: %s, 偏移量是:%s', filepath, offset )
            # 搜索范围应该是从索引里的时间来获取
            search_time_begin = int(timestamp) / 3600 * 3600 + tens*10
            for record, context in Record.record_generator(\
                    filepath, offset, search_time_begin,\
                    filters=filters, callbacks=callbacks):

                if query:
                    # 搜索字段增加，将过滤条件放在record generator之后，避免缺失字段
                    if not query_record(record, query):
                        continue

                if record:
                    if record.name == 'HTTP_DYNAMIC':
                        http_count += 1
                    logger.debug( DEBUG_PREFIX+"找到一个.. filepath:%s, offset:%s", filepath, context.get('offset'))
                    traverse_count += 1
                    if len(records) >= limit and not need_read_next_index:
                        # 当上个索引的时间和当前索引一样时会继续读取日志
                        # 满足查询返回的大小就跳出
                        break
                    else:
                        records.append(record)
                else:
                    error_infos.append(context.get('err', ''))

        logger.debug( DEBUG_PREFIX+'遍历过的非空的record 个数:%s', traverse_count)
        logger.debug( DEBUG_PREFIX+'遍历过的http_dynamic 个数:%s', http_count)
        # 逐条的错误信息包括, 所有传入参数, key, key_type, timestamp, 这层再加上 db_path
        err = ';\n'.join([_ for _ in error_infos if _])
        if records:
            logger.debug(DEBUG_PREFIX+ 'record获取的错误们:%s', err)
            return records, err
        else:
            return None, err
    finally:
        if locals().has_key('db'):
            del db

def scan_log(key, timestamp, key_type, specify_db_path=None, eid=None, query=None, limit=None, end=None):
    """
    测试不用索引的扫描

    Paramter:
    - key: (str)维度的值
    - key_type: (str)维度, ex. ip, page, user, did ,ipc
    - timestamp: 时间戳 unix(float)

    - specify_db_path:(str)
      指定的持久化数据库目录, 而非从timestamp中获取

    - eid:(ObjectId)
      查询页面详情的时候需要的event id(come from bson ObjectId).

    - query:(str)
      过滤record的查询参数
    leveldb key pattern: 4bytes of ip, 4bytes of timestamp(分钟秒毫秒)
    """
    # a.获得持久化存储日志path, ex.
    # - db_path ex. /path/to/persistent/2015040112/index
    # - log_path ex. /path/to/persistent/2015040112/log
    try:
        db_path, log_path = get_log_path(specify_db_path, timestamp)
    except ValueError as e:
        logger.critical(u'key: %s, key_type: %s, specify_db_path: %s, timestamp: %s can not gain log path.', key, key_type, specify_db_path, timestamp)
        traceback.print_exc()
        return None, 'key:%s, %s' % (key, e.message)

    # 查询返回数量的限制, 默认20
    if limit is None:
        limit = 20

    # load查询小时内schema和header key的version
    utils.load_schema(db_path)
    utils.load_header_version(db_path)

    db_path = opath.join(db_path, settings.INDEX_PATH)

    # a. checkpoint db_path
    logger.debug( DEBUG_PREFIX+ '查询的db_path is: %s', db_path)
    logger.debug( DEBUG_PREFIX+ '查询的log_path is: %s', log_path)

    # 匹配 event id, 各种维度值的过滤函数
    # @todo 有event id匹配的，只匹配到一个就丢出来了
    filters = []
    filters.append( partial(Record.filter_header_keys, key=key, key_type=key_type) )

    def filter_timestamp(record, fromtime, endtime):
        if record is None:
            return None
        if fromtime and record.timestamp < fromtime * 1000:
            return None
        if endtime and record.timestamp > endtime * 1000:
            return None
        return record
    # 查询、格式化record字段的毁掉函数注册
    callbacks = []
    
    callbacks.append(partial(filter_timestamp, fromtime=timestamp, endtime=end))

    try:
        db = get_db(db_path)
        if not db:
            return
        if eid:
            log_offsets = Index.get_offsets(key, key_type, db, timestamp)
        else:
            log_offsets = Index.get_offsets(key, key_type, db, timestamp, end=end)

        # [(logname, offset), ..]
        logger.debug(DEBUG_PREFIX+'获取的索引值们是:%s', log_offsets)
        records = []
        error_infos = []
        http_count = 0
        traverse_count = 0
        lognames = set([ logname for logname, offset, tens in log_offsets])
        for logname in lognames:
            filepath = opath.join(log_path, str(logname))
#            logger.debug(FORCUS_PREFIX+'当前使用的日志文件: %s, 偏移量是:%s', filepath )
            # 搜索范围应该是从索引里的时间来获取
#            search_time_begin = int(timestamp) / 3600 * 3600 + tens*10
            for record, context in Record.record_generator(\
                    filepath, filters=filters, callbacks=callbacks):

                record = record
                if record:
                    logger.debug( DEBUG_PREFIX+"找到一个.. filepath:%s, offset:%s", filepath, context.get('offset'))
                    traverse_count += 1
                    if len(records) >= limit:
                        # 满足查询返回的大小就跳出
                        break
                    else:
                        records.append(record)
                else:
                    error_infos.append(context.get('err', ''))
            if len(records) >= limit:
                # 满足查询返回的大小就跳出
                break
        logger.debug( DEBUG_PREFIX+'遍历过的非空的record 个数:%s', traverse_count)
        # 逐条的错误信息包括, 所有传入参数, key, key_type, timestamp, 这层再加上 db_path
        err = ';\n'.join([_ for _ in error_infos if _])
        if records:
            logger.debug(DEBUG_PREFIX+ 'record获取的错误们:%s', err)
            return records, err
        else:
            return None, err
    finally:
        if locals().has_key('db'):
            del db

if __name__ == '__main__':
    records, err = scan_log('58.19.18.24', None, 'ip', '/home/threathunter/nebula/persistent/2016082913/', limit=200)
    print len(records or [])
#    a = '10.4.36.154'
#    print get_request_log(a, 1460798322.906152,)
#    print get_request_log(a, 1460800725.588, True) # detail
