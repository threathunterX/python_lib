# -*- coding: utf-8 -*-
import logging, os
from binascii import hexlify
from os import path as opath
from struct import calcsize, pack
from datetime import datetime
import traceback
from itertools import chain

from nebula_utils.persist_utils import settings
from nebula_utils.persist_utils.log import Record, Index
from nebula_utils.persist_utils.db import db_context, get_db

from nebula_utils.persist_utils import utils as wutils

DEBUG_PREFIX = '==============='

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('test_index_uniformity')

def generate_index(db_path):
    func_prefix = ' generate_index'
    print "=========== python generate index ==================="
    #log_path = opath.join(db_path , wsettings.LOG_PATH)
        
    _, logs_path = Index.get_log_paths(db_path)
    
#    logger.debug('generate index from: %s, logs_path:%s', db_path, logs_path)

    index_cache = dict()
    # index(index's key): { filename:offset, filename1:offset1}
    record_count = 0
    for r,err in log_record_generator(logs_path):
        record_count += 1
        for col, key_type in [('c_ip','ip'),('uid', 'user'), ('did', 'did'), ('uri_stem', 'page')]:
            if r.has_key(col) and r[col]: # 临时不做空值的索引 @issue
                try:
                    index_list = Index.generate_key(
                        key_type, r[col], datetime.fromtimestamp(r["timestamp"]/1000))
                except ValueError:
                    print 'key_type:%s ,key: %s, filepath:%s, timestamp:%s, offset:%s, 无法生成索引key.\n' % (key_type, r[col], err['filepath'], r.timestamp, r.buff_startpoint)
                    continue
#                logger.debug(DEBUG_PREFIX+"生成的索引的byte list是%s", (index_list,))
                cache_key = str(index_list)
                if cache_key not in index_cache:
#                    logger.debug('filname:%s, type is %s, pos:%s ,type is %s', filename, type(filename), pos, type(pos))
                    cache_dict = index_cache[cache_key] = dict()
                else:
                    cache_dict = index_cache[cache_key]
                fn = int(err['filepath'].rsplit('/',1)[-1])
                if not cache_dict.has_key(fn):
                    cache_dict[fn] = r.buff_startpoint

#    logger.debug(DEBUG_PREFIX+"获取的record的count为%s", record_count)
    
    output_index_cache = dict()
    for index, index_dict in index_cache.iteritems():
        offsets = ''.join( pack('>BL', fn, pos) for fn,pos in index_dict.iteritems() )
        output_index_cache[index] = offsets
    # index: offset_value
    return output_index_cache
    
def write_index(db_path, index_path, index_dict):
    """
    生成测试所引用
    """
    db = get_db(opath.join(db_path, index_path))
    for k,v in index_dict.iteritems():
        db.Put(k, v)
    del db

def test_generate_index(db_path, index_path):
    index_cache = generate_index(db_path)
    write_index(db_path, index_path, index_cache)
    
def test_python_index(db_path):
    index_path = 'temp_index'
    wutils.load_schema(db_path)
    wutils.load_header_version(db_path)
    
    test_generate_index(db_path, index_path)
    test_index_intact(db_path, index_path)
    
def test_index_intact(db_path, index_path):
    print "=========== 检查index  %s===================" % index_path
    wutils.load_schema(db_path)
    wutils.load_header_version(db_path)
    index_path = opath.join(db_path , index_path)
    logfilepaths = [ opath.join(db_path, settings.LOG_PATH, str(_)) for _ in range(16)]
    total = 0
    fail_n = 0
    with db_context(index_path) as index_db:
        for r,err in log_record_generator(logfilepaths):
            total +=1
            fail = False
            if r:
                ip = r.c_ip
                user = r.uid
                did = r.did
                page = r.uri_stem
                timestamp = datetime.fromtimestamp(r.timestamp / 1000.0)
                timeslot = ((timestamp.minute * 60) + timestamp.second)/10
                start_point = r.buff_startpoint
                log_name = err['filepath'].rsplit('/',1)[-1]
                
                key_dicts = dict(ip=ip, user=user, did=did, page=page)
                for key_type, key in key_dicts.iteritems():
                    if not key:
#                        print 'key_type', key_type, ' key is blank'
                        continue
                    try:
                        index_offsets = Index.get_offsets(key, key_type, index_db, r.timestamp / 1000.0)
                    except ValueError:
                        print 'key_type:%s ,key: %s, filepath:%s, timestamp:%s, timeslot:%s, offset:%s, 无法生成索引key.\n' % (key_type, key, err['filepath'], timestamp, timeslot, start_point), 
                        continue
                    
                    if index_offsets:
                        # 判断索引是否正确
                        same_log_file = [ _ for _ in index_offsets if str(_[0]) == str(log_name)]
                        same_timeslot = [ _ for _ in same_log_file if str(_[2]) == str(timeslot)]
                        if not same_timeslot:
                            fail = True
                            print 'key_type:%s ,key: %s, filepath:%s, timestamp:%s, timeslot:%s, offset:%s, 应该有索引:%s , 但是获得的所有索引是:%s.\n' % (key_type, key, err['filepath'], timestamp, timeslot, start_point, (int(log_name), start_point, timeslot), index_offsets), 
                    else:
                        print 'key_type:%s ,key: %s, filepath:%s, timestamp:%s, timeslot:%s, offset:%s, 没有索引.\n' % (key_type, key, err['filepath'], timestamp, timeslot, start_point), 
                    
            else:
                print(u"文件: %s，日志起始偏移量: %s 日志解析错误: %s\n" % (err['filepath'], err['start_point'], err['err']))
            if fail:
                fail_n += 1
    print 'total record:%s, success: %s, fail ratio:%s'% (total, total-fail_n, float(fail_n)/total)
    
def log_record_generator(filepaths):
    for g in chain( Record.record_generator(f) for f in filepaths ):
        for r, err in g:
            yield r,err
    
    
if __name__ == '__main__':
    import sys
    fp = sys.argv[1] if len(sys.argv)>1 else None
    print fp
    if fp:
        test_index_intact(fp,'index')
        test_python_index(fp)
