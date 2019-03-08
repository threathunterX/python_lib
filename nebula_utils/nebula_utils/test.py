# -*- coding: utf-8 -*-

import os, traceback
from datetime import datetime
from struct import calcsize
from itertools import permutations

from persist_utils import extract_header
from persist_utils.log import Record, Index
from persist_utils.db import db_context
from persist_utils import utils

def parse_file(filepath):
    """
    @工具函数 顺序解析某个日志文件,检查日志文件格式
    """
    with open(filepath, 'rb') as f:
        try:
            fsize = os.stat(filepath).st_size
            ifovertime,ifmatch, record_header = extract_header(f, ismatch=False)
            while not ifovertime:
                # 正常都命中的情况 yield util next 10s' timestamp 
                record = Record.unpack_body(f, record_header)
                yield record
                ifovertime,ifmatch, record_header = extract_header(f, ismatch=False)
        except RuntimeError as e:
            if fsize == f.tell():
                # 文件结束
                pass
            if fsize - f.tell() < calcsize(e.message):
                # 不完整的一条日志
                print 'incomplete record, file:%s, file_size:%s, now at:%s, to read size: %s' % (filepath,fsize, f.tell(), calcsize(e.message))
            print 'Unpack error: file:%s , position: %s format:%s' % (filepath, f.tell(),e.message )
        except TypeError as e:
            traceback.print_exc()
            print 'Unpack error: %s , file: %s, at position:%s' % (e.message, filepath, f.tell())

def index_writer():
    """
    遍历ip生成对应索引
    """
    tmp_db_path = ''
    with db_context(tmp_db_path) as db:
        ip_seg_gen = permutations(xrange(255), 4)
        for ip_segs in ip_seg_gen:
            # 索引类型
            t = utils.IP_Record_Type
            ip = '.'.join(map(str, ip_segs))
            timestamp = datetime.now()
            index = Index.generate_key(t, ip, timestamp)
            db.Put(index, 0)

def index_reader():
    """
    检查索引格式 测试函数
    """

def index_examine(index_path, log_path):
    """
    遍历索引，查询出来offset是否含有对应的ipc
    """
