# -*- coding: utf-8 -*-

from contextlib import contextmanager

import leveldb

@contextmanager
def db_context(path):
    """
    根据路径获得leveldb实例, 上下文结束自动释放
    """

    db = leveldb.LevelDB(path, create_if_missing=True)
    yield db
    del db

def get_db(path):
    return leveldb.LevelDB(path )

def scan_keys(prefix, db, include_value=True):
    """
    根据key的prefix来筛选对应的key
    """
    if db is None:
        return []
    end = bytearray(prefix)
    size_end = len(end)
    for index, _ in enumerate(end[::-1], 1):
        # 从end 后到前 + 1去scan key, 避免任一字段为255的情况
        if _ != 255:
            end[size_end-index] = end[size_end-index] + 1
            break
#    end[-1] = end[-1] + 1
    if include_value:
        return [ (k,v) for k,v in db.RangeIter(key_from=prefix, key_to=end, include_value=True)]
    else:
        return [k for k in db.RangeIter(key_from=prefix, key_to=end, include_value=False)]

def scan_keys_between(start, end, db, include_value=True):
    """
    根据key的[start, end)来筛选对应的key
    """
    if db is None: return []
    if include_value:
        return [ (k,v) for k,v in db.RangeIter(key_from=start, key_to=end, include_value=True)]
    else:
        return [k for k in db.RangeIter(key_from=start, key_to=end, include_value=False)]
