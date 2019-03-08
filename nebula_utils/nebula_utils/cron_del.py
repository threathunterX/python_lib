#!/usr/local/bin/python
# -*- coding: utf-8 -*-

"""
因为对动态API的http流量做成了访问日志持久化下来，量可能很大，所以需要定期清理。
但是带来一个问题，就是根据持久化日志统计出来的数据跟这边是分开存储的，所以用户使用时可能出现界面上还有统计数据，但是详情页查不到日志的情况。
"""

import os
from os import path as opath
from os.path import isdir
from shutil import rmtree
from datetime import datetime

import nebula_utils.settings as settings

leveldb_path = settings.DB_ROOT_PATH

today = datetime.today()

def ifdir_need_del(path_name):
    """
    目录是否过期，未知名字也会略过
    
    只有过期会返回True
    """
    try:
        path_time = datetime.strptime(path_name, settings.LogPath_Format)
    except ValueError:
        print '未知的leveldb持久化的目录', path_time
        return False
        
    if (today - path_time).days > settings.Expire_Days:
        return True
    
    return False
    

def main():
#    print os.listdir(leveldb_path)
    to_del_paths = ( opath.join(leveldb_path, _) for _ in os.listdir(leveldb_path)
                     if isdir(opath.join(leveldb_path,_)) and ifdir_need_del(_))
    to_del_paths = list(to_del_paths)
    if to_del_paths:
        print today, '有以下目录需要删除', to_del_paths
    else:
        print today, '没有目录需要删除'
    map(rmtree, to_del_paths)
    
if __name__ == '__main__':
    main()
