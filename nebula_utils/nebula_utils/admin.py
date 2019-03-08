# -*- coding: utf-8 -*-
"""
这里可以放一下一些检查数据完整性的工具函数们,
目前只有解析log, 以检测数据完整性

TODO:
- 按照命令行输入去调用各个功能
- 检查某个小时的目录里面的索引的正确性
"""

import sys, traceback
from os import path as opath

from persist_utils.log import Record
from persist_utils import utils, settings

aRecord = None
record_gen = None
last_offset_fn = '.debug_last_offset'
lastest_offset = None
last_offset = None

doc = '''
 请输入以下指令:
- q: quit
- n: next seg
- now: 当前文件流的offset
- goto: 跳转到当前文件指定的偏移量
- read: 读取指定字节数大小的文件流
- back: 跳回上一次解析(ex. read操作)的偏移量
- doc | help | h: 打印offset

'''
#给定一个log日志全路径, 只检查是否存在
print '请输入log文件的目录:'
path = raw_input()

if not opath.exists(path):
    print 'log文件的目录并不存在'
    sys.exit(-1)

utils.load_schema(path)
utils.load_header_version(path)

assert utils.name2_schema is not None, "不能成功加载文件夹:%s 中的 %s" % (path, settings.SCHEMA_FILE_NAME)

assert utils.Header_Version is not None, "不能成功加载文件夹:%s 中的 %s" % (path, settings.HEADER_VERION_FILE)

print 'log文件名:'
filename = raw_input()

fn = opath.join(path, 'log', filename)

if not opath.exists(fn):
    print '输入的文件不存在!'
    sys.exit(-1)

try:
    with open(fn, 'rb') as f:
        print doc
        
        while True:
            sw = raw_input()
            if sw == 'q':
                break
            elif sw == 'n':
                # 这边抛异常了， 会回到解析开始的地方
                if aRecord is None:
                    aRecord = Record()
                    record_gen = aRecord.unpack_record_debug(f)
                try:
                    field, field_value, field_type = record_gen.next()
                except StopIteration:
                    # @todo 不完整的record，文件结束的处理
                    record_gen = aRecord.unpack_record_debug(f)
                    field, field_value, field_type = record_gen.next()
                last_offset = lastest_offset
                lastest_offset = f.tell()
                print 'field:%s, value is %s, type is %s' % (field, field_value, field_type)
            elif sw == 'back':
                if last_offset:
                    print '跳回到偏移量: %s' % last_offset
                    f.seek(last_offset)
            elif sw == 'goto':
                print '输入跳转的偏移量:'
                goto = raw_input()
                f.seek(int(goto))
            elif sw == 'read':
                print '输入读取文件流的大小:'
                size = raw_input()
                print (f.read(int(size)), )
            elif sw == 'now':
                print 'current offset: ', f.tell()
            elif sw in ('doc', 'help', 'h'):
                print doc
except Exception:
    traceback.print_exc()
    if lastest_offset:
        print '解析的未知异常发生前所处文件%s, 的偏移量: %s处' % (fn, lastest_offset)
#@todo 光存个offset是不能恢复现场的        
#        with open(last_offset_fn, 'w') as f:
#            f.write(last_offset)
    sys.exit(-1)