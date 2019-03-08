# -*- coding: utf-8 -*-
"""
脚本说明: 日志查询功能提供脚本功能和使用方法的说明

# 查询时间范围字段参数 st, et
时间戳, 浮点数, 以秒为单位

# 展示字段参数 show_cols 说明
 当前事件所有事件字段说明: 只能去对应小时里面看完整的记录,因为根据事件日志的类型不同，字段会有些许变化 ex. /data/persistent/2016122116/events_schema.json
 常用字段们: c_ip(client ip), timestamp(时间戳, ms), uri_stem(请求url), uid(user id), did(device id)
 特殊字段: buff_startpoint, 内部使用的该条日志所在文件的偏移量, 但是并没有一个所在文件名的说明.

# 过滤条件参数 query_terms 说明:
 query_terms是形如的表达式字典组成的列表{'left':[col], 'right':[val], 'op': [operator]}, 只有列表中的表达式都满足的时候才会命中, left字段是事件的某一个字段, op字段代表操作符, right字段是用户指定的值

 常用过滤操作符: contain, !contain, stringequals, ==, > , >=, !=, startwith, !startwith, endwith, !endwith

# request_id 对于页面使用来说是必须的, 这里是顺便填的

# 其他参数说明

除了st, et, query_terms参数顺序固定且不需要指定参数名外，其他参数都需要指定参数名

ex. show_cols=show_cols, request_id=99

## size

返回的命中的日志个数， 因为时间范围内命中的日志可能很多，避免长时间无响应，一旦size满足就停止扫描

# query_log函数返回说明

ex.{'logs': [{'timestamp': 1482307200338, 'c_ip': '119.37.12.99'}, {'timestamp': 1482307200335, 'c_ip': '111.172.127.124'}, {'timestamp': 1482307200344, 'c_ip': '120.40.6.139'}, {'timestamp': 1482307200332, 'c_ip': '120.195.227.53'}, {'timestamp': 1482307200333, 'c_ip': '112.26.73.210'}, {'timestamp': 1482307200334, 'c_ip': '124.115.251.206'}, {'timestamp': 1482307200857, 'c_ip': '60.215.125.116'}, {'timestamp': 1482307200334, 'c_ip': '125.94.208.2'}, {'timestamp': 1482307200852, 'c_ip': '182.35.123.229'}]}

其中'logs'字段包含了查询到的日志的列表

# 原理及用途说明
因为这里没有使用索引,是遍历的方式去查询日志, 可以用来佐证日志索引的问题,其他的需求诸如需要导出原始日志来手工分析的时候。

# TODO
增加query_log 函数增加DEBUG参数, 给命中的日志增加file_path字段, 这样拿到命中日志的file_path和具体的偏移量，就可以利用另一个nebula_utils/admin.py调试具体位置的日志了。

"""
import time
import logging
logging.basicConfig(level=logging.DEBUG)

from nebula_utils.persist_utils import query_log

##################### Help Functions ########################

def get_hour_start(point=None):
    """
    获取point时间戳所在的小时的开始的时间戳, 默认获取当前时间所在小时的开始时的时间戳
    """
    if point is None:
        p = time.time()
    else:
        p = point
        
    return ((int(p) / 3600) * 3600) * 1.0

def get_current_hour_interval():
    fromtime = int(get_hour_start()) * 1000
    endtime = int(time.time() * 1000)
    return fromtime, endtime

def get_last_hour_interval():
    current_hour = int(get_hour_start()) * 1000
    fromtime = current_hour - (3600 * 1000)
    endtime = current_hour - 1
    return fromtime, endtime

#####################  Usage ###############################
# 查询上个小时ip中包括'12'字段的日志, 显示c_ip, timestamp字段 
st, et = get_last_hour_interval()
st = st / 1000.0
et = et / 1000.0

show_cols = ['c_ip', 'timestamp']

query_terms = [{'right':'12', 'left':'c_ip', 'op':'contain'}]

print query_log(st, et, query_terms, show_cols=show_cols, request_id=99)

# 查询上个小时ip为'119.37.12.99' 的日志 
#st, et = get_last_hour_interval()
#st = st / 1000.0
#et = et / 1000.0
#
#show_cols = ['c_ip', 'timestamp', 'uri_stem', 'buff_startpoint']
#
#query_terms = [{'right':'119.37.12.99', 'left':'c_ip', 'op':'=='}]
#
#print query_log(st, et, query_terms, show_cols=show_cols, request_id=99)

