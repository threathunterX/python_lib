# -*- coding: utf-8 -*-

import logging, json
from os import path as opath
from functools import partial
from datetime import datetime, timedelta

from tornado.httpclient import HTTPClient
from tornado.escape import json_decode, json_encode

from threathunter_common.util import curr_timestamp

from . import settings
from nebula_utils import settings as global_settings
from nebula_utils.persist_utils.metrics import catch_latency

DEBUG_PREFIX = '==============='
logger = logging.getLogger('nebula_utils.persist_utils.utils')


# 不同log对应body的schema
# ex. {'httplog':[{"field":"sid", "type":"string"},...]}
name2_schema = None # 需要从每个小时生成的日志文件夹load ex. /path/to/persistent/2055011208/record_schema.json

# 持久化记录的log的 不同版本header部分的各维度值部分结构说明
# ex.{"0": ["ip"],}
# 需要从每个小时生成的日志文件夹load ex. /path/to/persistent/2055011208/header_version.json
Header_Version = None

# 持久化数据index目录所写的索引前一个字节的type
IP_Record_Type = 1
PAGE_Record_Type = 2 
UID_Record_Type = 3
DID_Record_Type = 4

key_type2_index = {
    'ip':IP_Record_Type,
    'user': UID_Record_Type,
    'did': DID_Record_Type,
    'page': PAGE_Record_Type
}
# 持久化数据data目录所写的统计数据前一个字节的type
IP_Stat_Type = 2
IPC_Stat_Type = 3
PAGE_Stat_Type = 4
UID_Stat_Type = 5
DID_Stat_Type = 6

type2struct = {
    'double':'>d', # 8 bytes float
    'long':'>Q', # 8bytes unsigned int
    'string':'s',
    'int':'>q', # 8bytes int
    'boolean':'?', # 1byte
}

# 内部的这些大小都是无符号的.
Timestamp_f = '>Q' # 8 bytes unsigned int
Str_Suf_f = 's'
IP_Size_f = '>L' # 4bytes unsigned int
BODY_SIZE_f = '>H' # 2bytes unsigned int
RECORD_SIZE_f = '>H' # 2bytes unsigned int
RECORD_ID_f = '>QL' # 12 bytes id, joint by 8 and 4 bytes
Header_Ver_f = '>B' # 1byte unsigned int

# 请求nebula web API
#Auth_Code = None
Strategies_Weigh = None


def load_schema(db_path):
    global name2_schema
    schema_path = opath.join(db_path, settings.SCHEMA_FILE_NAME)
    if not opath.exists(schema_path):
        logging.error( u'path: %s schema文件: %s 并不存在', db_path, schema_path)
        return
        
    with open(schema_path, 'r') as f:
        events_schema = json.load(f)
        name2_schema = dict()
        for schema in events_schema:
            name = schema['name']
            name2_schema[name] = list()

            for proper in schema.get('properties', []):
                proper_type = proper['type']
                proper_field = proper['name']
                name2_schema[name].append(dict(type=proper_type, field=proper_field))


def load_header_version(db_path):
    global Header_Version
    file_path = opath.join(db_path, settings.HEADER_VERION_FILE)
    if not opath.exists(file_path):
        logging.error( u'path: %s schema文件: %s 并不存在', db_path, file_path)
        return
        
    with open(file_path, 'r') as f:
        Header_Version = json.load(f)


@catch_latency("查询日志格式")
def load_record_settings(db_path):
    load_schema(db_path)
    load_header_version(db_path)
    global Header_Version, name2_schema
    if Header_Version is None or name2_schema is None:
        raise SystemError, u'不能获取统计日志的格式配置，不能继续,请检查 %s 目录下, 文件: %s 和 %s' % (db_path, settings.SCHEMA_FILE_NAME, settings.HEADER_VERION_FILE)
    
class Storage(dict):
    """
    A Storage object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`.
    
        >>> o = storage(a=1)
        >>> o.a
        1
        >>> o['a']
        1
        >>> o.a = 2
        >>> o['a']
        2
        >>> del o.a
        >>> o.a
        Traceback (most recent call last):
            ...
        AttributeError: 'a'
    
    """
    def __getattr__(self, key): 
        try:
            return self[key]
        except KeyError, k:
            raise AttributeError, k
    
    def __setattr__(self, key, value): 
        self[key] = value
    
    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError, k:
            raise AttributeError, k
    
    def __repr__(self):     
        return dict.__repr__(self)

        
def get_last_hour(f='%Y%m%d%H'):
    """
    获取上个小时的时间字符串, ex. 2015040915 
    """
    n = datetime.now()
    td = timedelta(seconds=3600)
    last_hour_n = n - td
    return last_hour_n.strftime(f)

def get_db_path_from_timestamp(timestamp, f='%Y%m%d%H'):
    """
    从时间戳 获取 每小时产生的持久化文件夹的名字 ex. 2015040915 
    """
    d = datetime.fromtimestamp(timestamp)
    now = datetime.now()
    # @todo 请求当前小时， 大于30天最大存储
    return d.strftime(f)
    
def get_path(timestamp=None, base_path=None):
    """
    根据时间戳,目录前缀 获取持久化日志所在的完整路径, ex. base_path/2015040915
    """
    if timestamp is None:
        path_name = get_last_hour()
    else:
        path_name = get_db_path_from_timestamp(timestamp)
    return opath.join(base_path, path_name)
    
def get_db_path(timestamp):
    return get_path(timestamp, global_settings.DB_ROOT_PATH)

def get_byte_array(i, size = None):
    """
    将id pid的 8个字节生成bytearray 但是看上去id  ,pid
    # 但是id pid都是存的都是string
    """

    if size is None:
        size = 8
    array = []
    for _ in xrange(size):
        array.append( i & 255 )
        i = i >> 8
    
    array.reverse()
    return bytearray(array)


def get_last_hour_timestamp():
    """
    获取上个小时整点时间戳,ex. 1471168800000
    """
    return (curr_timestamp() / 3600 * 3600 - 3600) * 1000


def parse_host_url_path(url):
    if url.find('/') == -1:
        # ex. 183.131.68.9:8080, auth.maplestory.nexon.com:443
        host = url
        url_path = ''
    else:
        if url.startswith('http') or url.startswith('https'):
            # 有协议的, 需要扩充
            segs = url.split('/', 3)
            host = '/'.join(segs[:3])
            url_path = segs[-1]
        else:
            host, url_path = url.split('/', 1)
    return host, url_path


def get_auth_code():
    """
    请求nebula web API所需的auth code
    Returns:
    """

#    global Auth_Code
#
#    if Auth_Code is None:
#
#        client = HTTPClient()
#        # 登录
#        login_url = 'http://{}:{}/auth/login'.format(WebUI_Address, WebUI_Port)
#        login_user = 'threathunter'
#        login_pa = 'threathunter.com'
#        login_body = json_encode(dict(username=login_user, password=login_pa))
#        login_res = client.fetch(login_url, method='POST', body=login_body)
#        res = json_decode(login_res.body)
#        if res['auth'] is not True:
#            raise RuntimeError, '不能登录url: %s' % login_url
#        else:
#            Auth_Code = res['code']

    from nebula_utils import settings
    return settings.Auth_Code


@catch_latency("查询计算变量")
def fetch_compute_variables():
    auth_code = get_auth_code()
    url = 'http://{}:{}/platform/variables?modules=slot&modules=base&auth={}'.format(
        global_settings.WebUI_Address, global_settings.WebUI_Port, auth_code)
    hc = HTTPClient()

    logger.debug(DEBUG_PREFIX + u"获得的认证令牌是:%s", auth_code)

    # 获取计算变量们
    res = hc.fetch(url)
    try:
        jres = json_decode(res.body)
    except ValueError as e:
        # 可能是
        raise ValueError, e.message + "response body:" + res.body

    compute_variables = jres['values']

    return compute_variables


@catch_latency("查询策略权重")
def get_strategies_weigh():
    """
    请求nebula web API获取strategies,包含每个策略的场景、分数、标签等数据
    Returns:
    """

    global Strategies_Weigh

    if Strategies_Weigh is None:
        Strategies_Weigh = dict()

        auth_code = get_auth_code()
        client = HTTPClient()
        url = 'http://{}:{}/nebula/strategyweigh?auth={}'.format(
            global_settings.WebUI_Address, global_settings.WebUI_Port, auth_code)
        strategies_response = client.fetch(url, method='GET')
        res = json_decode(strategies_response.body)
        if res.get('msg', '') == 'ok':
            strategies = res.get('values', [])
            for strategy in strategies:
                if not strategy:
                    continue
                name = strategy.pop('name')
                Strategies_Weigh[name] = strategy


@catch_latency("清除web API缓存")
def clean_cache():
    """
    调用nebula web清理缓存API
    """
    logger.info("开始清除web API缓存")

    auth_code = get_auth_code()
    client = HTTPClient()
    url = 'http://{}:{}/platform/stats/clean_cache?auth={}&url=/platform/stats/offline_serial&method=GET'.format(
        global_settings.WebUI_Address, global_settings.WebUI_Port, auth_code)
    res = client.fetch(url, method='GET')
    body = json.loads(res.body)
    if body.get('status') == 0:
        logger.info('清除web API缓存完成')
    else:
        logger.error('清除web API缓存失败')
