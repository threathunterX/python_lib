# -*- coding: utf-8 -*-

import os, traceback, logging, sys
from struct import pack, unpack, calcsize, error
from os import path as opath
from datetime import datetime
from itertools import chain

import mmh3

from nebula_utils import settings
from nebula_utils.persist_utils import settings as usettings
from . import utils
from .db import db_context, scan_keys, scan_keys_between, get_db
from .bson.objectid import ObjectId

logger = logging.getLogger('nebula_utils.persist_utils.log')
DEBUG_PREFIX = '==============='


class RecordNotMatch(Exception):
    pass

class OverSearchScope(Exception):
    pass

def unpack_seg(seg_format, buff, ifall=False):
    """
    根据给出的format从buff中解析，并生成对应类型的数据
    Paramters:
    seg_format: format of struct
    buff: buffer (file or stringio obj, need read method)
    ifall: 是否返回解析出的tuple中所有项
    """
    try:
        t = buff.read(calcsize(seg_format))
        tuples = unpack(seg_format, t)
        if tuples:
            if ifall:
                return tuples
            return tuples[0]
    except error:
        raise RuntimeError, seg_format

class Record(utils.Storage):
    @classmethod
    def get_record_size(cls, buff):
        """
        return int
        """
        return unpack_seg(utils.RECORD_SIZE_f, buff)

    @classmethod
    def get_app_offset(cls, buff):
        """
        return int
        """
        return unpack_seg(utils.RECORD_SIZE_f, buff)

    @classmethod
    def get_timestamp(cls, buff):
        return unpack_seg(utils.Timestamp_f, buff)

    @classmethod
    def get_version(cls, buff):
        return unpack_seg(utils.Header_Ver_f, buff)

    @classmethod
    def get_event_id(cls, buff):
        """
        return string
        """
        return unpack_seg('12s', buff)

    @classmethod
    def get_ip_size(cls, buff):
        """
        return int
        """
        return unpack_seg(utils.BODY_SIZE_f, buff)

    @classmethod
    def get_ip(cls, buff, ip_size):
        """
        return string
        """
        return buff.read(int(ip_size))

    @classmethod
    def get_string(cls, buff):
        size = unpack_seg(utils.RECORD_SIZE_f, buff)
        return unpack_seg('%s%s' % (size, utils.Str_Suf_f), buff)

    @classmethod
    def get_app(cls, buff):
        return cls.get_string(buff)

    @classmethod
    def get_name(cls, buff):
        return cls.get_string(buff)

    def is_integrated(self):
        if self['buff_endpoint'] - self['buff_startpoint'] != self['record_size']:
            raise EOFError, 'at buff offset: %s can not parse a complete record' % self['buff_startpoint']
        return True

    @classmethod
    def record_generator_chain(cls, filepaths):
        for g in chain( cls.record_generator(f) for f in filepaths ):
            for r, err in g:
                yield r,err
        
    @classmethod
    def record_generator(cls, filepath, offset=None, timestamp=None, filters=None, callbacks=None):
        """
        In:
        - filepath:
        - offset:
        - timestamp:
        - timestamp: 时间戳 unix(float)
        - filters: 搜索 record header 的过滤函数
        - callbacks: 格式化 record字段之类的回调函数
        Return:
          record, err_info
        """
        with open(filepath, 'rb') as f:
            if offset:
                f.seek(offset)
            fsize = os.stat(filepath).st_size
            if timestamp:
                end_time = int(timestamp + 10)*1000
            else:
                end_time = None
            while True:
                try:
                    record_start_point = f.tell()
                    record_header = Record.extract_header(f, end_time, filters, callbacks)
                    if record_header:
                        record = Record.unpack_body(f, record_header, callbacks)
                    else:
                        break
                    yield record, dict(err=None,filepath=filepath, offset=f.tell())
                except RecordNotMatch:
                    # 没有匹配就继续搜索
                    continue
                except OverSearchScope:
                    # 超过了每个索引的10s范围就退出
                    break
                except EOFError as e:
                    logger.error("%s, filepath: %s" % (e.message, filepath))
                    raise StopIteration
                except RuntimeError as e:
                    if fsize == f.tell():
                        # file ends
                        break
                    if fsize - f.tell() < calcsize(e.message):
                        # incomplete record pass
                        logger.error( 'incomplete record, file:%s, file_size:%s, now at:%s, to read size: %s' % (filepath,fsize, f.tell(), calcsize(e.message)))
                        break
                    yield None, dict(filepath=filepath, offset=f.tell(),
                                     start_point=record_start_point,
                                     err='Unpack error: format:%s' % e.message)
                except TypeError as e:
                    traceback.print_exc()
#                    _,_,tb = sys.exc_info()
#                    logger.error( tb )
                    yield None, dict(err='Unpack error: %s' % e.message,
                                     start_point=record_start_point,
                                     filepath=filepath, offset=f.tell())
        raise StopIteration

    @classmethod
    def get_header_keys(cls, buff, version_keys):
        """
        In:
        - buff: file or buffer object.
        - version_keys: record header里面 key的字段列表 ex. ['c_ip', 'uid', 'did', 'uri_stem']

        Out:
        {version_key: key_value, }
        """

        header_keys = dict()
        for vk in version_keys:
            if vk == 'c_ip':
                ip_size = cls.get_ip_size(buff)
                header_keys[vk] = cls.get_ip(buff, ip_size)
            elif vk in ('uid', 'did', 'page'):
                header_keys[vk] = cls.get_string(buff)

        return header_keys

    @classmethod
    def filter_header_keys(cls, header_keys, key, key_type):
        # 维度对应record header里面字段名, ipc从ip中解析
        key_type_version_key = dict(
            ip = 'c_ip',
            user = 'uid',
            did = 'did',
            page = 'page',
        )
        vk = key_type_version_key.get(key_type, None)
        if vk is None:
            return False

#        logger.debug(DEBUG_PREFIX+ '过滤record header函数中.. ; 查询的key: %s, key_type:%s, 查询的字段是:%s, 查询的字典header_keys:%s', str(key), str(key_type), vk, header_keys)

        if key_type == 'ipc':
            ipc = '.'.join(header_keys[vk].split('.')[:3])
#            logger.debug(DEBUG_PREFIX+"比较的两个值: header key:%s, type:%s, input key:%s, type: %s, 是否匹配?%s", ipc,type(ipc),str(key),type(key), ipc == key)
            if ipc == key:
                return True
        else:
#            logger.debug(DEBUG_PREFIX+"比较的两个值: header key:%s, type:%s, input key:%s, type: %s, 是否匹配?%s", header_keys[vk],type(header_keys[vk]),str(key),type(key), header_keys[vk] == key)
            if header_keys[vk] == key:
                return True
        return False

    @classmethod
    def filter_event_id(cls, header_keys, event_id):
        src = str(event_id)
        dst = header_keys['id']
        logger.debug(DEBUG_PREFIX+"查找的event_id是%s(type:%s), 查找的对象是%s(type:%s), 是否匹配? %s", src, type(src),dst, type(dst), dst == src)
        if dst == src:
            return True
        return False

    @classmethod
    def filter_query(cls, header_keys, query):
        if header_keys is None:
            return False
        filter_cols = ["page", "sid", "uid"]
        for col in filter_cols:
            v = header_keys.get(col, "")
#            logger.debug(DEBUG_PREFIX+ u"filter record: %s 从字段:%s, %s查找:%s", header_keys, col, v, query)
            if not v:
                continue
            if v.find(query) != -1:
                return True
        return False

    @classmethod
    def query_record(cls, record, query):
        if record is None:
            raise RecordNotMatch
        filter_cols = ["page", "sid", "uid"]
        for col in filter_cols:
            v = record.get(col, "")
#            logger.debug(DEBUG_PREFIX+ u"query record: %s 从字段:%s, %s查找:%s", record, col, v, query)
            if not v:
                continue
            if v.find(query) != -1:
                return record
        raise RecordNotMatch

    @classmethod
    def extract_header(cls, buff, end_time=None, filters=None, callbacks=None):
        """
        buff:
        end_time: 本索引段的结束时间, 超过的话应该结束本次搜索
        """

        record = None
        buff_startpoint = buff.tell()
        record_size = cls.get_record_size(buff)
        app_offset = cls.get_app_offset(buff)
        timestamp = cls.get_timestamp(buff)

#        logger.debug(DEBUG_PREFIX+ "搜索的时间节点:%s(type:%s), 获取的时间是%s(type:%s)",end_time, type(end_time), timestamp, type(timestamp))
        if end_time and timestamp > end_time:
            logger.debug("文件偏移量为%s 的日志的时间: %s, 设置的超时时间:%s超过了搜索的时间范围", buff_startpoint, timestamp, end_time)
            raise OverSearchScope

        # extract header keys
        ver = cls.get_version(buff)
        version_keys = utils.Header_Version.get(str(ver))
        i = str(ObjectId(cls.get_event_id(buff)))
        pi = str(ObjectId(cls.get_event_id(buff)))
#        logger.debug(DEBUG_PREFIX+"解析出来的数据: id:%s, pid:%s, timestamp:%s, record_size:%s, record_start_point:%s, 当前偏移量: %s", i, pi, timestamp, record_size, buff_startpoint, buff.tell())
#        logger.debug(DEBUG_PREFIX+ '解析头部获取的header版本相关数据: version: %s, type is %s, version_keys:%s, utils.Header_Version is %s, ', ver, type(ver), version_keys, utils.Header_Version)

        record_headers = cls.get_header_keys(buff, version_keys)
        record_headers['id'] = i
        record_headers['pid'] = pi
        # record_headers: dict type

#        logger.debug(DEBUG_PREFIX+ '解析出来的header: %s', record_headers)

        if filters:
            # 做header里面的key匹配
            if not all(f(record_headers) for f in filters):
                # 不匹配时,需要跳到下一条record开始处
#                logger.debug(DEBUG_PREFIX+ '不匹配，跳到下一条record的偏移量为:%s', buff_startpoint+record_size+1)
                # 计算填充大小
                align_bytes = 8
                align_size = record_size % align_bytes
                if align_size == 0:
                    align_size = align_bytes
                else:
                    align_size = align_bytes - align_size

                buff.seek(buff_startpoint+record_size+align_size)
                raise RecordNotMatch

        record = Record()
        record.c_ip = record_headers.get('c_ip', '')
        record.uid = record_headers.get('uid', '')
        record.did = record_headers.get('did', '')
        record.uri_stem = record_headers.get('page', '')
        record.buff_startpoint = buff_startpoint
        record.record_size = record_size
        record.id = i
        record.pid = pi
        record.app = Record.get_app(buff)
        record.name =  Record.get_name(buff)
        record.timestamp = timestamp

        if callbacks:
            # 做record字段们的格式化之类的, 或者搜索
            for func in callbacks:
                record = func(record)

        return record

    @classmethod
    def unpack_body(cls, buff, record, callbacks=None):
        """
        解析日志文件buff的record body部分, 将其装填进传入的record 对象中
        Paramters:
        - buff: file or stringio obj, 支持read方法
        - record: NamedDict字典的一个子类对象, 增加了 record.key = value
        """
        if record is None: return None

        schema = utils.name2_schema.get(record.name, None)
        if not schema:
            logger.debug(DEBUG_PREFIX+u"record start at %s", record.buff_startpoint)
            raise TypeError, "record's name: '%s' have no schema" % record.name

#        logger.debug(DEBUG_PREFIX+' ; unpack_body get schema name is %s, schema is None? %s, record is None? %s', record.name, schema is None, record is None)

        # 按照schema读取record剩余的body部分
        record._unpack_body(schema, buff)
        record.value = unpack(">q",buff.read(8))
        record.buff_endpoint = buff.tell()

#        logger.debug(DEBUG_PREFIX+ ' ; after unpack body record is %s', record)
        # 检查读取的record的完整性
        if record.is_integrated():
            # 跳过一些对齐填充8字节的数据
            align_bytes = 8
            excess_offset = align_bytes - (record.record_size % align_bytes)
            buff.read(excess_offset)

        if callbacks:
            # 做record字段们的格式化之类的, 或者搜索
            for func in callbacks:
                record = func(record)

        return record

    def _unpack_body(self, schema, buff):
#        DEBUG_PREFIX = '==============='
        if schema is None:
            raise ValueError, 'schema参数不能为空, record body必须要schema才能解析'
        for s in schema:
            k = s.get('field', None)
            t = s.get('type', None)
            if not k or not t:
#                logger.debug(DEBUG_PREFIX+'; schema %s 拿不到field或者type字段', s)
                raise ValueError, 'field is %s, type is %s, neither can be None' % (k, t)

            f = utils.type2struct.get(t, None)
            if not f:
#                logger.debug(DEBUG_PREFIX+'; type:%s 拿不到对应的pack format', t)
                raise ValueError, 'invalid type: %s for struct pack' % t

            field_size = unpack_seg(utils.BODY_SIZE_f, buff)
            if t == 'string':
                _ = '>%s%s' % (field_size, f)
                f = _
            if int(field_size) == 0:
                self[k] = None
                continue

            field_value = unpack_seg(f, buff)
#            logger.debug(DEBUG_PREFIX + ' ; unpack field:%s, value:%s', k, field_value)
            self[k] = field_value

    def unpack_record_debug(self, buff):
        """
        测试顺序解析日志中的每条record的生成器， 每次next的结果是下一个record的字段名、字段的值， 以及字段的类型, ps.打印的schema 字段名并非解析出来的
        """
        self.record_size = self.get_record_size(buff)
        yield 'record_size', self.record_size, type(self.record_size)

        self.app_offset = self.get_app_offset(buff)
        yield 'app_offset', self.app_offset, type(self.app_offset)

        self.timestamp = self.get_timestamp(buff)
        yield 'timestamp', self.timestamp, type(self.timestamp)

        self.version = self.get_version(buff)
        yield 'version', self.version, type(self.version)

        self.id = ObjectId(self.get_event_id(buff))
        yield 'id', self.id, type(self.id)

        self.pid = ObjectId(self.get_event_id(buff))
        yield 'pid', self.pid, type(self.pid)

        if self.version == 0:
            self.ip_size = self.get_ip_size(buff)
            yield 'ip_size', self.ip_size, type(self.ip_size)
            self.ip = self.get_ip(buff, self.ip_size)
            yield 'ip', self.ip, type(self.ip)
        elif self.version == 1:
            self.ip_size = self.get_ip_size(buff)
            yield 'ip_size', self.ip_size, type(self.ip_size)
            self.ip = self.get_ip(buff, self.ip_size)
            yield 'ip', self.ip, type(self.ip)
            uid = self.get_string(buff)
            yield 'header uid', uid, type(uid)
            did = self.get_string(buff)
            yield 'header did', did, type(did)
            uri_stem = self.get_string(buff)
            yield 'header uri_stem', uri_stem, type(uri_stem)

        self.app = self.get_app(buff)
        yield 'app', self.app, type(self.app)

        self.name = self.get_name(buff)
        yield 'name', self.name, type(self.name)

        schema = utils.name2_schema.get(self.name, None)
        yield 'schema', schema, type(schema)
        self._unpack_body(schema, buff)
        for s in schema:
            field = s['field']
            yield field, getattr(self, field), s['type']

        # 完整性补全
        align_size = self.record_size % 8
        if align_size == 0:
            align_size = 8
        else:
            align_size = 8 - align_size
        print 'A record end here, record_size: %s, align_size: %s' % (self.record_size, align_size)
        buff.read(align_size)


class Index(object):
    """
    持久化索引格式:
    key:  type(8bits int 维度的类型)|key(24bits int ip维度是用ipc直接用, 其他维度是mmh3.hash的后3个字节)|timestamp(16bits int 当前小时的第几个10s段)
    value:  logname-1(8bits log filename 0 - 15)|offset-1(32bits int 日志文件中的偏移量，max 2gb)|logname-2|offset-2|...

    详细说明参见 http://wiki.threathunter.home/pages/viewpage.action?pageId=3244535
    """
    byte = 8
    offset_seg_size = 5 # filename(1byte)offset(4bytes)
    offset_size = 4 * byte # offset 4 bytes
    mask = (2 ** byte) - 1 # 255

    @classmethod
    def get_filename(cls, key):
        """
        根据key(ipc段)来生成sharding文件名, 可能的值: 0-15
        跟java写的sharding生成逻辑对应

        update: 1.4 的索引值内会加上文件名, 所以该函数现在只是说明了log文件sharding的原理
        另外就是单元测试造数据的时候使用
        """
        ht = mmh3.hash(key)
        if ht < 0:
            t = (ht * -1) % 16
        else:
            t = ht % 16
        return t

    @classmethod
    def get_offsets(cls, key, key_type, db, timestamp=None, end=None):
        """
        根据key, key_type, timestamp(optional) 来从db中查询索引，返回日志文件名和对应文件的偏移
        In:
        - key: (str)维度的值
        - key_type: (str)维度, ex. ip, page, user, did ,ipc
        - db: 数据库连接对象
        - timestamp: 时间戳 unix(float)
        Return:
        [(logname1, offset1, tens), (logname2, offset2, tens), ]
        """
#        logger.debug(DEBUG_PREFIX+ '从db中查询key:%s, key_type:%s, timestamp:%s 的日志索引..', str(key), str(key_type), timestamp)
        if timestamp is None:
            td = None
        else:
            td = datetime.fromtimestamp(timestamp)
        prefix = Index.generate_key(key_type, key, td)
#        logger.debug(DEBUG_PREFIX +'索引的搜索的前缀是%s', (prefix,))
        if not end:
            # scan by the prefix
            key_offsets = scan_keys(prefix, db)
        else:
            end_prefix = Index.generate_key(key_type, key, datetime.fromtimestamp(end))
            key_offsets = scan_keys_between(prefix, end_prefix, db)
#        logger.debug(DEBUG_PREFIX +'获取的索引是 %s', key_offsets)
        # 因为使用ipc来做文件sharding的,
        ret = []
        for key, offset in key_offsets:
            _, _, tens = cls.unpack_index(key)
            parsed_offset = Index.parse_offset(offset)
            for po in parsed_offset:
                po.append(tens)
            ret.extend(parsed_offset)
        # 以索引的时间来从小到大排序索引
        ret.sort(key=lambda x:x[2])
        return ret

    @classmethod
    def unpack_index(cls, index):
        """
        解析index key，返回各部分
        return: index_type(hex), hash_key(hex), tens of seconds(int)
        """
        type_hashkey,tens = unpack('>LH', index)
        type_hashkey_hex = utils.get_byte_array(type_hashkey, 4)
        return type_hashkey_hex[0],type_hashkey_hex[1:], tens

    @classmethod
    def generate_key(cls, key_type, key, timestamp=None):
        """
        产生leveldb索引所存储的key的前缀
        key的格式 type(1byte)|key(3bytes)|tens_of_seconds(2bytes)
        Parameters:
        key_type: type of key
        key: ip维度: ipc值, user, did, page维度是mmh3.hash出来的4字节hash值的后3字节
        timestamp: datetime obj or None
        """

        if key_type == 'ip':
            key_bytes = [ utils.IP_Record_Type, ]
            key_bytes.extend([ int(_) for _ in key.split('.')[:3] ])
        elif key_type == 'ipc':
            # ip, ipc 公用的是同一个索引类型, 区别在于ip的话需要取前三个字节
            key_bytes = [ utils.IP_Record_Type, ]
            key_bytes.extend([ int(_) for _ in key.split('.') ])
        elif key_type in ('user', 'did', 'page'):
            key_bytes = [ utils.key_type2_index.get(key_type), ]
            hash_key = mmh3.hash(key)
            if hash_key < 0:
                hash_key = hash_key * -1
            # 取hash出来的4个字节的后3个字节来做key @未来 如果故意产生碰撞的方案产生的索引量不多的话，可以试试用4个字节
            for _ in xrange(3):
                key_bytes.append( hash_key & Index.mask)
                hash_key = hash_key >> 8

        if timestamp:
            timeslot = ((timestamp.minute * 60) + timestamp.second)/10
            key_bytes.extend([timeslot / 256, timeslot % 256])

        return bytearray(key_bytes)

    @classmethod
    def parse_offset(cls, index_value):
        """
        解析索引值得工具函数
        In: binary offset value str ( filename1(1byte)offset1(4bytes)filename2(1byte)offset2(4bytes)... ) )
        Out: [(filename, offset), ..]
        """
        func_prefix = 'Index.parse_offset'
        res = []

        len_value = len(index_value) / Index.offset_seg_size # 索引的值有多少个5个字节的索引段
        # 下面这个列表中每一项为 5个字节的字符串
        offset_list = [ index_value[ (_-1)*Index.offset_seg_size : _*Index.offset_seg_size ]
                        for _ in xrange(1, len_value+1)]

#        logger.debug( DEBUG_PREFIX + func_prefix+ "索引值有 %s 个索引段, 索引段们: %s", len_value, offset_list)
        for offset_seg in offset_list:
            # 头一个字节是log所在的文件名
            # 后4个字节是offset
            filename, offset = unpack('>BL', offset_seg)
            res.append([filename, offset])

        return res
        
    @classmethod
    def regen_index(cls, db_path):
        index_cache = cls.generate_index(db_path)
        cls.write_index(db_path, usettings.INDEX_PATH, index_cache)

    @classmethod
    def get_log_paths(cls, log_path):
        """
        获得log_path/log文件夹下的sharding日志文件们(默认是上个小时的日志文件夹)所在的路径
        Output:
        - db_path /path/to/persistent/2015040122/
        - logs [ /path/to/persistent/2015040122/log/0, /path/to/persistent/2015040122/log/1, ..]
        """
        if log_path:
            db_path = log_path
        else:
            db_path = utils.get_path(None, settings.DB_ROOT_PATH)
        log_path = opath.join(db_path , usettings.LOG_PATH)
        
        # 日志文件名字是0-15
        log_files = [ opath.join(log_path, str(_)) for _ in range(16) ]
        
    #    logger.debug('logs paths: %s', log_files)
    
        logs = filter(lambda x: opath.exists(x) and opath.isfile(x), log_files)
        return db_path, logs
    
    @classmethod
    def generate_index(cls, db_path):
        func_prefix = ' generate_index'
            
        _, logs_path = cls.get_log_paths(db_path)
        
    #    logger.debug('generate index from: %s, logs_path:%s', db_path, logs_path)
    
        index_cache = dict()
        # index(index's key): { filename:offset, filename1:offset1}
        record_count = 0
        for r,err in Record.record_generator_chain(logs_path):
            record_count += 1
            for col, key_type in [('c_ip','ip'),('uid', 'user'), ('did', 'did'), ('uri_stem', 'page')]:
                if r.has_key(col) and r[col]: # 临时不做空值的索引 @issue
                    try:
                        index_list = Index.generate_key(
                            key_type, r[col], datetime.fromtimestamp(r["timestamp"]/1000.0))
                    except ValueError:
                        logger.debug('key_type:%s ,key: %s, filepath:%s, timestamp:%s, offset:%s, 无法生成索引key.\n' % (key_type, r[col], err['filepath'], r.timestamp, r.buff_startpoint))
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
        
    @classmethod
    def write_index(cls, db_path, index_path, index_dict):
        """
        生成测试所引用
        """
        db = get_db(opath.join(db_path, index_path))
        for k,v in index_dict.iteritems():
            db.Put(k, v)
        del db
