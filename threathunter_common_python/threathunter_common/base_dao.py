#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time,six,json,hashlib

from sqlalchemy.sql import select
from sqlalchemy import (create_engine, MetaData, Table, Column,
                        Integer, Float, func, desc,or_)
from sqlalchemy.types import CHAR, String, Text

from .util import utf8, text
from .util import curr_timestamp,mobile_match

__author__ = "nebula"

if six.PY3:
    where_type = utf8
else:
    where_type = text
    
def connect_database(url):
    return 
    
def result2dict(columns, task):
    r = {}
    for c, t in zip(columns, task):
        if isinstance(c, six.string_types):
            r[c] = t
        else:
            r[c.name] = t
    return r    

    
        


class BaseDao(object):
    def __init__(self):
        pass
        

    def execute(self,*args,**kwargs):
        reexec_count = 0
        while 1:       
            try:
                reexec_count +=1
                return self.engine.execute(*args,**kwargs)
            except Exception,e:
                if "MySQL server has gone away" in e.message or "Can't connect to MySQL server" in e.message:
                    time.sleep(reexec_count)
                    if reexec_count > 8:
                        raise
                else:
                    raise
    def _parse(self,data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, six.binary_type):
                data[key] = text(value)
            for each in ('attrs','modules','available_hijack_type',"hijackresult","project_cols","cfg_value",'coupon_content',"product_content","weight_config","test_weight_config","bu_value"): #此处编辑需要json loads 的字段  
                if each == key:
                    if data[each]:
                        if isinstance(data[each], bytearray):
                            data[each] = str(data[each])
                        data[each] = json.loads(data[each])
                    else:
                        data[each] = {}                
        return data


    def _stringify(self,data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, six.string_types):
                data[key] = utf8(value)
            elif isinstance(value,dict):
                data[key] = json.dumps(value)
            elif isinstance(value,list):
                data[key] = json.dumps(value)                
        return data
    
    #返回所有记录的行数
    def all_records_count(self):
        return self.execute(self.table.select()
                                        .with_only_columns([func.count()]))

    def insert(self,obj={},**kwargs):
        obj = dict(obj)
        return self.execute(self.table.insert()
                                   .values(**self._stringify(obj))).inserted_primary_key[0]

    def delete(self,primary_id=None,user_id = None,):
        if primary_id:
            return self.execute(self.table.delete()
                                       .where(self.table.c.id == primary_id))

    #需要重载 默认返回所有结果
    def search(self, user_id = None, fields=None):
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []
        for task in self.execute(self.table.select()
                                        .with_only_columns(columns)):
            retval.append(self._parse(result2dict(columns, task)))
        return retval
            
    def update(self,  primary_id,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.id == primary_id)
                                   .values(**self._stringify(obj)))

    def get(self, primary_id,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.id == primary_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
        
    
    def __del__(self):
        pass
    
class ConfigDao(BaseDao):
    __table_name__ = 'config'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                           Column('id',Integer, primary_key=True),
                           Column('cfg_key',CHAR(200)) ,
                           Column('cfg_value',Text),
                           
                           )
        self.engine = create_engine(url, convert_unicode=True,pool_recycle=3600)
        self.table.create(self.engine, checkfirst=True)
        
    def get(self, key,  fields=None):
        columns = ["cfg_value"]
        for task in self.execute(self.table.select()
                                        .where(self.table.c.cfg_key == where_type(key))
                                        .limit(1)
                                            .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))["cfg_value"]
        
class HijackAttrsDao(BaseDao):
    __table_name__ = 'hijack_attrs'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                           Column('id',Integer, primary_key=True),
                           Column('user_id',Integer) ,
                           Column('available_project_nums',Integer),
                           Column('allow_download_video',Integer),
                           Column('available_hijack_type',CHAR(200)),
                           
                           )
        self.engine = create_engine(url, convert_unicode=True,pool_recycle=3600)
        self.table.create(self.engine, checkfirst=True)
        
    def get(self, user_id = None, fields=None):
        """
        没有primary_id 参数
        """
        retval = {}
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.user_id == user_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            retval = self._parse(result2dict(columns, task))
        return retval
    
    def update(self,  primary_id=None,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.user_id == user_id)
                                   .values(**self._stringify(obj)))
    
class IpRiskRepoAttrsDao(BaseDao):
    __table_name__ = 'ip_risk_repo_attrs'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                           Column('id',Integer, primary_key=True),
                           Column('user_id',Integer),
                           Column('auth',CHAR(32)),
                           Column('ip_query_used_amount',Integer),
                           Column('ip_query_total_amount',Integer),
                           Column('ip_query_expire_time',Integer),  #添加功能属性
                           Column('enable',Integer),
                           Column('allow_ontime',Integer),
                           Column('allow_batchquery',Integer),
                           Column('display_extra_cols',Integer),
                           Column('query_frequency',Integer), #查询频率
                           Column('allow_weight_config',Integer), # 是否支持权重自定义配置
                           Column('weight_config',Text), #权重配置
                           Column('test_weight_config',Text), #测试权重配置
                           Column('global_weight_config_switch',Integer), #是否采用全局配置
                           Column('test_weight_config_switch',Integer), #测试权重配置开关
                           Column('allow_write',Integer), #是否允许写入
                           Column('allow_mobile_detect',Integer), #是否允许手机实时检测
                           Column('write_source_mark',CHAR(50)), #写入数据来源定义
                           
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True)    
        
    def get(self, user_id = None, fields=None):
        """
        没有primary_id 参数
        """
        retval ={}
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.user_id == user_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            retval = self._parse(result2dict(columns, task))    

        return retval
    
    def get_via_auth(self, auth = None, fields=None):
        """
        没有primary_id 参数
        """
        retval ={}
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.auth == where_type(auth))
                                        .limit(1)
                                        .with_only_columns(columns)):
            retval = self._parse(result2dict(columns, task))      
            if retval:
                retval.pop("user_id")
                retval.pop("id")

        return retval
    
    def add_used_count_via_auth(self,auth,count):
        sql = self.table.update().where(self.table.c.auth == auth).values(ip_query_used_amount = self.table.c.ip_query_used_amount +count)
        return self.execute(sql)
    
    def update(self,  primary_id=None,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.user_id == user_id)
                                   .values(**self._stringify(obj)))
    
    def update_via_id(self,  primary_id=None,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.id == primary_id)
                                   .values(**self._stringify(obj)))    
class UserDao(BaseDao):
    __table_name__ = 'user'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                           Column('id',Integer, primary_key=True),
                           Column('name',CHAR(100)) ,
                           Column('email',CHAR(100)),
                           Column('password',CHAR(40)),
                           Column('is_super',Integer),
                           Column('attrs',Text),  #添加功能属性
                           Column('last_login',Integer), #上次登陆时间
                           Column('date_joined',Integer),   #注册时间
                           Column('is_active',Integer),#是否激活，允许登陆
                           Column('level',Integer),  #用户等级  1 测试用户 2 普通用户 3 企业用户  4 vip用户 
                           Column('consume_amount',Integer), #消费金额
                           Column('modules',CHAR(200)), #支持功能
                           Column('company',CHAR(500)), #公司名称
                           Column('telephone',CHAR(30)), #电话号码
                           Column('reg_ip',CHAR(18)), #注册IP地址
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True)             
    
    
    def get(self, primary_id,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.id == primary_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
        
    def get_via_name(self, name,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.name == where_type(name))
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
    def check_user_pwd(self,name,pwd):
        """
        :name 用户名
        :pwd 密码
        return 返回用户所有信息
        """

        for user in self.execute(self.table.select()
                            .where(self.table.c.name == where_type(name))
                            .where(self.table.c.password == where_type(pwd))
                            .limit(1) 
                            ) : 
            return self._parse(result2dict(self.table.c, user))
        else:
            return None
        
    def change_password(self,primary_id,old_pwd,new_pwd):
        """
        修改密码
        :id 用户id
        :old_pwd 旧密码
        :new_pwd 新密码
        return 返回修改状态
        """
        old_pwd = hashlib.sha1(old_pwd).hexdigest()
        for user in self.execute(self.table.select()
                            .where(self.table.c.id == primary_id)
                            .where(self.table.c.password == where_type(old_pwd))
                            .limit(1) 
                            ) : 
            new_pwd = hashlib.sha1(new_pwd).hexdigest()
            self.update(primary_id=primary_id,obj = {"password":new_pwd})
            return True
        return False
    
    def insert(self,obj={}):
        obj = dict(obj)
        obj.setdefault("attrs",{})
        obj["password"] = hashlib.sha1(obj["password"]).hexdigest()
        #obj["password"] = '75e8d2559aa21478e71392371f163d55950841a4' #初始化密码  threathunter.com
        return self.execute(self.table.insert()
                                   .values(**self._stringify(obj))).inserted_primary_key[0]
        
    def get_attrs(self,primary_id):
        """
        获取用户的可用的属性
        """
        user = self.get(primary_id, fields=["attrs"])
        if user:
            attrs = json.loads(user.get("attrs",'{}'))
        else:
            attrs = {}
        return attrs
    
    def update_via_id(self,  primary_id=None,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.id == primary_id)
                                   .values(**self._stringify(obj)))      
    
class HijackProjectDao(BaseDao):
    """
    group_id  默认组全部置为0
    """
    __table_name__ = 'hijack_project'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                           Column('id',Integer, primary_key=True),
                           Column('name',CHAR(100)),     #项目名称
                           Column('hijack_type',String(50, u'utf8_bin')),
                           Column('schedule_time',String(200, u'utf8_bin')), #计划时间周期
                           Column('enable',Integer), #是否启用
                           Column('user_id',Integer), #关联用户id
                           Column('project_cols',Text), #json 格式存储项目属性
                           Column('muscles_corr_id',CHAR(200)),
                           Column('add_time',Integer), #添加时间
                           Column('group_id',Integer), #fk hijack_project_group 组ID
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True)         
        
    def auth_project(self,user_id,project_id):
        """
        检查project所属用户
        """
        for user in self.execute(self.table.select()
                            .where(self.table.c.id == project_id)
                            .where(self.table.c.user_id == user_id)
                            .limit(1) 
                            ) : 
            return True
        else:
            return False
        
    def update(self,  primary_id,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.id == primary_id)
                                          .where(self.table.c.user_id == user_id)
                                   .values(**self._stringify(obj))).rowcount

    def get(self, primary_id,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.id == primary_id)
                                        .where(self.table.c.user_id == user_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
        
    def get_via_name(self, name,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.name == name)
                                        .where(self.table.c.user_id == user_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
        
    def delete(self,primary_id=None,user_id = None,):
        if primary_id:
            return self.execute(self.table.delete()
                                       .where(self.table.c.id == primary_id)
                                       .where(self.table.c.user_id == user_id)
                                       )
        

    def search(self, user_id = None, fields=None,**kwargs):
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []
        if kwargs.get('hijack_project_group',None) ==0 or kwargs.get('hijack_project_group',None) :
            for task in self.execute(self.table.select()
                                            .where(self.table.c.user_id == user_id)
                                            .where(self.table.c.group_id == int(kwargs.get('hijack_project_group',None)))
                                            .with_only_columns(columns).order_by(self.table.c.add_time.desc())):
                retval.append(self._parse(result2dict(columns, task)))
        else:
            for task in self.execute(self.table.select()
                                            .where(self.table.c.user_id == user_id)
                                            .with_only_columns(columns).order_by(self.table.c.add_time.desc())):
                retval.append(self._parse(result2dict(columns, task)))                
        return retval
    
    def user_exist_project_count(self,user_id):
        """
        用户所创建的project数量
        """
        
        for item in  self.execute(self.table.select()
                                   .where(self.table.c.user_id == user_id)
                                        .with_only_columns([func.count()])):
            return item[0]
        

    def insert(self,user_id=None,obj={}):
        obj = dict(obj)
        obj["user_id"] = user_id
        return self.execute(self.table.insert()
                                   .values(**self._stringify(obj))).inserted_primary_key[0]
class ProxyDao(BaseDao):
    __table_name__ = 'proxy'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('proxyip',String(255), unique=True),
                                          Column('proxyport',Integer),
                                          Column('type',Integer),
                                          Column('country',String(255,u'utf8_bin')),
                                          Column('city',String(255,u'utf8_bin')),
                                          Column('area',String(255,u'utf8_bin')),
                                          Column('province',String(255)),
                                          Column('isp',String(255)),
                                          Column('time',Integer),
                                          Column('proxy_type',String(50)),
                                          Column('anonymity',Integer),
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True)          

    def get_random_proxyip(self):
            columns = ["proxyip","proxyport"]
            for task in self.engine.execute("select proxyip,proxyport from proxy where type =1 order by rand() limit 1"):
                return self._parse(result2dict(columns, task))
            else:
                return None  
    
    def available_worknode(self,**kwargs):
        """
        当前可用检测节点数量
        """
        for item in  self.execute(self.table.select()
                                   .where(self.table.c.type == 1)
                                        .with_only_columns([func.count()])):
            return item[0]
        
    
    def search(self,fields=None,**kwargs):
        """
        @param available 是否提取有效的proxyip
        @return list
        """

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []
        if kwargs.get('available',False):
            for task in self.execute(self.table.select()
                                        .where(self.table.c.type == 1)
                                        .with_only_columns(columns)):
                retval.append(self._parse(result2dict(columns, task)))
        else:
            for task in self.execute(self.table.select()
                                        .with_only_columns(columns)):
                retval.append(self._parse(result2dict(columns, task)))
        return retval
    
    
class HijacklogDao(BaseDao):
    __table_name__ = 'hijack_log'
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('user_id',Integer),
                                          Column('hijack_type',String(50)),
                                          Column('proxyip',String(15)),
                                          Column('proxyport',Integer),
                                          Column('hijackresult',Text),
                                          Column('country',String(200,u'utf8_bin')),
                                          Column('area',String(200,u'utf8_bin')),
                                          Column('province',String(200,u'utf8_bin')),
                                          Column('city',String(200,u'utf8_bin')),
                                          Column('isp',String(100)),
                                          Column('project_id',Integer),
                                          Column('task_id',String(32)),
                                          Column('time',Integer)  
                           )    
        self.engine = create_engine(url,convert_unicode=True)
        self.table.create(self.engine, checkfirst=True)    
        
        
    def delete(self,primary_id=None,user_id = None,):
        if primary_id:
            return self.execute(self.table.delete()
                                       .where(self.table.c.user_id == user_id)
                                       .where(self.table.c.id == primary_id))
    def insert(self,user_id,obj={}):
        obj = dict(obj)
        obj["time"] = curr_timestamp() #增加时间戳
        return self.execute(self.table.insert()
                                   .values(**self._stringify(obj)))
    
    def get(self, primary_id,user_id = None, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.user_id == user_id)
                                        .where(self.table.c.id == primary_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))
        

    def search(self, user_id = None, fields=None,**kwargs):
        """ 
        @return list
        """
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []        
        hijack_project_ids = kwargs.get('hijack_project_ids',None)
        starttime = int(kwargs.get('starttime',0))
        endtime = int(kwargs.get('endtime',0))
        city_or_province = kwargs.get('city_or_province',None)
        q = self.table.select().where(self.table.c.user_id == user_id).order_by(self.table.c.time.desc()).with_only_columns(columns)
        if starttime and endtime:
            q = q.where(self.table.c.time >= starttime).where(self.table.c.time <= endtime)
        if hijack_project_ids != None:
            q = q.where(self.table.c.project_id.in_(hijack_project_ids))
        
        if city_or_province:
            city_or_province = "%"+city_or_province+"%"
            q = q.where(or_(self.table.c.city.like(city_or_province),self.table.c.province.like(city_or_province)))
        for task in self.execute(q):
            retval.append(self._parse(result2dict(columns, task)))                    
        return retval
    
    def index_redirect_hijack_url_stat(self, user_id = None,**kwargs):
        """
        @return list
        """
        res = []
        retval = []
        columns = ["hijackresult"]
        for task in self.execute("select hijackresult from hijack_log where user_id = %s and hijack_type = 'index_redirect_hijack' "%(user_id,)):
            res.append(self._parse(result2dict(columns, task)))
        tmp = {}
        for item in res:
            hijack_url = item['hijackresult']["hijack_url"]
            if hijack_url:
                if tmp.has_key(hijack_url):
                    tmp[hijack_url] +=1
                else:
                    tmp[hijack_url] = 1
        for k,v in tmp.iteritems():
            retval.append({"name":k,"value":v})
        return retval
    
    def hijack_total_count(self, user_id = None, fields=None,**kwargs):
        """
        @return length int
        """
        starttime = int(kwargs.get('starttime',0))
        endtime = int(kwargs.get('endtime',0))   
        q = select([func.count(self.table.c.id).label('value')]).where(self.table.c.user_id == user_id)
        if starttime and endtime:
            q = q.where(self.table.c.time >= starttime).where(self.table.c.time <= endtime)
        for task in self.execute(q):
            return task["value"]
    def hijack_type_proportion(self,user_id=None,**kwargs):
        """
        劫持类型占比
        """
        #for item in  self.execute("select * fro"):
        retval = []
        hijack_project_ids = kwargs.get('hijack_project_ids',[])
        starttime = int(kwargs.get('starttime',0))
        endtime = int(kwargs.get('endtime',0))   
        q = select([self.table.c.hijack_type.label('name'),func.count(self.table.c.hijack_type).label('value')]).where(self.table.c.user_id == user_id).group_by('name')
        if hijack_project_ids:
            q = q.where(self.table.c.project_id.in_(hijack_project_ids))
        if starttime and endtime:
            q = q.where(self.table.c.time >= starttime).where(self.table.c.time <= endtime)
        for item in  self.execute(q):
            retval.append( self._parse(result2dict(["name","value"], item)))
        return retval
        #if hijack_project_ids:
            #hijack_project_ids = ",".join([str(x) for x in hijack_project_ids])
            #for item in  self.execute("select hijack_type as name,count(hijack_type) as value from hijack_log  where user_id = %s and project_id in (%s) group by hijack_type"%(user_id,hijack_project_ids)):
                #retval.append( self._parse(result2dict(["name","value"], item)))
        #else:
            #for item in  self.execute("select hijack_type as name,count(hijack_type) as value from hijack_log  where user_id = %s group by hijack_type"%user_id):
                #retval.append( self._parse(result2dict(["name","value"], item)))
        #return retval
    
    def hijack_count_per_project_proportion(self,user_id=None,**kwargs):
        """
        监控任务劫持数量占比
        """
        retval = []
        hijack_project_ids = kwargs.get('hijack_project_ids',[])
        starttime = int(kwargs.get('starttime',0))
        endtime = int(kwargs.get('endtime',0))    
        
        if hijack_project_ids: 
            hijack_project_ids = ",".join([str(x) for x in hijack_project_ids])
            if starttime and endtime:
                for item in  self.execute( "select b.name as name,count(*) as value from hijack_log a , hijack_project b where a.project_id = b.id and a.user_id = %s and a.project_id in (%s) and a.time > %s and a.time < %s group by a.project_id"%(user_id,hijack_project_ids,starttime,endtime)):
                    retval.append( self._parse(result2dict(["name","value"], item)))                
            else:
                for item in  self.execute( "select b.name as name,count(*) as value from hijack_log a , hijack_project b where a.project_id = b.id and a.user_id = %s and a.project_id in (%s) group by a.project_id"%(user_id,hijack_project_ids)):
                    retval.append( self._parse(result2dict(["name","value"], item)))
            
        else:
            if starttime and endtime:
                for item in  self.execute( "select b.name as name,count(*) as value from hijack_log a , hijack_project b where a.project_id = b.id and a.user_id = %s and a.time > %s and a.time < %s group by a.project_id"%(user_id,starttime,endtime)):
                    retval.append( self._parse(result2dict(["name","value"], item)))
            else:
                for item in  self.execute( "select b.name as name,count(*) as value from hijack_log a , hijack_project b where a.project_id = b.id and a.user_id = %s group by a.project_id"%user_id):
                    retval.append( self._parse(result2dict(["name","value"], item)))
        return retval
    
    def hijack_province_stat(self,user_id=None,limit=10):
        retval = []
        for item in  self.execute( "select province as name,count(province) as value from hijack_log where user_id = %s and province != '' group by province order by value desc limit %s"%(user_id,int(limit))):
            retval.append( self._parse(result2dict(["name","value"], item)))
        return retval
    
    

        
    def hijack_city_stat(self,user_id=None,limit=10,**kwargs):
        retval = []
        hijack_project_ids = kwargs.get('hijack_project_ids',[])
        starttime = int(kwargs.get('starttime',0))
        endtime = int(kwargs.get('endtime',0))  
        
        q = select([self.table.c.city.label('name'),func.count(self.table.c.city).label('value')]).where(self.table.c.city != "").where(self.table.c.user_id == user_id).group_by('name').order_by(desc('value')).limit(int(limit))
        if hijack_project_ids:
            q = q.where(self.table.c.project_id.in_(hijack_project_ids))
        if starttime and endtime:
            q = q.where(self.table.c.time >= starttime).where(self.table.c.time <= endtime)
        for item in  self.execute(q):
            retval.append( self._parse(result2dict(["name","value"], item)))
        return retval
    
    def hijack_count_log(self,user_id,starttime,endtime):
        """
        @param starttime 开始时间 格式为
        @param endtime
        """
        retval =[]
        columns = ["hijack_type","time"]
        for task in self.execute(self.table.select()
                                    .where(self.table.c.user_id == user_id)
                                    .where(self.table.c.time >= starttime)
                                    .where(self.table.c.time <= endtime)
                                    .with_only_columns(columns)):
            retval.append(self._parse(result2dict(columns, task)))
        return retval
    

class CouponCodeDao(BaseDao):
    __table_name__ = 'coupon_code'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('code',Integer, unique=True),
                                          Column('coupon_content',String(1000)), #优惠码的json内容
                                          Column('price',Integer), #优惠码价格，用于消费记录的展示
                                          Column('details',String(5000)),  #优惠码详情，用于消费记录的展示
                                          Column('amount',Integer), #可用次数 -1 代表无限次数
                                          Column('expire_time',Integer), #过期时间 -1 代表无限时间
                                          Column('enable',Integer), #验证码是否启用 1 启用 0 不启用
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
        
    def get(self, code, fields=None):

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.code == where_type(code))
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))        
        
    def use(self, code):
        """
        与get不同的地方就是该方法在调用后会检查验证码是否失效 是否使用次数不足，如果不足返回空
        """
        
        columns = self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.code == where_type(code))
                                        .where(self.table.c.enable == 1)
                                        .limit(1)
                                        .with_only_columns(columns)):
            coupon_code_info = self._parse(result2dict(columns, task))
            curr_time = curr_timestamp()
            if (curr_time < coupon_code_info["expire_time"] or coupon_code_info["expire_time"] == -1)\
               and (coupon_code_info["amount"] > 0 or coupon_code_info["amount"] == -1 ):
                curr_amount = coupon_code_info["amount"] - 1
                self.execute(self.table.update()
                                    .where(self.table.c.id == coupon_code_info["id"])
                                    .values(**self._stringify({"amount":curr_amount}))).rowcount                
                return coupon_code_info
        return {}
        
class TradingRecordDao(BaseDao):
    __table_name__ = 'trading_record'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('order_id',String(40)), #交易号
                                          Column('order_type',String(32)), #交易方式 alipay,coupon_code
                                          Column('amount',Integer), #交易价格
                                          Column('prod_id',Integer),  #购买产品ID
                                          Column('prod_name',String(1000)), #购买产品名称
                                          Column('time',Integer), #交易时间
                                          Column('status',Integer), # 0 待支付 1 成功支付 2 支付失败
                                          Column('alipay_trade_no',String(70)), #支付宝交易号
                                          Column('user_id',Integer), #支付宝交易号
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
        
    def get(self, order_id, fields=None):
        """
        根据订单号查找订单
        @param order_id 32位订单号
        """

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.order_id == order_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))   
        
    def update(self,  order_id,user_id = None, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.order_id == order_id)
                                   .values(**self._stringify(obj))).rowcount
    
    def search(self, user_id = None, fields=None,**kwargs):
        """ 
        @return list
        """

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []
        for task in self.execute(self.table.select()
                                        .where(self.table.c.user_id == user_id)
                                        .order_by(self.table.c.time.desc())
                                        .with_only_columns(columns)):
            retval.append(self._parse(result2dict(columns, task)))
        return retval    
    
class ProductDao(BaseDao):
    __table_name__ = 'product'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('name',String(1000)), #产品名称
                                          Column('details',String(32)), #产品详情
                                          Column('amount',Integer), #产品价格
                                          Column('product_content',String(2000)), #产品详情
  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
        
    def get(self, primary_id, fields=None):
        """
        """

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.id == primary_id)
                                        .limit(1)
                                        .with_only_columns(columns)):
            return self._parse(result2dict(columns, task))       
        
class AnalyserDao(BaseDao):
    __table_name__ = 'analyser'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('ip',String(32)), #ip地址
                                          Column('account',String(300)), #账号
                                          Column('mobile',String(11)), #手机号码
                                          Column('time',Integer), #插入时间
                                          Column('auth',String(32)), #访问auth
                                          Column('src_ip',String(32)), #访问来源IP
  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
        
class SessionDao(BaseDao):
    __table_name__ = 'session'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('token',String(32)), #ip地址
                                          Column('captcha_code',String(10)), #账号
  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
        
    def verify_captcha_code(self, token,captcha_code, fields=None):
        """
        验证码的验证，验证之后删除该验证码
        """

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.execute(self.table.select()
                                        .where(self.table.c.token == where_type(token))
                                        .limit(1)
                                        .with_only_columns(columns)):
            if captcha_code.upper() == task["captcha_code"].upper():
                self.execute(self.table.delete()
                                           .where(self.table.c.token == where_type(token)))            
                return self._parse(result2dict(columns, task))  
        
    def insert_prod(self):
        prod1 = {""}

class HijackProjectGroupDao(BaseDao):
    """
    项目分组
    """
    __table_name__ = 'hijack_project_group'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('name',String(100)), #组名称
                                          Column('comment',String(1000)), #组备注
                                          Column('time',Integer), #插入时间
                                          Column('user_id',Integer) #组所有者

  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
    def search(self, user_id = None, fields=None):
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        retval = []
        for task in self.execute(self.table.select()
                                        .where(self.table.c.user_id == user_id)
                                        .with_only_columns(columns)):
            retval.append(self._parse(result2dict(columns, task)))
        return retval
    
class BuDataDao(BaseDao):
    """
    业务数据
    """
    __table_name__ = 'bu_data'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('bu_key',String(100)), #key
                                          Column('bu_value',String(1000)), #value
                                          Column('timestamp',Integer), #插入更新时间

  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
    def get(self, key, is_last = True):
        """
        @param is_last : 是否取最后一次更新的key value 为 True：只返回 bu_value 字典  False  返回 bu_value timstamp 的字典列表
        """
        
        if is_last:
            retval={}
            columns = ["bu_value"]
            q = self.table.select().where(self.table.c.bu_key == where_type(key)).order_by(self.table.c.timestamp.desc()).limit(1).with_only_columns(columns)
            for task in self.execute(q):
                retval = self._parse(result2dict(columns, task))["bu_value"]

        else:
            retval = []
            columns = ["bu_value","timestamp"]
            q = self.table.select().where(self.table.c.bu_key == where_type(key)).order_by(self.table.c.timestamp.desc()).limit(1).with_only_columns(columns)
            for task in self.execute(q):
                retval.append(self._parse(result2dict(columns, task)))
        return retval
            
                
class ModemDataDao(BaseDao):
    """
    猫池手机号码存储
    """
    __table_name__ = 'modem_data'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('mobile',String(11),primary_key=True), #手机号码
                                          Column('detect_count',Integer), #检测次数
                                          Column('detect_status',Integer),#检测状态  1 待检测 2 检测中
                                          Column('detect_result',Integer),#检测状态 0 不可用 1 可用  2 未知  3 未检测 4 检测完成
                                          Column('day',Integer), #白天是否检测过 0 未检测 1 检测
                                          Column('night',Integer), #夜晚是否检测过 0 未检测 1 检测
                                          Column('timestamp',Integer), #最后一次更新时间戳
                                           
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
    def select_mobiles(self,day=None,night=None,limit=None,is_order_by_timestamp=True,delay_starttime = None,delay_endtime=None,detect_count = None,auto_delete=False,detect_status=None,reset_detect_status_to_open=False,detect_result=None):
        retval = []
        q = self.table.select().with_only_columns(['mobile'])
        if limit != None:
            q =  q.limit(limit)
        if detect_result !=None:
            q = q.where(self.table.c.detect_result.in_(detect_result))
        if is_order_by_timestamp:
            q = q.order_by(self.table.c.timestamp)
        if delay_endtime != None :
            q = q.where(self.table.c.timestamp <= delay_endtime)
        if delay_starttime != None :
            curr_time  = curr_timestamp()
            q = q.where(self.table.c.timestamp >= delay_starttime)  
        
        if detect_status != None:
            q = q.where(self.table.c.detect_status == detect_status)
        if day != None :
            q = q.where(self.table.c.day == day)
        if night != None :
            q = q.where(self.table.c.night == night)
        if detect_count != None :
            q = q.where(self.table.c.detect_count == detect_count)    
        for task in self.execute(q):
            if auto_delete:
                self.execute(self.table.delete()
                                    .where(self.table.c.mobile == where_type(task["mobile"]))) 
            if reset_detect_status_to_open:
                self.update(task["mobile"],obj={"detect_status":2})
            retval.append(self._parse(result2dict(["mobile"], task)))
        return retval
            
    def insert_mobile(self,mobile,day=0,night=0,timestamp=None,detect_count=0,detect_status=1,detect_result=3):
        """
        插入待检测号码，待检测号码如果跟库中号码重复不进行替换
        """
        if timestamp == None:
            timestamp = curr_timestamp()   
        self.execute('insert ignore into modem_data (mobile,detect_count,day,night,timestamp,detect_status,detect_result)values("%s",%s,%s,%s,%s,%s,%s) '%(mobile,detect_count,day,night,timestamp,detect_status,detect_result))
        
    def insert_mobile_bulk(self,mobiles,day=0,night=0,timestamp=None,detect_count=0,detect_status=1,detect_result=3):
        if timestamp == None:
            timestamp = curr_timestamp()        
        bulk_list = []
        if len(mobiles) > 5000:
            raise Exception,"mobiles length > 5000"
        for mobile in mobiles:
            if mobile_match(mobile):
                bulk_list.append('("%s",%s,%s,%s,%s,%s,%s)'%(mobile,detect_count,day,night,timestamp,detect_status,detect_result))
        bulk_q = ','.join(bulk_list)
        self.execute('insert ignore into modem_data (mobile,detect_count,day,night,timestamp,detect_status,detect_result)values '+bulk_q)
    def update(self, mobile, obj={},detect_count_incr=False):
        if detect_count_incr:
            obj["detect_count"] = self.table.c.detect_count+1
        return self.execute(self.table.update()
                                   .where(self.table.c.mobile == mobile)
                                   .values(**self._stringify(obj))) 
    
    def delete_expired_mobiles(self,expired_time):
        """
        删除检测时间早于指定时间并且已经入库的手机号码
        """
        return self.execute(self.table.delete()
                                   .where(self.table.c.timestamp <= expired_time)
                                   .where(self.table.c.detect_result == 4))
    
    def reset_all_mobile_detect_status_to_close(self):
        q = self.table.select().with_only_columns(['mobile']).where(self.table.c.detect_status==2)
        for task in self.execute(q):
            self.update(task["mobile"],obj={"detect_status":1,"detect_result":3})
        
    def select_complete_detect_mobiles(self):
        """
        筛选出可以入库的手机号码，并置detect_result = 4 完成检测状态
        """
        retval = []
        
        #临时 只检测一次
        q = self.execute('select mobile,detect_result from  modem_data where (detect_result=1 or detect_result=2 or detect_result=0) and detect_status = 1')
        
        #检测两次 可用检测1次  不可用或者未知检测2次 未在检测队列中
        #q = self.execute('select mobile,detect_result from  modem_data where (detect_result=1 or ((detect_result=2 or detect_result=0) and detect_count=2)) and detect_status = 1')
        for task in q:
            self.update(task["mobile"],obj={"detect_result":4})
            retval.append(self._parse(result2dict(["mobile",'detect_result'], task)))
        return retval
    
class ModemSimCardDao(BaseDao):
    """
    sim卡数据
    """
    __table_name__ = 'modem_sim_card'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('phone_number',String(11)), #手机号码
                                          Column('imsi',String(15)), #imsi
                                          Column('isp',String(10)), #服务提供商
                                          Column('iccid',String(20)), #SIM卡背面唯一标识
                                          Column('comment',String(1000)), #备注 手机卡业务信息
                                          Column('purchase_timestamp',Integer), #购买时间
                                          Column('balance',Float), #手机余额
                                          Column('area',String(50)), #号码所在地
                                          
  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
        
    def get_mobile_via_imsi(self,imsi):
        q = self.table.select().with_only_columns(['phone_number']).where(self.table.c.imsi==imsi)
        for task in self.execute(q):
            return task["phone_number"]
        else:
            return ""
        
    def update(self,  mobile, obj={}, **kwargs):

        return self.execute(self.table.update()
                                   .where(self.table.c.phone_number == mobile)
                                   .values(**self._stringify(obj)))
        
class ModemSmsDao(BaseDao):
    """
    短信库
    """
    __table_name__ = 'modem_sms'
    
    def __init__(self,url):
        self.table = Table(self.__table_name__, MetaData(),
                                          Column('id',Integer, primary_key=True),
                                          Column('phone_number',String(11)), #手机号码
                                          Column('message',Text), #短信内容
                                          Column('timestamp',Integer), #接收时间
                                          Column('send_number',String(20)), #手机号码
  
                           )
        self.engine = create_engine(url, convert_unicode=True)
        self.table.create(self.engine, checkfirst=True) 
