#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import datetime,json,datetime,time,re,ipcalc,six

from .SSDB import SSDB
from .util import curr_timestamp
from .ip_opt import Ip

__author__ = "nebula"

#share
class IpRiskRepoDAO:
    def __init__(self,ssdb_port,ssdb_host):
        self.ssdb = SSDB(host=ssdb_host, port=ssdb_port)
        
        
    def get_ip_section(self,ip):
        ip_section_info = {}
        try:
            org_ip = Ip.ip2int(ip) if isinstance(ip, six.string_types) else ip
            ip = org_ip*100+99
            ret = self.ssdb.request('zrscan',['ip_section','',ip,0,1]).data
            if ret["index"]:
                ip_section_key = ret["index"][0]
                ip_net_with_mask = ret['items'][ip_section_key]
                ip_section = ip_net_with_mask/100
                mask = int(str(ip_net_with_mask)[-2:])
                cin = ipcalc.Network(ip_section,mask=mask)
                if cin.in_network(org_ip):
                    info = self.ssdb.request('get', [ip_section_key]).data
                    if info:
                        ip_section_info = json.loads(info)
        except Exception,e:
            pass
        return ip_section_info
    
    def update_ip_section(self,ip_section,mask,values):
        """
        @param ip_section:ip地址 1.1.1.1
        @param mask: cidr  24 16 8 ...
        """
        try:
            if values:
                ip_section = Ip.ip2int(ip_section)
                mask = int(mask)
                net = "t%s"%(str(ip_section*100+mask))
                
                values["mask"] = mask
                content = self.ssdb.request('get', [net]).data
                if content:
                    new_values = json.loads(content)
                    new_values.update(values)
                    self.ssdb.request('set',[net,json.dumps(new_values)])
                    return True
                else:
                    values["ip_section"] = ip_section
                    
                    self._insert_ip_section(values)
                    return True
        except Exception,e:
            print e
        return None
    
    def _insert_ip_section(self,values):
        raw_values = {
            "is_vpn":None,
            "is_proxy":None,
            "is_crawler":None,
            "is_brute_force":None,
            "is_web_server":None,
            "is_mail_server":None,
            "is_dns":None,
            "is_org":None,
            "is_seo":None,
            "server_type":"",
            "seo_type":"",
            "city":"",
            "province":"",
            "isp":"",
            "country":"",
            "area":"",
            "address":"",
            "info":"",
            "remark":"",
            

        }
        values["insert_time"] = int(time.time())
        ip_section = values["ip_section"]
        mask = values["mask"]
        ip_section_score = ip_section*100+mask
        ip_section_key = "t%s"%(ip_section_score)
        values = self.values_prepare(values)
        raw_values.update(values)
        self.ssdb.request('set',[ip_section_key,json.dumps(raw_values)])     
        self.ssdb.request('zset',['ip_section',ip_section_key,ip_section_score])
    
    # 查询可用的proxyip
    def get_ip(self,ip,update_query_time=False,ip_section_search = False):
        """
        @param update_query_time 是否进行检查时间戳更新
        @param ip_section_search 是否搜索段信息
        """
        ip_info = {}
        try:
            ip = Ip.ip2int(ip)
            ret = self.ssdb.request('get', [ip]).data
            if ret:
                if update_query_time:
                    values ={"query_time":int(time.time())}
                    new_values = json.loads(ret)
                    new_values.update(values)                    
                    self.ssdb.request('set',[ip,json.dumps(new_values)])
                ip_info = json.loads(ret)
                ip_section_info = self.get_ip_section(ip)
                if ip_section_search:
                    for k,v in ip_section_info.iteritems():
                        if v and k not in ['insert_time','mask']:
                            ip_info[k] = v
            else:
                if ip_section_search:
                    ip_section_info = self.get_ip_section(ip)
                    for k,v in ip_section_info.iteritems():
                        if v and k not in ['insert_time','mask']:
                            ip_info[k] = v                
        except Exception,e:
            print e
        return ip_info
    
    def verify_mobile(self,mobile):
        mobile_exp  = re.compile("^0?(13[0-9]|15[012356789]|17[678]|18[0-9]|14[57])[0-9]{8}$")
        mobile = str(mobile) if isinstance(mobile,int) else mobile
        if mobile_exp.match(mobile):
            return 'i'+mobile 
        else:
            return None
        
    def get_mobile(self,mobile,update_query_time=False):
        mobile = self.verify_mobile(mobile)
        if mobile:
            ret = self.ssdb.request('get', [mobile]).data
            if ret:
                if update_query_time:
                    values ={"query_time":int(time.time())}
                    new_values = json.loads(ret)
                    new_values.update(values)                    
                    self.ssdb.request('set',[mobile,json.dumps(new_values)])                
                return json.loads(ret)
        return None
    
    def insert_mobile(self,values):
        try:
            ret = 0
            mobile = values.get('mobile',None)
            if mobile:
                ret = self.update_mobile(mobile,values)
            return ret
        except Exception,e:
            print e
        return ret
    
    def update_mobile(self,mobile,values):
        try:
            if values:
                mobile = self.verify_mobile(mobile)
                if mobile:
                    content = self.ssdb.request('get', [mobile]).data
                    if content:
                        values = self.values_prepare(values)
                        new_values = json.loads(content)
                        new_values.update(values)
                        new_values["update_time"] = curr_timestamp()
                        self.ssdb.request('set',[mobile,json.dumps(new_values)])
                        return 1
                    else:
                        values["mobile"] = mobile
                        self._insert_mobile(values)
                        return 2
        except Exception,e:
            print e
            return 0 
    
    def _insert_mobile(self,values):
        raw_values = {
            "is_notreal":None, #非真实用户
            "is_fraud":None,
            "is_black_marked":None,#是否被用户标记为黑名单
            "source_mark":"",  #数据池来源  f02 阿里小号等
            "is_crank_call":None #是否为骚扰电话

        }
        values["insert_time"] = int(time.time())
        mobile = values["mobile"]
        values = self.values_prepare(values)
        raw_values.update(values)
        self.ssdb.request('set',[mobile,json.dumps(raw_values)])    
        

    def update(self,ip,values):
        try:
            if values:
                ip_ = Ip.ip2int(ip)
                content = self.ssdb.request('get', [ip_]).data
                if content:
                    values = self.values_prepare(values)
                    new_values = json.loads(content)
                    new_values.update(values)
                    new_values["update_time"] = curr_timestamp()
                    self.ssdb.request('set',[ip_,json.dumps(new_values)])
                    return 1
                else:
                    values["ip"] = ip
                    self._insert(values)
                    return 2
        except Exception,e:
            print e
        return 0
        
    def insert(self,values):
        try:
            ret = 0
            ip = values.get("ip",None)
            if ip:
                ret = self.update(ip,values)
                return ret
        except Exception,e:
            print e
        return ret
    
    def _insert(self,values):
        raw_values = {
            "is_vpn":None,
            "is_proxy":None,
            "is_crawler":None,
            "is_brute_force":None,
            "is_web_server":None,
            "is_mail_server":None,
            "is_dns":None,
            "is_org":None,
            "is_seo":None,
            "is_black_marked":None,#是否被用户标记为黑名单
            "server_type":"",
            "seo_type":"",
            "city":"",
            "province":"",
            "isp":"",
            "country":"",
            "area":"",
            "address":"", #地址
            "info":"", #公司 组织出口等信息
            "remark":"",#备注
            "source_mark":"",#本条数据来源
            

        }
        values["insert_time"] = int(time.time())
        ip = values["ip"]
        ip = Ip.ip2int(ip)
        values = self.values_prepare(values)
        raw_values.update(values)
        self.ssdb.request('set',[ip,json.dumps(raw_values)])
    
    def values_prepare(self,values):
        """
        数据预处理
        """
        if values.has_key("ip"):
            values.pop("ip")
        if values.has_key("mobile"):
            values.pop("mobile")
        if values.has_key("ip_section"):
            values.pop("ip_section")            
        if values.has_key("check_time") and isinstance(values["check_time"],datetime.datetime):
            values["check_time"] = int(time.mktime(values["check_time"].timetuple()))
        if values.has_key("insert_time") and isinstance(values["insert_time"],datetime.datetime):
            values["insert_time"] = int(time.mktime(values["insert_time"].timetuple()))
            
        
        return values
