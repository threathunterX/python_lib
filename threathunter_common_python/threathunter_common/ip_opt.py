#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "nebula"

import urllib2,os
import json,struct,socket
import re
if os.name != "nt":
    import fcntl
    import struct


class Ip:
    """
    ip相关操作库
    """

    @staticmethod
    def iplocationtb(ip):
        '''
        Return ip info dict.keys:country,city,area,province,isp
        '''
        url = "http://ip.taobao.com/service/getIpInfo.php?ip="
        urlsearch = '%s%s'%(url,ip)
        try:
            resulttb = urllib2.urlopen(urlsearch).read()
        except Exception,e:
            return {}
        resulttb = json.loads(resulttb)
        resulttmp = {}
        if resulttb["code"] == 0:
            if resulttb["data"].has_key("country"):
                resulttmp["country"] = resulttb["data"]["country"]
                if resulttb["data"].has_key("city"):
                    resulttmp["city"] = resulttb["data"]["city"]
                else:
                    resulttmp["city"] = ""
                if resulttb["data"].has_key("area"):
                    resulttmp["area"] = resulttb["data"]["area"]
                else:
                    resulttmp["area"] = ""
                if resulttb["data"].has_key("region"):
                    resulttmp["province"] = resulttb["data"]["region"]
                else:
                    resulttmp["province"] = ""
                if resulttb["data"].has_key("isp"):
                    resulttmp["isp"] = resulttb["data"]["isp"]
                else:
                    resulttmp["isp"] = ""
        return resulttmp

    @staticmethod
    def iplocationgeo(ips):

        ips = ips
        reader = geoip2.database.Reader('./GeoLite2-City.mmdb')
        result = []
        for ip in ips:
            resulttmp = {}
            resulttmp["ip"] = ip["ip"]
            resulttmp["port"] = ip["port"]
            resulttmp["type"] = ip["type"]
            response = reader.city(ip["ip"])
            if response.country.names.has_key("zh-CN"):
                country = response.country.names['zh-CN'].encode('utf8')
                if response.city.names.has_key("zh-CN"):
                    city = response.city.names['zh-CN'].encode('utf8')
                else:
                    city = None
            resulttmp["country"] = country
            resulttmp["city"] = city
            result.append(resulttmp)
        return result

    @staticmethod
    def detectproxy(ip,port,timeout=15):
        """
        代理网站的健康度检测
        连通性测试 api.threathunter.com
        非篡改测试
            www.qq.com
            www.baidu.com
            www.sohu.com
            www.163.com
            www.weibo.com
            cntv.cn
            360.cn
            sogou.com

        """
        proxydict = {}
        proxydict['http'] = "http://%s:%s"%(ip,port)
        proxy_handler = urllib2.ProxyHandler(proxydict)
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        try:

            response = opener.open('http://api.threathunter.com/checkvip/proxyip_verify',timeout=timeout)
            content = response.read()
            if not content == '<!DOCTYPE html>\n<html lang="zh-CN">\n<head>\n<meta charset="utf-8">\n<meta http-equiv="X-UA-Compatible" content="IE=edge">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>default</title>\n</head>\n<body>\n<h1>ok</h1>\n</body>\n</html>':
                return False

            #try:
                #response = opener.open('http://sogou.com',timeout=timeout)
                #content = response.read()
                #if response.getcode() == 200 and 40000 < len(content) < 70000 and "<title>\xe6\x90\x9c\xe7\x8b\x97\xe6\x90\x9c\xe7\xb4\xa2\xe5\xbc\x95\xe6\x93\x8e" in content:
                    #health +=1
            #except Exception,e:
                #pass


            #try:
                #response = opener.open('http://360.cn',timeout=timeout)
                #content = response.read()
                #if response.getcode() == 200 and 70000 < len(content) < 120000 and "<title>360" in content:
                    #health +=1
            #except Exception,e:
                #pass



            #try:
                #response = opener.open('http://www.baidu.com',timeout=timeout)
                #content = response.read()
                #if response.getcode() == 200 and 70000 < len(content) < 120000 and "<title>\xe7\x99\xbe\xe5\xba\xa6\xe4\xb8\x80\xe4\xb8\x8b\xef\xbc\x8c\xe4\xbd\xa0\xe5\xb0\xb1\xe7\x9f\xa5\xe9\x81\x93</title>" in content:
                    #health +=1
            #except Exception,e:
                #pass




            return True
        except Exception,e:
            pass
        return False



    @staticmethod
    def ip_validation(ip):
        """
        ip有效性验证
        @param ip  字符串格式
        """
        ip_exp = re.compile("^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])$")
        if ip_exp.match(ip):
            return True
        else:
            return False

    @staticmethod
    def ip2int(ip):
        return struct.unpack("!I",socket.inet_aton(ip))[0]

    @staticmethod
    def ip2int_aligned(ip):
        return "%010d"%Ip.ip2int(ip)


    @staticmethod
    def int2ip(i):
        return socket.inet_ntoa(struct.pack("!I",i))

    @staticmethod
    def gen_iplist_via_ipsection(ipsection):
        """
        根据IP段生成IP列表
        @param ipsection :123.56.0.0-123.57.255.255
        @return ["xx.xx.xx.xx","xx.xx.xx.xx"]
        """
        start,end = [Ip.ip2int(x) for x in ipsection.split('-')]
        return [Ip.int2ip(num) for num in range(start,end+1) if num & 0xff]




    @staticmethod
    def get_interface_ip(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])



    @staticmethod
    def get_local_ip():
        """
        Author:Lw
        获取本地IP
        """
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = '-'
        if ip.startswith("127.") and os.name != "nt":
            interfaces = ["eth0", "eth1", "eth2", "wlan0", "wlan1", "wifi0", "ath0", "ath1", "ppp0", "ppp1", "em0", "em1",
                          "em2"]
            for ifname in interfaces:
                try:
                    ip = Ip().get_interface_ip(ifname)
                    break;
                except IOError:
                    pass

        return ip


if __name__ == '__main__':
    pass
