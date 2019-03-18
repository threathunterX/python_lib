#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import array
import io
import os
import socket
import struct
from ipaddr import IPv4Address

from ..util import text

try:
    import mmap
except ImportError:
    mmap = None

__author__ = "nebula"
__all__ = ['IPv4Database', 'find']

_unpack_L = lambda b: struct.unpack("<L", b)[0]
_unpack_B = lambda b: struct.unpack(">L", b)[0]


def ip2int(addr):
    return struct.unpack("!I", socket.inet_aton(addr))[0]


def n_to_a(ip):
    return "%d.%d.%d.%d" % ((ip >> 24) % 256, (ip >> 16) % 256, (ip >> 8) % 256, ip % 256)


def _unpack_C(b):
    if isinstance(b, int):
        return b
    return struct.unpack("B", b)[0]


datfile = os.path.join(os.path.dirname(__file__), "threathunter_ip.dat")


instance = None


class IPv4Database(object):
    """Database for search IPv4 address.
    The 17mon dat file format in bytes::
        -----------
        | 4 bytes |                     <- offset number
        -----------------
        | 256 * 4 bytes |               <- first ip number index
        -----------------------
        | offset - 1028 bytes |         <- ip index
        -----------------------
        |    data  storage    |
        -----------------------
    """
    def __init__(self, filename=None):
        if filename is None:
            filename = datfile
        # store the index by the first number of ip address
        self.first_indexes = None

        # start the index ip and the correlated offset information in the db
        self.ip_num = 0
        self.ip_array = None
        self.offset_array = None
        self.length_array = None

        # store the data
        self.data = None

        self._parse(filename)

    def _parse(self, filename):
        buf = io.open(filename, "rb").read()
        offset = _unpack_B(buf[:4])
        offset -= 1024

        self.indexes = [None] * 257
        for i in range(256):
            i_start = i * 4 + 4
            i_end = i_start + 4
            self.indexes[i] = _unpack_L(buf[i_start:i_end])
        self.indexes[256] = (offset - 1028) / 8

        self.ip_num = int((offset - 1028) / 8)
        self.ip_array = array.array("L", [0] * self.ip_num)
        addr_cache = dict()
        self.addr_array = [None]*self.ip_num

        for i in range(0, self.ip_num):
            pos = 1028 + 8 * i
            ip_int = _unpack_B(buf[pos:pos+4])
            data_offset = _unpack_L(buf[pos+4:pos+7]+b'\0')
            data_len = _unpack_C(buf[pos + 7])

            if data_offset not in addr_cache:
                addr_cache[data_offset] = text(buf[offset+data_offset:offset+data_offset+data_len])
            self.addr_array[i] = addr_cache[data_offset]
            self.ip_array[i] = ip_int

    def close(self):
        pass

    def _lookup_ipv4(self, ip):
        nip = ip2int(ip)
        first_number = (nip >> 24) % 256
        lo, hi = self.indexes[first_number:first_number+2]

        while lo < hi:
            mid = (lo + hi) // 2
            mid_val = self.ip_array[mid]

            if mid_val < nip:
                lo = mid + 1
            else:
                hi = mid

        if lo >= self.ip_num:
            return None

        return self.addr_array[lo]

    def find(self, ip):
        return self._lookup_ipv4(ip)


def find(ip):
    # keep find for compatibility
    try:
        ip = socket.gethostbyname(ip)
    except socket.gaierror:
        return u'未知\t未知\t未知'

    # 增加非中国的IP的操作，需求文档：http://wiki.threathunter.home/pages/viewpage.action?pageId=4296613
    ipv4 = IPv4Address(ip)

    # 判断IP是否为内网地址或本地地址，网段为：
    # 10.0.0.0/8
    # 172.16.0.0/12
    # 192.168.0.0/16
    # 127.0.0.0/8
    if ipv4.is_private or ipv4.is_loopback:
        return u'内网\t内网\t内网'

    # 判断IP是否为特殊地址，网段为：169.254.0.0/16
    if ipv4.is_link_local:
        return u'未知\t未知\t未知'

    global instance
    if instance is None:
        instance = IPv4Database()

    info = instance.find(ip)
    # 如果地址为none返回未知
    if info is None:
        return u'未知\t未知\t未知'
    else:
        return info


# input = open("/Users/lw/taobaodata.txt").readlines()
# input = input
# for i in input:
#     ip, loc = i.split(":")
#     expect = loc.decode("utf-8").strip()
#     actual = find(ip).strip()
#     assert expect == actual
#
# print len(input)

# input = open("/Users/lw/taobaodata.txt").readlines()
# input = input
# for i in input[:100]:
#     i = i.split(":")[0]
#     previous = n_to_a(ip2int(i)-1)
#     next = n_to_a(ip2int(i)+1)
#     assert find(previous) == find(i)
#
# print len(input)
