#!/usr/bin/env python
# -*- coding: utf-8 -*-
import array

__author__ = 'lw'

import os
import socket
import struct
try:
    import mmap
except ImportError:
    mmap = None

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


net_192_168 = (192 << 8) + 168
net_10_0 = 10
net_172_16 = (172 << 4) + 1


def is_private_ip(ip):
    try:
        i = ip2int(ip)
        if i >> 16 == net_192_168:
            return True
        if i >> 24 == net_10_0:
            return True
        if i >> 20 == net_172_16:
            return True
        return False
    except:
        return False


datfile = os.path.join(os.path.dirname(__file__), "17monipdb.dat")


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
        buf = file(filename, "rb").read()
        offset = _unpack_B(buf[:4])
        offset -= 1024

        self.indexes = [None] * 257
        for i in range(256):
            i_start = i * 4 + 4
            i_end = i_start + 4
            self.indexes[i] = _unpack_L(buf[i_start:i_end])
        self.indexes[256] = (offset - 1028) / 8

        self.ip_num = (offset - 1028) / 8
        self.ip_array = array.array("L", [0] * self.ip_num)
        self.offset_array = array.array("L", [0] * self.ip_num)
        self.length_array = array.array("c", [chr(0)] * self.ip_num)

        self.data = buf[offset:]
        o = file("/Users/lw/ips.txt", "w")
        for i in range(0, self.ip_num):
            pos = 1028 + 8 * i
            ip_int = _unpack_B(buf[pos:pos+4])
            data_offset = _unpack_L(buf[pos+4:pos+7]+b'\0')
            data_len = _unpack_C(buf[pos + 7])
            self.ip_array[i] = ip_int
            self.offset_array[i] = data_offset
            self.length_array[i] = chr(data_len)
            print >> o, n_to_a(ip_int), self.data[data_offset:data_offset+data_len]
        o.close()

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

        offset = self.offset_array[lo]
        length = ord(self.length_array[lo])
        value = self.data[offset:offset+length]
        return value.decode('utf-8')

    def find(self, ip):
        return self._lookup_ipv4(ip)


def find(ip):
    # keep find for compatibility
    try:
        ip = socket.gethostbyname(ip)
    except socket.gaierror:
        return

    global instance
    if instance is None:
        instance = IPv4Database()

    return instance.find(ip)
