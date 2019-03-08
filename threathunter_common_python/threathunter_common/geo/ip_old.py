#!/usr/bin/env python
# coding: utf-8
# author: frk

import struct
import socket
import os

_unpack_V = lambda b: struct.unpack("<L", b)
_unpack_N = lambda b: struct.unpack(">L", b)
_unpack_C = lambda b: struct.unpack("B", b)


class IP:
    offset = 0
    index = 0
    binary = ""

    @staticmethod
    def load(file):
        try:
            path = os.path.abspath(file)
            with open(path, "rb") as f:
                IP.binary = f.read()
                IP.offset, = _unpack_N(IP.binary[:4])
                IP.index = IP.binary[4:IP.offset]
        except Exception as ex:
            print "cannot open file %s" % file
            print ex.message
            exit(0)

    @staticmethod
    def find(ip):
        index = IP.index
        offset = IP.offset
        binary = IP.binary
        nip = socket.inet_aton(ip)
        ipdot = ip.split('.')
        if int(ipdot[0]) < 0 or int(ipdot[0]) > 255 or len(ipdot) != 4:
            return "N/A"

        tmp_offset = int(ipdot[0]) * 4
        start, = _unpack_V(index[tmp_offset:tmp_offset + 4])

        index_offset = index_length = 0
        max_comp_len = offset - 1024
        start = start * 8 + 1024
        while start < max_comp_len:
            if index[start:start + 4] >= nip:
                index_offset, = _unpack_V(index[start + 4:start + 7] + chr(0).encode('utf-8'))
                index_length, = _unpack_C(index[start + 7])
                break
            start += 8

        res_offset = offset + index_offset - 1024
        return binary[res_offset:res_offset + index_length].decode('utf-8')


instance = None
datfile = os.path.join(os.path.dirname(__file__), "17monipdb.dat")


def find(ip):
    # keep find for compatibility
    try:
        ip = socket.gethostbyname(ip)
    except socket.gaierror:
        return

    global instance
    if instance is None:
        instance = IP()
        instance.load(datfile)

    return instance.find(ip)
