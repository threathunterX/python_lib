#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import socket
if os.name != "nt":
    import fcntl
    import struct


__author__ = "nebula"


def get_interface_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

local_ip = None


def get_local_ip():
    global local_ip
    if not local_ip:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = '-'
        if ip.startswith("127.") and os.name != "nt":
            interfaces = ["eth0", "eth1", "eth2", "wlan0", "wlan1", "wifi0", "ath0", "ath1", "ppp0", "ppp1", "em0", "em1",
                          "em2"]
            for ifname in interfaces:
                try:
                    ip = get_interface_ip(ifname)
                    break;
                except IOError:
                    pass
        local_ip = ip

    return local_ip
