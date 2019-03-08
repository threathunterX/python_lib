#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.geo import ip_old
from threathunter_common.geo import ip
from threathunter_common.geo import threathunter_ip
from threathunter_common.geo.ip import is_private_ip
from threathunter_common.util import millis_now

__author__ = 'lw'


def test_ip_public():
    print is_private_ip("192.168.1.1")
    print is_private_ip("10.1.1.1")
    print is_private_ip("172.16.0.1")
    print is_private_ip("172.20.0.1")
    print is_private_ip("182.20.0.1")
    print is_private_ip("192.178.1.1")


def test_ip_old():
    print ip_old.find("10.231.114.58")
    print ip_old.find("20.231.114.58")
    print ip_old.find("30.231.114.58")
    print ip_old.find("101.231.114.58")
    print ip_old.find("120.80.119.255")
    print ip_old.find("192.168.1.2")
    print ip_old.find("223.208.32.0")
    print ip_old.find("255.255.254.255")
    assert False


def test_ip():
    print ip.find("10.231.114.58")
    print ip.find("20.231.114.58")
    print ip.find("30.231.114.58")
    print ip.find("101.231.114.58")
    print ip.find("120.80.119.255")
    print ip.find("192.168.1.2")
    print ip.find("223.208.32.0")
    print ip.find("255.255.254.255")
    assert False


def test_ip_threathunter():
    print threathunter_ip.find("10.231.114.58")
    print threathunter_ip.find("20.231.114.58")
    print threathunter_ip.find("30.231.114.58")
    print threathunter_ip.find("101.231.114.58")
    print threathunter_ip.find("120.80.119.255")
    print threathunter_ip.find("192.168.1.2")
    print threathunter_ip.find("223.208.32.0")
    print threathunter_ip.find("255.255.254.255")


    print threathunter_ip.find("")
    print threathunter_ip.find("")
    print threathunter_ip.find("")
    assert False


def n_to_a(ip):
    return "%d.%d.%d.%d" % ((ip >> 24) % 256, (ip >> 16) % 256, (ip >> 8) % 256, ip % 256)


test_data = []
for i in range(1, 100000):
    test_data.append(n_to_a(i*16))


def test_performance():
    for mod in (ip, ip_old, threathunter_ip):
        round = len(test_data)
        mod.find("1.1.1.1")
        start = millis_now()
        for d in test_data:
            mod.find(d)
        end = millis_now()
        t = end - start
        print "{} spend {} ms on {} records, {} records per second".format(mod.__name__, t, round, round * 1000.0 / t)
    assert False


def test_compatibility():
    for d in test_data:
        old = ip_old.find(d)
        new = ip.find(d)
        if (old != new):
            print d
            print old
            print new
    assert False
