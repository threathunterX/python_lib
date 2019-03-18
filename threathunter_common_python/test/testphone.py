#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.geo.phonelocator import *

__author__ = "nebula"


def test_phone():
    print check_phone_number("+13482345020", None)
    assert check_phone_number("13482345020", 'CN')
    assert not check_phone_number("+134823450", None)

    print get_carrier("13482121123", 'CN')
    print get_carrier("13815430576", 'CN')
    print get_carrier("13093705423", 'CN')

    print get_geo("13482121123", 'CN')
    print get_geo("13815430576", 'CN')
    print get_geo("13093705423", 'CN')
    print 111, get_geo("020 8366 1177", "GB")
    print 111, get_geo("+442083661177")

    print phonenumbers.parse("020 8366 1177", "GB")
    print phonenumbers.parse("+442083661177")



    assert False


