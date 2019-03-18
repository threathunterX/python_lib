#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.network import get_local_ip

__author__ = "nebula"


def test_localip():
    print get_local_ip()
    assert False

test_localip()
