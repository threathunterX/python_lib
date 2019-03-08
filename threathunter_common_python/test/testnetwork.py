#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threathunter_common.network import get_local_ip

__author__ = 'lw'


def test_localip():
    print get_local_ip()
    assert False

test_localip()
