#!/usr/bin/env python
# -*- coding: utf-8 -*-
from babel_python.babelrabbitmq import get_hash

__author__ = 'lw'


class TestSharding:

    def test_sharding_alg(self):
        for i in range(100):
            print i, get_hash(str(i))

        assert False
