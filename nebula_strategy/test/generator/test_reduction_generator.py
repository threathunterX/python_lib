#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from nebula_strategy.generator.reduction_gen import *
from nebula_meta.property import Property

from ..setup_test import *


def setup_module(module):
    print ('start testing')
    load_config()


def teardown_module(module):
    print ('finish testing')


def test_gen_reduction():
    gen_function('count', 'c_ip')
    gen_function('distinct_count', 'c_ip')

    with pytest.raises(RuntimeError) as excinfo:
        gen_function('invalid', 'c_ip')
    assert excinfo.value.message == '不支持操作(invalid)'

