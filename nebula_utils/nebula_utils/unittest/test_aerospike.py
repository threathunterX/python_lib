#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
import unittest

from nebula_utils.persist_compute.cache import ContinuousDB

current_ts = int(time.time()) / 3600 * 3600
work_ts = float(current_ts)
work_day = int(work_ts - (current_ts % 86400) + time.timezone)
var = 'ip__visit__dynamic_distinct_user__1h__slot'
var_value = 1


class TestContinuousDB(unittest.TestCase):

    def setUp(self):
        ContinuousDB.get_db()

    def test_add_key(self):
        ContinuousDB.ttl = 10
        ContinuousDB.get_db()
        ContinuousDB.add('0.0.0.0', 'ip', work_day, work_ts, {var: var_value})
        record = ContinuousDB.query_many(
            '0.0.0.0', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), var_value)

        # 测试key过期生效
        time.sleep(ContinuousDB.ttl + 1)
        record = ContinuousDB.query_many(
            '0.0.0.0', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), None)

    def test_same_bin(self):
        ttl = 5
        ContinuousDB.ttl = ttl
        ContinuousDB.get_db()
        ContinuousDB.add('0.0.0.1', 'ip', work_day, work_ts, {var: var_value})
        record = ContinuousDB.query_many(
            '0.0.0.1', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), var_value)

        # 测试重新插入，改变ttl，key没有过期
        ContinuousDB.ttl = ttl * 2
        ContinuousDB.add('0.0.0.1', 'ip', work_day, work_ts, {var: var_value})
        time.sleep(ttl + 1)
        record = ContinuousDB.query_many(
            '0.0.0.1', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), var_value)

    def test_diff_bin(self):
        ttl = 10
        ContinuousDB.ttl = ttl
        ContinuousDB.get_db()
        ContinuousDB.add('0.0.0.2', 'ip', work_day, work_ts, {var: var_value})
        record = ContinuousDB.query_many(
            '0.0.0.2', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), var_value)

        # 测试重新插入，改变ttl，key过期
        ContinuousDB.ttl = ttl / 2
        ContinuousDB.add('0.0.0.2', 'ip', work_day,
                         work_ts + 3600, {var: var_value})
        time.sleep(ttl / 2 + 1)
        record = ContinuousDB.query_many(
            '0.0.0.2', 'ip', [str(work_ts)], [var])
        self.assertEqual(record.get(str(work_ts), {}).get(var), None)

if __name__ == '__main__':
    unittest.main()
