#!/usr/bin/env python
# -*- coding: utf-8 -*-


import datetime
import pytest

from threathunter_common import util as big_util

FAKE_TIME = datetime.datetime(2016, 11, 15, 17, 05, 55)
origin_datetime_cls = datetime.datetime


def get_fake_time():
    return FAKE_TIME


def set_fake_time(faketime):
    global FAKE_TIME
    FAKE_TIME = origin_datetime_cls(*faketime)


def set_fake_time_on_ts(ts):
    global FAKE_TIME
    FAKE_TIME = origin_datetime_cls.fromtimestamp(ts)


@pytest.fixture
def patch_datetime_now(monkeypatch):

    class mydatetime:
        @classmethod
        def now(cls):
            return FAKE_TIME

    def mock_now():
        import time
        return time.mktime(FAKE_TIME.timetuple())

    monkeypatch.setattr(datetime, 'datetime', mydatetime)
    monkeypatch.setattr(big_util, 'millis_now', mock_now)

