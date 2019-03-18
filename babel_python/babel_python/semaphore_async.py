#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gevent.timeout import Timeout
import gevent.event as gevent_event

__author__ = "nebula"

"""
Add timeout for wait of the standard implementation.

see @threading.Semaphore for detail
"""


def CountDownLatch(*args, **kwargs):
    return _CountDownLatch(*args, **kwargs)


class _CountDownLatch(object):

    def __init__(self, value=1):
        self.__value = int(value)
        if self.__value <= 0:
            raise ValueError("count down latch initial value must be > 0")
        self.ev = gevent_event.AsyncResult()

    def wait(self, blocking=True, timeout=1):
        if not blocking:
            return self.__value <= 0

        if self.__value <= 0:
            return True

        try:
            self.ev.wait(timeout)
            return self.__value <= 0
        except Timeout:
            return False

    def countdown(self):
        self.__value -= 1
        if self.__value <= 0:
            self.ev.set()

    @property
    def value(self):
        return self.__value
