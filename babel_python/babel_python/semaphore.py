#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threading import Condition, Lock
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

__author__ = 'lw'

"""
Add timeout for wait of the standard implementation.

see @threading.Semaphore for detail
"""


def CountDownLatch(*args, **kwargs):
    return _CountDownLatch(*args, **kwargs)


class _CountDownLatch(object):

    # After Tim Peters' semaphore class, but not quite the same (no maximum)

    def __init__(self, value=1):
        if value <= 0:
            raise ValueError("count down latch initial value must be > 0")
        self.__cond = Condition(Lock())
        self.__value = value

    def wait(self, blocking=True, timeout=1):
        if not blocking:
            return self.__value <= 0

        endtime = None
        with self.__cond:
            while self.__value > 0:
                if endtime is None:
                    endtime = _time() + timeout
                else:
                    timeout = endtime - _time()
                    if timeout <= 0:
                        return False
                self.__cond.wait(timeout)
            else:
                return True

    def countdown(self):
        with self.__cond:
            self.__value -= 1
            self.__cond.notify()
