#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = "nebula"


try:
    from atomic import AtomicLong
except ImportError:
    """
    In case there is not atomic, such as the windows platform
    """
    class AtomicLong:
        def __init__(self, value=0):
            self._value = value

        @property
        def value(self):
            return self._value

        @value.setter
        def value(self, new):
            self._value = new

        def __iadd__(self, inc):
            self._value += inc
            return self

        def __isub__(self, dec):
            self._valu -= dec
            return self

        def get_and_set(self, new_value):
            old_value = self._value
            self._value = new_value
            return old_value

        def swap(self, new_value):
            return self.get_and_set(new_value)

        def compare_and_set(self, expect, new_value):
            if self._value == expect:
                self._value = new_value
                return True
            else:
                return False

        def compare_and_swap(self, expect_value, new_value):
            return self.compare_and_set(expect_value, new_value)

        def __eq__(self, a):
            if self is a:
                return True
            elif isinstance(a, AtomicLong):
                return self.value == a.value
            else:
                return self.value == a

        def __ne__(self, a):
            return not (self == a)

        def __lt__(self, a):
            if self is a:
                return False
            elif isinstance(a, AtomicLong):
                return self.value < a.value
            else:
                return self.value < a








