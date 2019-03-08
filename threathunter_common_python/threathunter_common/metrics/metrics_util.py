#!/usr/bin/env python
# -*- coding: utf-8 -*-

def get_latency_str_for_millisecond(ms):
    if ms < 50:
        return "<50ms"
    elif ms < 100:
        return "50ms-100ms"
    elif ms < 500:
        return "100ms-500ms"
    elif ms < 1000:
        return "500ms-1s"
    elif ms < 5000:
        return "1s-5s"
    elif ms < 10000:
        return "5s-10s"
    elif ms < 15000:
        return "10s-15s"
    else:
        return ">15s"


def get_latency_str_for_second(s):
    return get_latency_str_for_millisecond(int(s * 1000))

