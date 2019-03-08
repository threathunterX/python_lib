#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .metrics_util import get_latency_str_for_millisecond, get_latency_str_for_second


_async_mode = False
_is_used = False


def set_async_mode():
    global _is_used
    global _async_mode

    if _is_used:
        raise RuntimeError("metrics has already been used by others")

    _async_mode = True


class MetricsAgent(object):

    @staticmethod
    def get_instance():
        global _is_used
        global _async_mode
        _is_used = True

        if _async_mode:
            from .metricsagent_async import MetricsAgentAsync
            return MetricsAgentAsync.get_instance()
        else:
            from .metricsagent_sync import MetricsAgentSync
            return MetricsAgentSync.get_instance()