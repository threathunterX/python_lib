#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "nebula"

# 详细日志查询时,合并日志所需的event的固定字段

Event_Schema = {
    'fixed': [
        'id',
        'pid',
        'timestamp',
        'sid',
        'uid',
        'did',
        'platform',
        'page',
        'c_ip',
        'c_port',
        'c_bytes',
        'c_body',
        'c_type',
        's_ip',
        's_port',
        's_bytes',
        's_body',
        's_type',
        'host',
        'uri_stem',
        'uri_query',
        'referer',
        'method',
        'status',
        'cookie',
        'useragent',
        'xforward',
        'request_time',
        'request_type',
        'referer_hit'
    ]

}
