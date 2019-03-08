#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
初始化parser.

设置以下内容：
1. event schema loader
2. 配置式parser列表
3. 手工parser目录
"""

import requests
import json

_fn_load_event_schemas = None
_fn_load_parsers = None


def init_parser(fn_load_event_schemas, fn_load_parsers):
    """
    初始化parser

    :param fn_load_event_schemas:
    :param fn_load_parsers:
    :return:
    """
    global _fn_load_event_schemas
    global _fn_load_parsers

    _fn_load_event_schemas = fn_load_event_schemas
    _fn_load_parsers = fn_load_parsers


def get_fn_load_event_schemas():
    """
    获取event schema生成器，该回调返回evnet schema列表{eventname:{fieldname:fieldtype}}
    :return:
    """
    return _fn_load_event_schemas


def get_fn_load_parsers():
    return _fn_load_parsers


# some helper functions for building functions
def build_fn_load_event_schemas_on_web(event_url, auth):
    def result_fn():
        response = requests.get(event_url, params={"auth": auth})

        new_event_schema_dictionary = {}
        if response.status_code == 200:
            events = json.loads(response.text)["values"]
            for event in events:
                properties = {p["name"]: p["type"] for p in event["properties"]}
                new_event_schema_dictionary[event["name"]] = properties

        if new_event_schema_dictionary:
            return new_event_schema_dictionary
        else:
            raise RuntimeError(u"无法获取日志定义配置")

    return result_fn


def build_fn_load_event_schemas_on_file(file_path):
    def result_fn():
        with file(file_path) as input:
            content = input.read()
            events = json.loads(content)["values"]

            new_event_schema_dictionary = dict()
            for event in events:
                properties = {p["name"]: p["type"] for p in event["properties"]}
                new_event_schema_dictionary[event["name"]] = properties

            if new_event_schema_dictionary:
                return new_event_schema_dictionary
            else:
                raise RuntimeError(u"无法获取日志定义配置")

    return result_fn


def build_fn_load_event_schemas_on_dict(dict_data_container):
    def result_fn():
        content = dict_data_container.get()
        events = json.loads(content)["values"]

        new_event_schema_dictionary = dict()
        for event in events:
            properties = {p["name"]: p["type"] for p in event["properties"]}
            new_event_schema_dictionary[event["name"]] = properties

        if new_event_schema_dictionary:
            return new_event_schema_dictionary
        else:
            raise RuntimeError(u"无法获取日志定义配置")

    return result_fn


def build_fn_load_parsers_on_web(parser_url, auth):
    def result_fn():
        response = requests.get(parser_url, params={"auth": auth})

        parsers = []
        if response.status_code == 200:
            parsers = json.loads(response.text)["values"]

        if parsers:
            parsers = [p for p in parsers if p["status"]]
            return parsers

        raise RuntimeError(u"无法获取日志解析配置")

    return result_fn


def build_fn_load_parsers_on_file(file_path):
    def result_fn():
        with file(file_path) as input:
            content = input.read()

            parsers = []
            parsers = json.loads(content)["values"]

            if parsers:
                parsers = [p for p in parsers if p["status"]]
                return parsers

            raise RuntimeError(u"无法获取日志解析配置")

    return result_fn
