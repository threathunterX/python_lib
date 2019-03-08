#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'lw'


class HttpDataContext(object):
    """
    Contains dict data from body/cookie/query in http message
    """
    def __init__(self):
        self.request_body = dict()
        self.response_body = dict()
        self.query = dict()
        self.cookie = dict()

    def from_http_msg(self, msg):
        self.request_body = msg.req_form_data or msg.req_flat_json_data
        self.response_body = msg.resp_form_data or msg.resp_flat_json_data
        self.cookie = msg.cookie_data
        self.query = msg.uri_query_data

    def get_data_from_request_body(self, field, default=None):
        return self.request_body.get(field, default)

    def get_data_from_response_body(self, field, default=None):
        return self.response_body.get(field, default)

    def get_data_from_query(self, field, default=None):
        return self.query.get(field, default)

    def get_data_from_cookie(self, field, default=None):
        return self.cookie.get(field, default)

