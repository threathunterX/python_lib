#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from threathunter_common.eventmeta import EventMeta
from ..util import text

__author__ = 'lw'

class Service(object):
    def __init__(self, request_meta=None, response_meta=None, oneway=None, remark=None):
        self._request_meta = request_meta
        self._response_meta = response_meta
        self._oneway = oneway
        self._remark = text(remark or "")

    @property
    def request_meta(self):
        return self._request_meta

    @request_meta.setter
    def request_meta(self, request_meta):
        self._request_meta = request_meta

    @property
    def response_meta(self):
        return self._response_meta

    @response_meta.setter
    def response_meta(self, response_meta):
        self._response_meta = response_meta

    @property
    def oneway(self):
        return self._oneway

    @oneway.setter
    def oneway(self, oneway):
        self._oneway = oneway

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or "")

    def process(self, event):
        pass

    def get_dict(self):
        if self.oneway:
            response_meta_dict = None
        else:
            response_meta_dict = self.response_meta.get_dict()
        return {
            "requestMeta": self.request_meta.get_dict(),
            "responseMeta": response_meta_dict,
            "oneway": self.oneway,
            "remark": self.remark
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return Service(EventMeta.from_dict(d.get("requestMeta")), EventMeta.from_dict(d.get("responseMeta")),
                       d.get("oneway"), d.get("remark"))

    @staticmethod
    def from_json(json_str):
        return Service.from_dict(json.loads(json_str))

    def __str__(self):
        return "Service[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


