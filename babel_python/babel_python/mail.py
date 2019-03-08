#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json, base64

from .util import text, unicode_list, unicode_dict, binary_data

__author__ = 'lw'


class Mail(object):
    """Carry data communicated in the network,

    The f, to, reply, headers all contain unicode string, in order to keep consistency
    """
    pass

    def __init__(self, f, to, reply, requestid, headers, body):
        self._from = text(f)

        if not isinstance(to, list):
            to = [to]
        self._to = unicode_list(to)

        if not isinstance(reply, list):
            reply = [reply]
        self._reply = unicode_list(reply)

        self._requestid = text(requestid)

        self._headers = unicode_dict(headers)
        self._body = binary_data(body)

    @property
    def f(self):
        """the source f the mail"""
        return self._from

    @f.setter
    def f(self, f):
        self._from = text(f)

    @property
    def to(self):
        """the destination of the mail"""
        return self._to or []

    @to.setter
    def to(self, to):
        if not isinstance(to, list):
            to = [to]
        self._to = unicode_list(to)

    @property
    def reply(self):
        return self._reply or []

    @reply.setter
    def reply(self, reply):
        """the reply address of the mail"""
        if not isinstance(reply, list):
            reply = [reply]
        self._reply = unicode_list(reply)

    @property
    def headers(self):
        """meta data of the mail, it should be Map<String, String>"""
        return self._headers or {}

    @headers.setter
    def headers(self, headers):
        if not isinstance(headers, dict):
            headers = dict()

        self._headers = unicode_dict(headers)

    @property
    def requestid(self):
        return self._requestid

    @requestid.setter
    def requestid(self, requestid):
        self._requestid = text(requestid)

    def add_header(self, key, value):
        if self._headers is None:
            self._headers = dict()

        self._headers[text(key)] = text(value)

    def get_header(self, key, default=""):
        return self._headers.get(key, default)

    @property
    def body(self):
        """binary data of the body.

        The body is a byte array, if you a string, pls use method string_body
        """
        if self._body is None:
            return bytearray()

        return self._body

    @property
    def string_body(self):
        """return the string representation of the body"""
        return text(str(self.body))

    @body.setter
    def body(self, body):
        """set body data.

        If body is a string/unicode, it will first be converted to utf8 string, and then
        byte array, you can get it back by using string_body
        """
        if body is None:
            body = bytearray()

        self._body = binary_data(body)

    def get_dict(self):
        result = dict()
        result["from"] = self.f
        result["to"] = self.to
        result["reply"] = self.reply
        result["headers"] = self.headers
        result["body"] = self.body
        result["requestid"] = self.requestid
        return result

    @staticmethod
    def from_dict(data):
        return Mail(data.get("from"), data.get("to"), data.get("reply"), data.get("requestid"),
                    data.get("headers"), data.get("body"))

    def get_json(self):
        data = self.get_dict()
        # body: bytearray --> base64
        data["body"] = base64.b64encode(self._body)
        return json.dumps(data)

    @staticmethod
    def from_json(json_str):
        try:
            data = json.loads(text(json_str))
        except Exception as err:
            print "meet error {} while decode {}".format(err, json_str)

        # body: base64 --> bytearray
        body = data["body"]
        if not body:
            return None

        body = base64.b64decode(body)
        data["body"] = binary_data(body)

        return Mail.from_dict(data)

    @staticmethod
    def new_mail(sender, receiver, requestid):
        return Mail(sender, [receiver], [sender], requestid, {}, "")

    def __str__(self):
        return 'Mail[{}]'.format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

