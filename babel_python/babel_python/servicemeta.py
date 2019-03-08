#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from .util import text, unicode_dict, unicode_obj

__author__ = 'lw'


class ServiceMeta(object):
    CALLMODES = frozenset(("notify", "rpc", "polling"))
    DELIVERMODES = frozenset(("queue", "topic", "sharding", "shuffle", "topicsharding", "topicshuffle"))
    SERVERIMPLS = frozenset(("redis", "rabbitmq"))
    CODERS = frozenset(("mail", ))

    def __init__(self, **kwargs):
        self._name = text(kwargs.get("name", ""))
        self._callmode = text(kwargs.get("callmode", ""))
        self._delivermode = text(kwargs.get("delivermode", ""))
        self._serverimpl = text(kwargs.get("serverimpl", ""))
        self._coder = text(kwargs.get("coder", ""))
        self._options = unicode_dict(kwargs.get("options", dict()))

        self._validate_callmode()
        self._validate_deliver_mode()
        self._validate_serverimpl()
        self._validate_coder()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)
        if not name:
            raise RuntimeError("empty service name")

    @property
    def callmode(self):
        return self._callmode

    @callmode.setter
    def callmode(self, callmode):
        self._callmode = text(callmode)
        self._validate_callmode()

    def _validate_callmode(self):
        if self._callmode not in ServiceMeta.CALLMODES:
            raise RuntimeError("invalid call mode")

    @property
    def delivermode(self):
        return self._delivermode

    @delivermode.setter
    def delivermode(self, delivermode):
        self._delivermode = text(delivermode)
        self._validate_deliver_mode()

    def _validate_deliver_mode(self):
        if self._delivermode not in ServiceMeta.DELIVERMODES:
            raise RuntimeError("invalid deliver modes")

    @property
    def serverimpl(self):
        return self._serverimpl

    @serverimpl.setter
    def serverimpl(self, serverimpl):
        self._serverimpl = text(serverimpl)
        self._validate_serverimpl()

    def _validate_serverimpl(self):
        if self._serverimpl not in ServiceMeta.SERVERIMPLS:
            raise RuntimeError("invalid server implements")

    @property
    def coder(self):
        return self._coder

    @coder.setter
    def coder(self, coder):
        self._coder = coder
        self._validate_coder()

    def _validate_coder(self):
        if self._coder not in ServiceMeta.CODERS:
            raise RuntimeError("invalid coder")

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, options):
        self._options = unicode_dict(options or dict())

    def add_option(self, op_name, op_value):
        self._options = self._options or dict()
        self._options[text(op_name)] = unicode_obj(op_value)

    def get_option(self, op_name, default = None):
        if not self._options:
            return default

        return self._options.get(op_name, None)

    def get_dict(self):
        return {
            "name": self.name,
            "callmode": self.callmode,
            "delivermode": self.delivermode,
            "serverimpl": self.serverimpl,
            "coder": self.coder,
            "options": self.options
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return ServiceMeta(**d)

    @staticmethod
    def from_json(j):
        return ServiceMeta.from_dict(json.loads(j))

    def __str__(self):
        return "ServiceMeta[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict == other.get_dict()

    def __ne__(self, other):
        return not self == other
