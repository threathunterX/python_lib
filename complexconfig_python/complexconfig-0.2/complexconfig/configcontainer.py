#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .config import Config, EmptyConfig


class Container(object):
    def __init__(self):
        self.config_store = dict()

    def get_config(self, name):
        return self.config_store.get(name) or EmptyConfig()

    def set_config(self, name, c):
        assert isinstance(c, Config)
        self.config_store[name] = c


configcontainer = Container()
