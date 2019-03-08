#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
container for global singleton config object
"""

from .config import Config, EmptyConfig


class Container(object):
    """
    A config dict for different config objects with different names
    """

    def __init__(self):
        self.config_store = dict()

    def get_config(self, name):
        """
        Get config corresponding to the name
        :param name: config name
        :return: corresponding config
        """

        return self.config_store.get(name) or EmptyConfig()

    def set_config(self, name, config):
        """
        Add a new config with a name
        :param name: config name
        :param config: the new config
        """

        assert isinstance(config, Config)
        self.config_store[name] = config


configcontainer = Container()
