# -*- coding: utf8 -*-
"""
Parse config from json
"""

from __future__ import print_function, unicode_literals

import json
import logging

from . import BaseParser

LOGGER = logging.getLogger("config")


class JsonParser(BaseParser):
    """
    Parse config from json.
    """

    def __init__(self, name):
        """
        :param name: parser name
        """
        self.name = name

    def parse(self, data):
        """
        parse json text into python dict
        :param data: text in json format
        :return:
        """
        try:
            return json.loads(data)
        except Exception as err:
            LOGGER.error("config %s: fail to parse json config, the error is %s", self.name, err)
            return {}

    def dump(self, config, **kwargs):
        """
        dump dict to json format
        :param config: config as dict
        """
        try:
            return json.dumps(config, **kwargs)
        except Exception as err:
            LOGGER.error("config %s: fail to dump config to json, the error is %s", self.name, err)
            return ""
