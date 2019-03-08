# -*- coding: utf8 -*-
"""
Parse config in properties format
"""

from __future__ import print_function

import logging

from threathunter_common.util import utf8

from . import BaseParser

LOGGER = logging.getLogger("config")


class PropertiesParser(BaseParser):
    """
    Parse config from properties file
    """

    def __init__(self, name):
        """
        :param name: parser name
        """

        self.name = name

    def parse(self, data):
        """
        parse text in properties format to python dict
        :param data: text in properties format
        """

        try:
            result = {}
            for _ in data.splitlines():
                _ = utf8(_)
                _ = _.strip()
                if not _:
                    continue
                if _.startswith("#"):
                    continue

                key, value = _.split("=", 1)
                key, value = key.strip(), value.strip()
                if not key:
                    continue

                result[key] = value
            return result
        except Exception as err:
            LOGGER.error("config %s: fail to parse properties config, the error is %s",
                         self.name, err)
            return {}

    def dump(self, config):
        """
        dump to properties format.
        """

        try:
            result = "\n".join(["{}={}".format(k, v) for k, v in config.items()])
            return result
        except Exception as err:
            LOGGER.error("config %s: fail to generate properties config, the error is %s",
                         self.name, err)
            return ""
