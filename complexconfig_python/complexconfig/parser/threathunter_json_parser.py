# -*- coding: utf8 -*-
"""
parse config in threathunter json format
"""

from __future__ import print_function, unicode_literals

import json
import logging

from . import BaseParser

LOGGER = logging.getLogger("config")


class ThreathunterJsonParser(BaseParser):
    """
    Parse config from json with threathunter format.

    Threathunter api uses special json format.
    """
    def __init__(self, name):
        """
        :param name: parser name
        """
        self.name = name or ""

    def parse(self, data):
        """

        :param data:
        :return:
        """
        if not data:
            return {}

        try:
            result = json.loads(data)
            if result["status"] != 0:
                LOGGER.error("config %s: the status is not good, status is %s",
                             self.name, result["status"])
                return {}

            config = {}
            for _ in result["values"]:
                config[_["key"]] = _["value"]
            return config
        except Exception as err:
            LOGGER.error("config %s: fail to parse threathunter json config, the error is %s",
                         self.name, err)
        return {}

    def dump(self, config):
        result = {
            "status": 0,
            "msg": "OK",
        }
        try:
            result["values"] = [{"key": k, "value": v} for k, v in config.items()]
            return result
        except Exception as err:
            LOGGER.error("config %s: fail to dump config to threathunter json, he error is %s",
                         self.name, err)
            result["values"] = []
        return result
