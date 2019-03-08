# -*- coding: utf8 -*-

from __future__ import print_function, unicode_literals

import logging

# code from project kaptan
try:
    import ConfigParser as configparser
    from StringIO import StringIO

    configparser.RawConfigParser.read_file = configparser.RawConfigParser.readfp
except ImportError:  # Python 3
    import configparser
    from io import StringIO

from . import BaseParser

logger = logging.getLogger("config")


class KaptanIniParser(configparser.RawConfigParser):

    def as_dict(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d


class IniParser(BaseParser):

    def __init__(self, name):
        """
        :param name: parser name
        """
        self.name = name

    def parse(self, data):
        """
        parse text in ini format to python dict
        :param value: text in ini format
        """
        config = KaptanIniParser()
        try:
            # ConfigParser.ConfigParser wants to read value as file / IO
            config.read_file(StringIO(data))
            return config.as_dict()
        except Exception as err:
            logger.error("config %s: fail to parse ini config, the error is %s", self.name, err)
            return {}

    def dump(self, config):
        """
        not implement for ini format
        """
        raise NotImplementedError("Exporting .ini format is not supported.")
