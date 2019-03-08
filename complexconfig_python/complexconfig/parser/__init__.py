# -*- coding: utf8 -*-
"""
Parsers that parse config in different formats.
"""

from __future__ import print_function, unicode_literals


class BaseParser(object):
    """Base class for config parser."""

    def parse(self, data):
        """
        Parse config from text.
        :param data: the text
        :return: config as dict
        """
        raise NotImplementedError

    def dump(self, config):
        """
        Dump to text from config
        :param config: config as dict
        :return: text
        """
        raise NotImplementedError
