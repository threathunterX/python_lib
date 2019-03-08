# -*- coding: utf8 -*-
"""
Parse config in dict format
"""

from __future__ import print_function, unicode_literals


from . import BaseParser


class DictParser(BaseParser):
    """
    Parse config from dictionary.
    This is a specific parser which parse code from python dictionary instead of text
    """

    def parse(self, data):
        return data

    def dump(self, config):
        return config
