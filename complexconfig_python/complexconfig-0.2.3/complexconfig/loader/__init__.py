# -*- coding: utf8 -*-
"""
Loader that will load data from specific data source.
"""

from __future__ import print_function, unicode_literals


class BaseLoader(object):
    """
    Base class for config loader.

    Load text from special data source.
    """
    def __init__(self):
        self.last_updated_ts = 0

    def load(self):
        """
        template method for config loading.

        :return:
        """
        raise NotImplementedError
