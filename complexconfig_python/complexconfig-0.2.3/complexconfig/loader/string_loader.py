# -*- coding: utf8 -*-
"""
load config from string
"""

from __future__ import print_function, unicode_literals


from . import BaseLoader


class StringLoader(BaseLoader):
    """
    Load data from string
    """

    def __init__(self, name, config_text):
        super(StringLoader, self).__init__()
        self.name = name
        self.config_text = config_text

    def load(self):
        return self.config_text
