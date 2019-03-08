# -*- coding: utf8 -*-
"""
Load config from file.

"""

from __future__ import print_function, unicode_literals


import logging

from . import BaseLoader


LOGGER = logging.getLogger("config")


class FileLoader(BaseLoader):
    """
    Load data from web
    """

    def __init__(self, name, file_name):
        super(FileLoader, self).__init__()
        self.name = name
        self.file_name = file_name

    def load(self):
        """
        load file for configuration
        """

        try:
            with open(self.file_name) as input_file:
                return input_file.read()
        except Exception as err:
            LOGGER.error("config %s: fail to load file config, file is %s, err is %s", self.name,
                         self.file_name, err)
            return ""
