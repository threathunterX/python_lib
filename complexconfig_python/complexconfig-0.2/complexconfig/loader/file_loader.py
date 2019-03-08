# -*- coding: utf8 -*-

from __future__ import print_function, unicode_literals


import logging

from . import BaseLoader


logger = logging.getLogger("config")


class FileLoader(BaseLoader):
    """
    Load data from web
    """

    def __init__(self, name, file_name):
        self.name = name
        self.file_name = file_name

    def load(self):
        try:
            with open(self.file_name) as of:
                return of.read()
        except Exception as err:
            logger.error("config %s: fail to load file config, file is %s, err is %s", self.name, self.file_name, err)
            return ""

