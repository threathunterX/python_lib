# -*- coding: utf8 -*-

from __future__ import print_function, unicode_literals

import yaml

from . import BaseParser


class YamlParser(BaseParser):
    """
    Parse config from yaml content
    """

    def __init__(self, name):
        self.name = name

    def parse(self, data):
        return yaml.load(data)

    def dump(self, data, safe=False, **kwargs):
        if not safe:
            return yaml.dump(data, **kwargs)
        else:
            return yaml.safe_dump(data, **kwargs)
