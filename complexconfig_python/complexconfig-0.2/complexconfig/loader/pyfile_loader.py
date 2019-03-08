# -*- coding: utf8 -*-

from __future__ import print_function, unicode_literals


import os
import sys
import json
import logging

from . import BaseLoader

logger = logging.getLogger("config")

# from kaptan
def import_pyfile(pathname, mod_name=''):
    """Import the contents of filepath as a Python module.

    Parameters
    ----------
    pathname: str
        Path to the .py file to be imported as a module

    mod_name: str
        Name of the module when imported

    Returns
    -------
    mod
        The imported module

    Raises
    ------
    IOError
        If file is not found
    """
    if not os.path.isfile(pathname):
        raise IOError('File {0} not found.'.format(pathname))

    if sys.version_info[0] == 3 and sys.version_info[1] > 2: # Python >= 3.3
        import importlib.machinery
        loader = importlib.machinery.SourceFileLoader('', pathname)
        mod = loader.load_module(mod_name)
    else: #  2.6 >= Python <= 3.2
        import imp
        mod = imp.load_source(mod_name, pathname)
    return mod


class PyfileLoader(BaseLoader):
    """
    Load data from python source file
    """

    def __init__(self, name, filename):
        """
        load data from python source file.
        :return:
        """
        self.name = name
        self.filename = filename

    def load(self):
        try:
            module = import_pyfile(self.filename)
            data = dict()
            for key in dir(module):
                value = getattr(module, key)
                if not key.startswith("__"):
                    data.update({key: value})
            # first parse it, and then return json format to keep unified interface
            return json.dumps(data)
        except Exception as err:
            logger.error("config %s: fail to load file config, file is %s, err is %s", self.name, self.filename, err)
            return ""
