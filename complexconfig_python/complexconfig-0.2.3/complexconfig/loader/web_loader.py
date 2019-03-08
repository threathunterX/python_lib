# -*- coding: utf8 -*-
"""
import config from web
"""

from __future__ import print_function, unicode_literals


import logging
import requests

from . import BaseLoader


LOGGER = logging.getLogger("config")


class WebLoader(BaseLoader):
    """
    Load data from web
    """

    def __init__(self, name, url, login_fn=None, params=None):
        """
        load data from remote url by web request.
        :param name: loader name
        :param url: url of config service
        :param login_fn:
        :return:
        """

        if params is None:
            params = dict()

        super(WebLoader, self).__init__()
        self.name = name
        self.url = url
        self.login_fn = login_fn
        self.params = params

    def load(self):
        if self.url:
            try:
                result = requests.get(self.url, timeout=5, allow_redirects=False,
                                      params=self.params)
            except Exception as err:
                LOGGER.error("config %s: fail to access [%s], the error is %s",
                             self.name, self.url, err)
                return

            if result.status_code == 200:
                return result.text
            elif self.login_fn:
                # try login
                try:
                    login_result = self.login_fn()
                except Exception as err:
                    LOGGER.error("config %s: fail to load web config on [%s], the error is %s",
                                 self.name, self.url, err)
                    return ""

                # try login again
                try:
                    self.params = login_result.get('params', dict())
                    result = requests.get(self.url, timeout=5, allow_redirects=False,
                                          params=self.params)
                except Exception as err:
                    LOGGER.error("config %s: fail to access [%s], the error is %s",
                                 self.name, self.url, err)
                    return

                if result.status_code == 200:
                    return result.text
                else:
                    LOGGER.error("config %s: fail to load web config on [%s], the error is %s",
                                 self.name, self.url, result.status_code)
                    return ""
            else:
                LOGGER.error("config %s: fail to load web config on [%s], and no login func",
                             self.name, self.url)
                return ""
        else:
            LOGGER.error("config %s: fail to load web config, no url is given", self.name)
            return ""
