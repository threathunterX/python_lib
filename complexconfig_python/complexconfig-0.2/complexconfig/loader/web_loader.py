# -*- coding: utf8 -*-

from __future__ import print_function, unicode_literals


import requests
import logging

from . import BaseLoader


logger = logging.getLogger("config")


class WebLoader(BaseLoader):
    """
    Load data from web
    """

    def __init__(self, name, url, login_fn=None, params={}):
        """
        load data from remote url by web request.
        :param name: loader name
        :param url: url of config service
        :param login_fn:
        :return:
        """
        self.name = name
        self.url = url
        self.login_fn = login_fn
        self.params = params

    def load(self):
        if self.url:
            try:
                result = requests.get(self.url, timeout=5, allow_redirects=False, params=self.params)
            except Exception as err:
                logger.error("config %s: fail to access [%s], the error is %s", self.name, self.url, err)
                return

            if result.status_code == 200:
                return result.text
            elif self.login_fn:
                # try login
                try:
                    login_result = self.login_fn()
                except Exception as err:
                    logger.error("config %s: fail to load web config on [%s], the error is %s", self.name, self.url,
                                 err)
                    return ""

                #try login again
                try:
                    self.params = login_result.get('params', dict())
                    result = requests.get(self.url, timeout=5, allow_redirects=False, params=self.params)
                except Exception as err:
                    logger.error("config %s: fail to access [%s], the error is %s", self.name, self.url, err)
                    return

                if result.status_code == 200:
                    return result.text
                else:
                    logger.error("config %s: fail to load web config on [%s], the error is %s", self.name, self.url,
                                 result.status_code)
                    return ""
            else:
                logger.error("config %s: fail to load web config on [%s], and no login func", self.name, self.url,
                             result.status_code)
                return ""
        else:
            logger.error("config %s: fail to load web config, no url is given", self.name)
            return ""
