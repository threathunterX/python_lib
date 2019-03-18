#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "nebula"


class RabbitmqCtx(object):
    _instance = None

    def __init__(self):
        self._amqp_url = "amqp://guest:guest@localhost:5672/"

    @property
    def amqp_url(self):
        return self._amqp_url

    @amqp_url.setter
    def amqp_url(self, amqp_url):
        self._amqp_url = amqp_url

    @staticmethod
    def get_instance():
        if not RabbitmqCtx._instance:
            RabbitmqCtx._instance = RabbitmqCtx()
        return RabbitmqCtx._instance

    def __str__(self):
        return "RabbitmqCtx[amqp_url={}]".format(self._amqp_url)
