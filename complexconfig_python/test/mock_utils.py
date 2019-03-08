#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Result(object):
    pass


class RequestMock(object):
    def __init__(self, content):
        self.content = content

    def request_mock(self, url, *args, **kwargs):
        result = Result()
        if url == "config_url":
            if kwargs.get("params", {}).get("auth") == "1234":
                setattr(result, "text", self.content)
                setattr(result, "status_code", 200)
            else:
                setattr(result, "text", "")
                setattr(result, "status_code", 401)
            return result
        if url == "login_url":
            setattr(result, "text", '{"auth":"1234"}')
            setattr(result, "status_code", 200)
            return result

        setattr(result, "text", "unhandled request %s" % url)
        setattr(result, "status_code", 400)

