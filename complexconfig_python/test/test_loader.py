#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import requests
import pytest
import mock

from complexconfig.loader.string_loader import StringLoader
from complexconfig.loader.web_loader import WebLoader
from complexconfig.loader.pyfile_loader import PyfileLoader
from complexconfig.loader.file_loader import FileLoader

from mock_utils import RequestMock


@pytest.fixture
def config_value():
    return """redis.host=1111"""


def test_file_loader(tmpdir, config_value):
    test_file = tmpdir.mkdir("sub").join("test.py")
    test_file.write(config_value)
    loader = FileLoader("file_loader", str(test_file))
    assert loader.load() == config_value


def test_string_loader(config_value):
    loader = StringLoader("str_loader", config_value)
    assert loader.load() == config_value


def test_pyfile_loader(tmpdir):
    test_file = tmpdir.mkdir("sub").join("test.py")
    test_file.write("test=1")
    loader = PyfileLoader("py_loader", str(test_file))
    config = loader.load()
    assert json.loads(config) == {"test": 1}


@mock.patch('requests.get', mock.Mock(side_effect=RequestMock(config_value()).request_mock))
def test_web_loader():
    config = config_value()

    def login():
        result = requests.get("login_url", allow_redirects=False)
        assert result.status_code == 200
        return {"params": json.loads(result.text)}

    loader = WebLoader("web_loader", "config_url", login)
    assert loader.load() == config
