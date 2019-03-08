# -*- coding: utf8 -*-

import time
import json
import pytest
import mock
import requests
from threathunter_common.util import run_in_thread

from complexconfig.parser.properties_parser import PropertiesParser
from complexconfig.loader.file_loader import FileLoader
from complexconfig.loader.web_loader import WebLoader
from complexconfig.config import Config
from complexconfig.config import PeriodicalConfig
from complexconfig.config import CascadingConfig
from complexconfig.config import PrefixConfig

from mock_utils import RequestMock


@pytest.fixture
def tmp_file(tmpdir):
    temp_file = tmpdir.mkdir("sub").join("test.properties")
    return temp_file


@pytest.fixture
def config():
    return """
a=1
b=test
c=yes
int_key=1
str_key=aa
list_key=1,2,3
boolean_true_key=yes
boolean_false_key=No
only_in_config=2
    """


@pytest.fixture
def another_config():
    return """
a=2
b=test2
ca=no
int_key=10
str_key=test
list_key=a,b,c
boolean_true_key=true
boolean_false_key=false
    """


def test_simple_config(tmp_file, config):
    tmp_file.write(config)
    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    c = Config(loader, parser)
    c.load_config(sync=True)
    assert c.get_value("a") == "1"
    assert c.get_value("b") == "test"

    assert c.get_int("a") == 1
    assert c.get_string("a") == "1"
    assert c.get_boolean("c") == True


def test_simple_config_in_period(tmp_file, config, another_config):
    tmp_file.write(config)

    def async_update():
        time.sleep(2)
        tmp_file.write(another_config)

    run_in_thread(async_update)
    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    c = Config(loader, parser)
    c.load_config(sync=True)

    # first time
    assert c.get_value("a") == "1"
    assert c.get_value("b") == "test"

    assert c.get_int("a") == 1
    assert c.get_string("a") == "1"
    assert c.get_boolean("c")

    time.sleep(5)

    # second time
    assert c.get_value("a") == "1"
    assert c.get_value("b") == "test"

    assert c.get_int("a") == 1
    assert c.get_string("a") == "1"
    assert c.get_boolean("c")


def test_periodical_config_in_period(tmp_file, config, another_config):
    tmp_file.write(config)

    def async_update():
        time.sleep(2)
        tmp_file.write(another_config)

    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    c = Config(loader, parser)
    pc = PeriodicalConfig(c, 3)
    pc.load_config(sync=True)
    run_in_thread(async_update)
    # first time
    assert pc.get_value("a") == "1"
    assert pc.get_value("b") == "test"

    assert pc.get_int("a") == 1
    assert pc.get_string("a") == "1"
    assert pc.get_boolean("c")

    time.sleep(5)

    # second time
    pc.get_value("a")
    time.sleep(1)
    assert pc.get_value("a") == "2"
    assert pc.get_value("b") == "test2"

    assert pc.get_int("a") == 2
    assert pc.get_string("a") == "2"
    assert not pc.get_boolean("c")


def test_int_config_item_in_period(tmp_file, config, another_config):
    tmp_file.write(config)

    def async_update():
        time.sleep(2)
        tmp_file.write(another_config)

    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    c = Config(loader, parser)
    pc = PeriodicalConfig(c, 3)
    item = pc.int_item("a", caching=1)
    pc.load_config(sync=True)

    run_in_thread(async_update)
    # first time
    assert pc.get_value("a") == "1"
    assert item.get() == 1

    time.sleep(5)

    # second time
    pc.get_value("a")  # trigger update
    time.sleep(1)
    assert pc.get_value("a") == "2"
    assert item.get() == 2


def test_callback_config_item_in_period(tmp_file, config, another_config):
    tmp_file.write(config)

    def async_update():
        time.sleep(2)
        tmp_file.write(another_config)

    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    c = Config(loader, parser)
    pc = PeriodicalConfig(c, 3)
    item = pc.item("a", caching=1, cb_load=lambda x: int(x)+1)
    pc.load_config(sync=True)

    run_in_thread(async_update)
    # first time
    assert pc.get_value("a") == "1"
    assert item.get() == 2

    time.sleep(5)

    # second time
    pc.get_value("a")  # trigger update
    time.sleep(1)
    assert pc.get_value("a") == "2"
    assert item.get() == 3


def test_config_item_helper_methods(tmp_file, config, another_config):
    tmp_file.write(config)

    def async_update():
        time.sleep(2)
        tmp_file.write(another_config)

    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    c = Config(loader, parser)
    pc = PeriodicalConfig(c, 3)
    int_item = pc.int_item("int_key", caching=1)
    str_item = pc.str_item("str_key", caching=1)
    list_item = pc.list_item("list_key", caching=1)
    boolean_true_item = pc.boolean_item("boolean_true_key")
    boolean_false_item = pc.boolean_item("boolean_false_key")
    pc.load_config(sync=True)

    # first batch of assert
    assert int_item.get() == 1
    assert str_item.get() == "aa"
    assert list(list_item.get()) == list(["1", "2", "3"])
    assert boolean_true_item.get()
    assert not boolean_false_item.get()

    run_in_thread(async_update)

    time.sleep(5)
    pc.get_value("a")  # trigger update
    time.sleep(1)

    assert int_item.get() == 10
    assert str_item.get() == "test"
    assert list(list_item.get()) == list(["a", "b", "c"])
    assert boolean_true_item.get()
    assert not boolean_false_item.get()


def test_config_helper_methods(tmp_file, config):
    tmp_file.write(config)
    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    c = Config(loader, parser)
    c.load_config(sync=True)
    assert c.get_boolean("boolean_true_key")
    assert not c.get_boolean("boolean_false_key")
    assert c.get_int("int_key")
    assert c.get_list("list_key") == ["1", "2", "3"]


@mock.patch('requests.get', mock.Mock(side_effect=RequestMock(another_config()).request_mock))
def test_cascading_config(tmp_file, config):
    # build web config
    def login():
        result = requests.get("login_url", allow_redirects=False)
        assert result.status_code == 200
        return {"params": json.loads(result.text)}

    loader = WebLoader("web_loader", "config_url", login)
    parser = PropertiesParser("parser")
    web_config = Config(loader, parser)
    web_config.load_config(sync=True)

    # build file config
    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    file_config = Config(loader, parser)
    file_config.load_config(sync=True)

    # build cascading config
    cascading_config = CascadingConfig(file_config, web_config)

    cascading_config.get_int("a")

    assert file_config.get_int("a") == 1
    assert web_config.get_int("a") == 2
    assert cascading_config.get_int("a") == 2

    assert file_config.get_string("str_key") == "aa"
    assert web_config.get_string("str_key") == "test"
    assert cascading_config.get_string("str_key") == "test"

    assert file_config.get_value("only_in_config") == "2"
    assert web_config.get_value("only_in_config") is None
    assert cascading_config.get_value("only_in_config") == "2"


def test_prefix_config(tmp_file, config):
    tmp_file.write(config)

    loader = FileLoader("loader", str(tmp_file))
    parser = PropertiesParser("parser")
    tmp_file.write(config)
    # 增加前缀
    c = Config(loader, parser, cb_after_load=lambda x: {'prefix': x})
    c.load_config(sync=True)
    # first time
    assert c.get_value("a") is None
    assert c.get_value("prefix.a") == "1"
    assert c.get_value("prefix.b") == "test"
    assert c.get_int("prefix.a") == 1
    assert c.get_string("prefix.a") == "1"
    assert c.get_boolean("prefix.c")

    prefix_config = PrefixConfig(c, prefix='prefix')
    assert prefix_config.get_value("a") == "1"
    assert prefix_config.get_value("b") == "test"
    assert prefix_config.get_int("a") == 1
    assert prefix_config.get_string("a") == "1"
    assert prefix_config.get_boolean("c")


def test_dumb():
    pass
