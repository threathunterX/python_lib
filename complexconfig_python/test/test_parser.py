#!/usr/bin/env python
# -*- coding: utf-8 -*-


from complexconfig.parser.ini_parser import IniParser
from complexconfig.parser.json_parser import JsonParser
from complexconfig.parser.threathunter_json_parser import ThreathunterJsonParser
from complexconfig.parser.properties_parser import PropertiesParser
from complexconfig.parser.yaml_parser import YamlParser
from complexconfig.parser.dict_parser import DictParser


def test_ini_parser():
    parser = IniParser("ini_parser")
    result = parser.parse("""[test]
int_key = 1
str_key = test
    """)
    assert result["test"]["int_key"] == '1'
    assert result["test"]["str_key"] == "test"


def test_json_parser():
    parser = JsonParser("json_parser")
    result = parser.parse("""{"int_key": 1, "str_key": "test"}
    """)
    assert result["int_key"] == 1
    assert result["str_key"] == "test"


def test_threathunter_json_parser():
    parser = ThreathunterJsonParser("threathunter_json_parser")
    result = parser.parse("""{"status": 0, "values": [{"key":"int_key", "value":1}, {"key": "str_key", "value": "test"}]}
    """)
    print parser.dump(result)
    assert result["int_key"] == 1
    assert result["str_key"] == "test"


def test_properties_parser():
    parser = PropertiesParser("properties_parser")
    result = parser.parse("""int_key=1
str_key=test
    """)
    assert result["int_key"] == "1"
    assert result["str_key"] == "test"


def test_properties_comment():
    parser = PropertiesParser("properties_parser")
    result = parser.parse("""int_key=1
str_key=test
#comment_key=test
    """)
    assert result["int_key"] == "1"
    assert result["str_key"] == "test"
    assert len(result) == 2


def test_yaml_parser():
    parser = YamlParser("yaml_parser")
    result = parser.parse("""int_key: 1
str_key: test
    """)
    assert result["int_key"] == 1
    assert result["str_key"] == "test"


def test_dict_parser():
    data = {"int_key": 1, "str_key": "test"}
    parser = DictParser()
    result = parser.parse(data)
    assert result["int_key"] == 1
    assert result["str_key"] == "test"
