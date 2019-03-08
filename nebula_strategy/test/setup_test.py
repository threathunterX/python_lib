#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from nebula_meta.event_model import EventModel, add_event_to_registry
from nebula_meta.variable_model import VariableModel, add_variable_to_registry


def get_local_path(file_name):
    import os
    return os.path.join(os.path.dirname(__file__), file_name)


def get_file_json_content(file_name):
    with file(get_local_path(file_name)) as input_file:
        content = input_file.read()
        return json.loads(content)


def load_config():
    print ("loading config")
    for _ in get_file_json_content('data/event_model_default.json'):
        add_event_to_registry(EventModel.from_dict(_))

    for _ in get_file_json_content('data/common_variable_default.json'):
        add_variable_to_registry(VariableModel.from_dict(_))
    for _ in get_file_json_content('data/realtime_variable_default.json'):
        add_variable_to_registry(VariableModel.from_dict(_))
    for _ in get_file_json_content('data/profile_variable_default.json'):
        add_variable_to_registry(VariableModel.from_dict(_))


def load_strategy_content(filename='data/strategy.json'):
    with open(get_local_path(filename)) as strategy:
        return strategy.read()

