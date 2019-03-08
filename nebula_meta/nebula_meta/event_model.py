#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .property_model import *
from .util import *


class EventModel(object):

    def __init__(self, app, name, visible_name, type, remark, source, version, properties):
        self._app = text(app or '')
        self._name = text(name or '')
        self._visible_name = text(visible_name or '')
        self._type = text(type or '')
        self._remark = text(remark or '')
        self._source = unicode_list(source or list())
        self._version = text(version or '')
        self._properties = self._minify_properties([Property.from_dict(_) for _ in properties])

    def _extend_properties(self, properties):
        """
        根据配置的Properties和来自父事件的properties，生成最终的properties列表
        :return:
        """

        if not self.source:
            return properties

        first_source = self.source[0]
        first_source_event = get_event_from_registry(first_source['app'], first_source['name'])
        if not first_source_event:
            raise RuntimeError('事件({})未定义'.format(first_source))

        source_properties = first_source_event.properties
        self_names = [_.name for _ in properties]

        result = list()

        # 将父事件中不同名的property加进来
        for _ in source_properties:
            if _.name not in self_names:
                result.append(_)

        result.extend(properties)
        return result

    def _minify_properties(self, properties):
        """
        生成简化的properties列表，如果父事件有相应的的property，则去掉
        :param properties: 原始的属性列表
        :return: 最小化的属性列表
        """

        if self.source:
            source_event = get_event_from_registry(self.source[0]['app'], self.source[0]['name'])
            if not source_event:
                raise RuntimeError('事件{}未定义'.format(self.source[0]['name']))
            source_properties_dict = {_.name: _ for _ in source_event.properties}
        else:
            source_properties_dict = dict()

        # 只留两种，一种是该名称的字段父事件压根没有；另一种是虽然有同名字段，但有少许差别
        result = [_ for _ in properties if _.name not in source_properties_dict
                      or source_properties_dict[_.name] != _]
        return result

    def sanity_check(self):
        if not self.name:
            raise RuntimeError('事件名称未定义')

        if self.properties is None or not isinstance(self.properties, list):
            raise RuntimeError('属性列表值不正确')

        for property in self.properties:
            if property is None:
                raise RuntimeError('非法的属性配置')
            property.sanity_check()

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = text(app or '')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name or '')

    @property
    def visible_name(self):
        return self._visible_name

    @visible_name.setter
    def visible_name(self, visible_name):
        self._visible_name = text(visible_name or '')

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type or '')

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or '')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        source = unicode_list(source or list())
        self._source = self._extend_properties([Property.from_dict(_) for _ in source])

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = text(version or '')

    @property
    def properties(self):
        return self._extend_properties(self._properties)

    @property
    def simplified_properties(self):
        return self._properties

    @property
    def properties_dict(self):
        return [_.get_dict() for _ in self.properties]

    @properties.setter
    def properties(self, properties):
        self._properties = self._minify_properties(
            [_ if isinstance(_, Property) else Property.from_dict(_) for _ in properties])

    def get_fields_mapping(self):
        """
        推导event每个字段的数据类型

        :return:
        """

        result = dict()
        for p in self.properties:
            result[p.name] = (p.type, p.subtype)

        # add 固定的id/pid/timestamp
        result['id'] = ('string', '')
        result['pid'] = ('string', '')
        result['timestamp'] = ('long', '')
        return result

    def get_dict(self):
        return {
            "app": self.app,
            "name": self.name,
            "visible_name": self.visible_name,
            "type": self.type,
            "version": self.version,
            "properties": self.properties_dict,
            "remark": self.remark,
            "source": self.source
        }

    def get_ordered_dict(self):
        return OrderedDict([
            ('app', self.app),
            ('name', self.name),
            ('visible_name', self.visible_name),
            ('remark', self.remark),
            ('type', self.type),
            ('version', self.version),
            ('source', self.source),
            ('properties', [_.get_ordered_dict() for _ in self.properties]),  # 拿全量的数据
        ])

    def get_simplified_ordered_dict(self):
        return OrderedDict([
            ('app', self.app),
            ('name', self.name),
            ('visible_name', self.visible_name),
            ('remark', self.remark),
            ('type', self.type),
            ('version', self.version),
            ('source', self.source),
            ('properties', [_.get_simplified_ordered_dict() for _ in self._properties])  # 只有少量数据
        ])

    def get_json(self):
        return json.dumps(self.get_ordered_dict())

    @staticmethod
    def from_dict(d):
        if not d:
            return None
        return EventModel(d.get('app'), d.get('name'), d.get('visible_name', ''), d.get('type'), d.get('remark', ''),
                          d.get('source', list()), d.get('version'), d.get('properties', list()))

    @staticmethod
    def from_json(json_str):
        return EventModel.from_dict(json.loads(json_str))

    def copy(self):
        return EventModel.from_dict(self.get_dict())

    def __str__(self):
        return "EventModel[{}]".format(self.get_dict())

    def __repr__(self):
        return "EventModel[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other


Global_Event_Registry = dict()
Global_Event_Fields_Mapping_Registry = dict()


def add_event_to_registry(event):
    Global_Event_Registry[(event.app, event.name)] = event
    Global_Event_Fields_Mapping_Registry[(event.app, event.name)] = event.get_fields_mapping()


def add_events_to_registry(events):
    for event in events:
        add_event_to_registry(event)


def update_events_to_registry(events):
    new_events_registry = dict()
    new_events_fields_mapping_registry = dict()
    for event in events:
        new_events_registry[(event.app, event.name)] = event
        new_events_fields_mapping_registry[(event.app, event.name)] = event.get_fields_mapping()

    global Global_Event_Registry, Global_Event_Fields_Mapping_Registry
    Global_Event_Registry = new_events_registry
    Global_Event_Fields_Mapping_Registry = new_events_fields_mapping_registry


def get_event_from_registry(event_app, event_name):
    return Global_Event_Registry.get((event_app, event_name))


def get_event_fields_mapping_from_registry(event_app, event_name):
    return Global_Event_Fields_Mapping_Registry.get((event_app, event_name))


def get_all_events():
    return Global_Event_Registry.values()


def get_all_events_in_dict():
    return dict(Global_Event_Registry)


def sort_event_models(event_models):
    """
    对所有的event进行排序，按照下面的顺序
    1. 无source的优先级最高
    2. source event在先
    3. 同顺序的event，按照名字排序
    :param event_models:
    :return: sorted event_models
    """

    if not event_models:
        return list()

    # 1. 初始化权重列表
    weight_dict = {_.name: 1 for _ in event_models}

    # 2. 迭代，将衍生event值抬高，直到无变化
    while True:
        modify_in_iteration = False
        for ev in event_models:
            if not ev.source:
                continue

            source_weights = [weight_dict[_['name']] for _ in ev.source]
            max_source_weight = max(source_weights)
            if weight_dict[ev.name] > max_source_weight:
                pass
            else:
                weight_dict[ev.name] = max_source_weight + 1
                modify_in_iteration = True

        if not modify_in_iteration:
            break

    def sort_event_cmp(left, right):
        return cmp(weight_dict[left.name], weight_dict[right.name]) or cmp(left.name, right.name)

    event_models.sort(cmp=sort_event_cmp)
    return event_models


