#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
from collections import OrderedDict

from .util import text

from .value_type import is_value_type_supported


class Property(object):
    def __init__(self, name, type, subtype, visible_name, remark):
        self._name = text(name or '')
        self._type = text(type or '')
        self._subtype = text(subtype or '')
        self._visible_name = text(visible_name or '')
        self._remark = text(remark or '')

    def sanity_check(self):
        if not self.name:
            raise RuntimeError('属性缺乏名称定义')

        if not is_value_type_supported(self.type, self.subtype):
            raise RuntimeError('数据类型({}, {})不支持'.format(self.type, self.subtype))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name or '')

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type or '')

    @property
    def subtype(self):
        return self._subtype

    @subtype.setter
    def subtype(self, subtype):
        self._subtype = text(subtype or '')

    @property
    def visible_name(self):
        return self._visible_name

    @visible_name.setter
    def visible_name(self, visible_name):
        self._visible_name = text(visible_name or '')

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark)

    def get_dict(self):
        return {'name': self.name, 'type': self.type, 'subtype': self.subtype, 'visible_name': self.visible_name,
                'remark': self.remark}

    def get_ordered_dict(self):
        return OrderedDict([
            ('name', self.name),
            ('type', self.type),
            ('subtype', self.subtype),
            ('visible_name', self.visible_name),
            ('remark', self.remark),
        ])

    def get_simplified_ordered_dict(self):
        tuples = list()
        tuples.append(('name', self.name))
        tuples.append(('type', self.type))
        if self.subtype:
            tuples.append(('subtype', self.subtype))
        tuples.append(('visible_name', self.visible_name))
        tuples.append(('remark', self.remark))
        return OrderedDict(tuples)

    def get_json(self):
        return json.dumps(self.get_ordered_dict())

    @staticmethod
    def from_dict(d):
        if not d:
            return None

        if not isinstance(d, dict):
            raise RuntimeError('非法的配置')
        return Property(d.get('name'), d.get('type'), d.get('subtype', ''), d.get('visible_name', ''),
                        d.get('remark', ''))

    @staticmethod
    def from_json(json_str):
        return Property.from_dict(json.loads(json_str))

    def copy(self):
        return Property.from_dict(self.get_dict())

    def __str__(self):
        return "Property[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other
