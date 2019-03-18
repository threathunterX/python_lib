#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import six, json

from ..util import text, millis_now,  unicode_list
from .term import Term

__author__ = "nebula"

class Strategy(object):
    status_list = ["inedit", "test", "online", "outline"]

    def __init__(self, app, name, remark, version, status, create_time, modify_time, \
                 start_effect, end_effect, terms, category, isLock, tags, score, group_id):
        self._app = text(app)
        self._name = text(name)
        self._remark = text(remark)
        if not category:
            raise RuntimeError("invalid category")
        self._category = text(category)
        self._score = int(score or 0)
        self._tags = unicode_list(tags or [])
        self._isLock = bool(isLock)
        self._version = version
        self._status = text(status).lower()
        if self._status not in Strategy.status_list:
            raise RuntimeError("invalid status")
        self._group_id = int(group_id) if isinstance(group_id, int) or isinstance(group_id, long) or (isinstance(group_id, basestring) and group_id.isdigit()) else 0
        self._create_time = long(create_time)
        if self._create_time <= 0:
            self._create_time = millis_now()
        self._modify_time = long(modify_time)
        if self._modify_time <= 0:
            self._modify_time = millis_now()
        self._start_effect = long(start_effect)
        self._end_effect = long(end_effect)

        if isinstance(terms, (six.binary_type, six.text_type)):
            terms = json.loads(text(terms))

        if isinstance(terms, (tuple, list)):
            terms = [Term.from_dict(_) for _ in terms]

        if not terms:
            raise RuntimeError("invalid terms")

        self._terms = terms

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = text(app)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name)

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, category):
        self._category = text(category)

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = unicode_list(tags)

    @property
    def score(self):
        return self._score

    @score.setter
    def score(self, score):
        self._score = int(score)

    @property
    def isLock(self):
        return self._isLock

    @isLock.setter
    def isLock(self, isLock):
        self._isLock = bool(isLock)

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark)

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = long(version)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        if self._status not in Strategy.status_list:
            raise RuntimeError("invalid status")

    @property
    def create_time(self):
        return self._create_time

    @create_time.setter
    def create_time(self, create_time):
        self._create_time = long(create_time)

    @property
    def modify_time(self):
        return self._modify_time

    @modify_time.setter
    def modfify_time(self, modify_time):
        self._modify_time = long(modify_time)

    @property
    def start_effect(self):
        return self._start_effect

    @start_effect.setter
    def start_effect(self, start_effect):
        self._start_effect = long(start_effect)

    @property
    def end_effect(self):
        return self._end_effect

    @end_effect.setter
    def end_effect(self, end_effect):
        self._end_effect = long(end_effect)

    @property
    def group_id(self):
        return self._group_id

    @group_id.setter
    def group_id(self, group_id):
        self._group_id = int(group_id)

    @property
    def terms(self):
        return self._terms

    @terms.setter
    def terms(self, terms):
        if isinstance(terms, (six.text_type, six.text_type)):
            terms = json.loads(text(terms))

        if isinstance(terms, (tuple, list)):
            new_terms = []
            for t in terms:
                if isinstance(t, Term):
                    new_terms.append(t)
                elif isinstance(t, dict):
                    new_terms.append(Term.from_dict(t))
                elif isinstance(t, six.text_type):
                    new_terms.append(Term.from_json(t))
            terms = new_terms

        if not terms:
            raise RuntimeError("invalid terms")
        self._terms = terms

    def get_dict(self):
        return {
            "app": self.app,
            "name": self.name,
            "category": self.category,
            "isLock": self.isLock,
            "tags": self.tags,
            "score": self.score,
            "remark": self.remark,
            "version": self.version,
            "status": self.status,
            "group_id": self.group_id,
            "createtime": self.create_time,
            "modifytime": self.modify_time,
            "starteffect": self.start_effect,
            "endeffect": self.end_effect,
            "terms": [_.get_dict() for _ in self.terms]
        }

    def get_json(self):
        return json.dumps(self.get_dict())

    @staticmethod
    def from_dict(d):
        return Strategy(
            d.get("app"), d.get("name", "").strip(), d.get("remark"), d.get("version"), \
            d.get("status"), d.get("createtime"), d.get("modifytime"), \
            d.get("starteffect"), d.get("endeffect"), d.get("terms"), \
            d.get("category"), d.get("isLock"), d.get("tags"), d.get("score"), \
            d.get("group_id"))

    @staticmethod
    def from_json(json_str):
        return Strategy.from_dict(json.loads(json_str))

    def copy(self):
        return Strategy.from_dict(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Strategy[{}]".format(self.get_dict())


