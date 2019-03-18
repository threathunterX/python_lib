#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import gzip
import json

from .util import text
from threathunter_common.event import Event

__author__ = "nebula"


def extract_data_from_mail(m):
    if not m:
        return None

    type = m.get_header("content-type", "")
    body = m.body
    compressing = m.get_header("compressing", "")

    if "error" == type:
        raise RuntimeError(text(body))

    if compressing:
        if "gzip" == compressing:
            body = gzip.zlib.decompress(str(body))
        else:
            raise RuntimeError("unsupported compressing method")

    if "event" == type:
        text_body = text(body)
        event = Event.from_json(text_body)
        return event
    elif "event[]" == type:
        events = json.loads(text(body))
        events = [Event.from_dict(_) for _ in events]
        return events
    else:
        raise RuntimeError("this body type ({}) is not supported yet".format(type))


def populate_event_into_mail(mail, event):
    if not mail or not event:
        return

    mail.body = event.get_json()
    mail.add_header("content-type", "event")


def populate_event_list_into_mail(mail, events):
    if not mail:
        return

    if events is None:
        events = []

    mail.body = json.dumps([_.get_dict() for _ in events])
    mail.add_header("content-type", "event[]")

    if len(mail.body) > 1000:
        mail.body = gzip.zlib.compress(str(mail.body), 5)
        mail.add_header("compressing", "gzip")


def populate_error_into_mail(mail, error):
    if not mail or not error:
        return

    mail.body = text(error)
    mail.add_header("content-type", "error")


def populate_data_into_mail(mail, data):
    if isinstance(data, Event):
        populate_event_into_mail(mail, data)
    elif isinstance(data, (tuple, list)) and (len(data) == 0 or isinstance(data[0], Event)):
        populate_event_list_into_mail(mail, data)
    elif isinstance(data, (str, unicode)):
        populate_error_into_mail(mail, data)
    else:
        raise RuntimeError("unrecognized payload for mail")


