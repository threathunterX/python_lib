# -*- encoding: utf-8 -*-
from __future__ import absolute_import

import codecs
import os

import six

from ..util import identity_card_match

data_id = {}
data_short_id = {}


def _get_data_source():
    global data_id
    data_path = os.path.join(os.path.dirname(__file__), 'id.txt')
    with codecs.open(data_path, 'r') as f:
        for line in f.readlines():
            tmp = line.split(' ')
            if six.PY2:
                if len(tmp[0]) == 2:
                    data_short_id[tmp[0]] = unicode(tmp[1].decode('utf-8'))
                else:
                    data_id[tmp[0]] = unicode(tmp[1].decode('utf-8'))
            else:
                if len(tmp[0]) == 2:
                    data_short_id[tmp[0]] = tmp[1]
                else:
                    data_id[tmp[0]] = tmp[1]


_get_data_source()


def find_source(id_number):
    if identity_card_match(id_number):
        if 2 <= len(id_number) < 6:
            return data_short_id[id_number[:2]]
        elif 6 <= len(id_number) <= 18:
            return data_id[id_number[:6]]
        else:
            return ['', '', '']
