#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-10 19:33:43
# @Version : 0.1

import os
import sys
from io import open
import struct
from threathunter_common.util import identity_card_match

_unpack_L = lambda b: struct.unpack("<L", b)[0]
_unpack_H = lambda b: struct.unpack(">H", b)[0]


class IdentityCard(object):
    """docstring for IdentityCard"""

    def __init__(self):
        super(IdentityCard, self).__init__()
        self.id_card_infos_list = []
        self.geo_info = ''

        buff = None
        data_path = os.path.join(os.path.dirname(__file__), 'identity_card.dat')
        with open(data_path, 'rb') as f:
            buff = f.read()

        id_cards_len = _unpack_L(buff[:4])
        buff = buff[4:]
        self.geo_info = buff[id_cards_len * 17:]

        for i in range(id_cards_len):
            start_id_number, end_id_number, geo_offset, geo_len = struct.unpack('IILB', buff[i * 17:(i + 1) * 17])
            self.id_card_infos_list.append([start_id_number, end_id_number, geo_offset, geo_len])

    def find(self, phone_number):
        phone_number = int(phone_number[:6])
        low, high = 0, len(self.id_card_infos_list)
        while low < high:
            mid = (low + high) // 2
            if self.id_card_infos_list[mid][0] < phone_number and self.id_card_infos_list[mid][1] < phone_number:
                low = mid + 1
            elif self.id_card_infos_list[mid][0] > phone_number and self.id_card_infos_list[mid][1] > phone_number:
                high = mid
            else:
                offset = self.id_card_infos_list[mid][-2]
                info_len = self.id_card_infos_list[mid][-1]
                info = self.geo_info[offset:offset + info_len]
                return u"中国 " + info.decode('utf-8')


instance = IdentityCard()


def find(id_number):
    global instance

    # keep find for compatibility
    if not identity_card_match(id_number):
        return ''
    return instance.find(id_number)

