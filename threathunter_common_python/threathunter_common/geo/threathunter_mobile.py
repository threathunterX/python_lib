#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-26 18:03:42
# @Version : 0.1

import os
import struct
from threathunter_common import util

_unpack_L = lambda b: struct.unpack("<L", b)[0]
_unpack_H = lambda b: struct.unpack(">H", b)[0]

class ThreathunterMobileGeo(object):

    """docstring for ThreathunterMobileGeo"""

    def __init__(self):
        super(ThreathunterMobileGeo, self).__init__()
        self.com_list = []
        self.geo_info = ''

        buff = None
        data_path = os.path.join(os.path.dirname(__file__), 'threathunter_mobile.dat')
        with open(data_path, 'rb') as f:
            buff = f.read()

        phone_len = _unpack_L(buff[:4])

        p_phone_data = buff[4:4+phone_len*12]
        for i in range(phone_len):
            start_phone, end_phone, geo_offset, geo_len = struct.unpack(
                'IIHH', p_phone_data[i*12:(i+1)*12])
            self.com_list.append([start_phone, end_phone, geo_offset, geo_len])

        self.geo_info = buff[4+phone_len*12:]

    def find(self, phone_number):
        phone_number = int(phone_number[:-4])
        low, high = 0, len(self.com_list)
        while low < high:
            mid = (low+high)//2
            if self.com_list[mid][0] < phone_number and self.com_list[mid][1] < phone_number:
                low = mid+1
            elif self.com_list[mid][0] > phone_number and self.com_list[mid][1] > phone_number:
                high = mid
            else:
                offset = self.com_list[mid][-2]
                info_len = self.com_list[mid][-1]
                info = self.geo_info[offset:offset+info_len]
                return u"中国 "+info.decode('utf-8')


instance = ThreathunterMobileGeo()


def find(phone_number):
    global instance
    if not util.mobile_match(str(phone_number)):
        return ''

    info = instance.find(str(phone_number))
    if info is None:
        return ''
    else:
        return info


if __name__ == '__main__':
    print(find(17155552525))
