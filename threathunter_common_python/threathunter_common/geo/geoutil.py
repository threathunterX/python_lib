#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import io
import os
import six
from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from . import threathunter_ip
from ..util import unicode_dict
from . import threathunter_identity_card
from . import threathunter_mobile

__author__ = "nebula"
__all__ = ["get_ip_location", "get_ip_geo_data", "get_ip_cn_city", "get_ip_cn_province", "get_ip_cn_province_city",
           "get_phone_location",
           "get_phone_cn_city", "get_phone_cn_province", "get_phone_cn_province_city", "get_cities_in_china",
           "get_provinces_in_china"]

"""
geo util return normalized location info for ip/mobile.
"""

yaml_file = os.path.join(os.path.dirname(__file__), "cities.yml")

# city detail data from yaml file
city_info = {}
_provinces = []
_cities = []


def load_config(filename=yaml_file):
    global city_info, _provinces, _cities
    if six.PY3:
        city_info = load(io.open(filename), Loader=Loader)
    else:
        city_info = load(file(yaml_file), Loader=Loader)

    city_info = unicode_dict(city_info or dict())

    _provinces = []
    _cities = []
    for province in city_info.values():
        _provinces.append(province[u"名称"])
        if u"城市" not in province:
            continue
        for city in province[u"城市"].values():
            if city not in _cities:
                _cities.append(city[u"名称"])
    # warm up the ip geo lib
    threathunter_ip.find("1.1.1.1")


load_config()


def _get_city_info(rawdata, city_info_dict):
    if not rawdata or not city_info_dict:
        return None

    if len(rawdata) < 2:
        return None

    for i in range(0, len(rawdata) - 1):
        key = rawdata[i:i + 2]
        value = city_info_dict.get(key)
        if value:
            return value[u"名称"]

    return None


def _get_geo_data(rawdata):
    """
    Get normalized location for the result from outernal ip/phone lib.

    For addresses in china, we try to return normalized [country, province, city],
    For addresses not in china, we just return [rawdata]
    """

    if rawdata.strip():
        raw_list = rawdata.split('\t')
        if len(raw_list) == 3:
            # 国家省份城市
            return [raw_list[0].strip(), raw_list[1].strip(), raw_list[2].strip()]
        elif len(raw_list) == 2:
            return [raw_list[0].strip(), raw_list[1].strip(), u'']
        elif len(raw_list) == 1:
            return [raw_list[0].strip(), u'', u'']
    else:
        return [u'', u'', u'']


def _get_location(geo_data):
    return " ".join(filter(lambda x: x, geo_data))


def _get_geo_cn_province_city(geo_data):
    if geo_data and isinstance(geo_data, list) and geo_data[0] == u"中国":
        return geo_data[1:3]
    else:
        return None


def _get_geo_cn_province(geo_data):
    if geo_data and isinstance(geo_data, list) and geo_data[0] == u"中国":
        return geo_data[1]
    else:
        return None


def _get_geo_cn_city(geo_data):
    if geo_data and isinstance(geo_data, list) and geo_data[0] == u"中国":
        return geo_data[2]
    else:
        return None


def _get_ip_geo_data(ip):
    rawdata = threathunter_ip.find(ip)
    if rawdata.strip():
        raw_list = rawdata.split('\t')
        if len(raw_list) == 3:
            # 国家省份城市
            return [raw_list[0].strip(), raw_list[1].strip(), raw_list[2].strip()]
        elif len(raw_list) == 2:
            return [raw_list[0].strip(), raw_list[1].strip(), u'']
        elif len(raw_list) == 1:
            return [raw_list[0].strip(), u'', u'']
        else:
            return [u'未知', u'未知', u'未知']
    else:
        return [u'未知', u'未知', u'未知']


def get_ip_geo_data(ip):
    return _get_ip_geo_data(ip)


def get_ip_location(ip):
    return _get_location(_get_ip_geo_data(ip))


def get_ip_cn_province(ip):
    return _get_geo_cn_province(_get_ip_geo_data(ip))


def get_ip_cn_city(ip):
    return _get_geo_cn_city(_get_ip_geo_data(ip))


def get_ip_cn_province_city(ip):
    return _get_geo_cn_province_city(_get_ip_geo_data(ip))


def _get_phone_geo_data(number):
    number = str(number)
    rawdata = threathunter_mobile.find(number)
    if rawdata[:2] != u"中国":
        return [rawdata.strip(), "", ""]

    # deal with the normalization in china
    country = u"中国"
    rawdata = rawdata[2:]
    rawdata = rawdata.replace(" ", "")
    if ',' in rawdata:
        rawdata = rawdata.replace(',', "")
    if len(rawdata) < 2:
        return [country, "", ""]

    province_key = rawdata[:2]
    province_info = city_info.get(province_key)
    if not province_info:
        return [country, "", ""]

    province = province_info.get(u"名称")

    city_info_dict = province_info.get(u"城市") or dict()
    city = ""
    if city_info_dict:
        city = _get_city_info(rawdata[2:], city_info_dict)

    if city:
        return [country, province, city]
    else:
        return [country, province, province]


def get_phone_geo_data(number):
    return _get_phone_geo_data(number)


def get_phone_location(number):
    return _get_location(_get_phone_geo_data(number))


def get_phone_cn_province(number):
    return _get_geo_cn_province(_get_phone_geo_data(number))


def get_phone_cn_city(number):
    return _get_geo_cn_city(_get_phone_geo_data(number))


def get_phone_cn_province_city(number):
    return _get_geo_cn_province_city(_get_phone_geo_data(number))


def get_cities_in_china():
    return _cities


def get_provinces_in_china():
    return _provinces


def _get_identity_card_geo_data(id_number):
    # bug fixed on 2017-04-05 am
    # make sure variable loc in a right scope
    loc = None
    # end

    try:
        loc = threathunter_identity_card.find(id_number)
    except KeyError:
        return [u'', u'', u'']
    if loc and loc.strip():
        raw_list = loc.split(' ')
        if len(raw_list) == 4:
            # 国家省份城市
            return [raw_list[0].strip(), raw_list[1].strip(), raw_list[2].strip()]
        else:
            return [u'未知', u'未知', u'未知']
    else:
        return [u'未知', u'未知', u'未知']
    # end

    # return _get_geo_data(loc)


def get_identity_card_geo_data(id_number):
    return _get_identity_card_geo_data(id_number)


def get_identity_card_location(id_number):
    return _get_location(_get_identity_card_geo_data(id_number))


def get_identity_card_cn_province(id_number):
    return _get_geo_cn_province(_get_identity_card_geo_data(id_number))


def get_identity_card_cn_city(id_number):
    return _get_geo_cn_city(_get_identity_card_geo_data(id_number))


def get_identity_card_cn_province_city(id_number):
    return _get_geo_cn_province_city(_get_identity_card_geo_data(id_number))


def test():
    assert _get_identity_card_geo_data('12')[1] == u'天津市'
    assert _get_identity_card_geo_data('522124')[1] == u'贵州省'
    assert _get_identity_card_geo_data('99')[1] == ''


if __name__ == '__main__':
    test()
