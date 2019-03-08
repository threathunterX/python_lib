#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 16-6-1
from threathunter_common.geo.geoutil import get_identity_card_geo_data
from threathunter_common.geo.geoutil import get_phone_geo_data
import unittest


class TestGeo(unittest.TestCase):

    def test_get_identity_card_geo_data(self):
        self.assertEqual(get_identity_card_geo_data('310104198703219093'), [u'中国', u'上海市', u'上海市'])
        self.assertEqual(get_identity_card_geo_data('340200198203092597'), [u'中国', u'安徽省', u'芜湖市'])
        self.assertEqual(get_identity_card_geo_data('220284198709259155'), [u'中国', u'吉林省', u'吉林市'])
        self.assertEqual(get_identity_card_geo_data('230207199102126501'), [u'中国', u'黑龙江省', u'齐齐哈尔市'])

    def test_get_phone_geo_data(self):
        self.assertEqual(get_phone_geo_data('13002212365'), [u'中国', u'天津市', u'天津市'])
        self.assertEqual(get_phone_geo_data('13355882979'), [u'中国', u'浙江省', u'温州市'])
        self.assertEqual(get_phone_geo_data('18515612365'), [u'中国', u'北京市', u'北京市'])
        self.assertEqual(get_phone_geo_data('18615612365'), [u'中国', u'山东省', u'济南市'])
        self.assertEqual(get_phone_geo_data('18715612365'), [u'中国', u'安徽省', u'淮北市'])
        self.assertEqual(get_phone_geo_data('1887218942q1'), ['', '', ''])

if __name__ == '__main__':
    unittest.main()
