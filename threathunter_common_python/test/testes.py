#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threathunter_common.util import curr_timestamp
from threathunter_common.es.elasticsearch_service import ESServiceForHoneyPot


__author__ = "nebula"



es = ESServiceForHoneyPot(thread_mode="single", port=9200, host="127.0.0.1")



etime = curr_timestamp()
stime = etime - 1800
#print es.get_records_list(starttime=stime, endtime=etime, type_names=["proxy_req_log"], must=[],
                   #must_not=[], should=[], size=10,
                   #sort=None, fields=None)
should = [{'field': 'url', 'value': 'password'},
          {'field': 'url', 'value': 'pwd'},
          {'field': 'url', 'value': 'user_name'},
          {'field': 'url', 'value': 'username'},
          {'field': 'post_data', 'value': 'password'},
          {'field': 'post_data', 'value': 'pwd'},
          {'field': 'post_data', 'value': 'user_name'},
          {'field': 'post_data', 'value': 'username'},]
print es.get_term_facet_list(starttime=stime, endtime=etime, type_names=["proxy_req_log"], must=[],
                      must_not=[], should=should,
                      facet_field="srcip",
                      facet_order=None, facet_size=1000)
