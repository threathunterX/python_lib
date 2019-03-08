#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import

import datetime

from .elasticsearch_service_base import BaseESServiceForThreathunter
from ..third_libs.pyes_0_99_5.exceptions import ElasticSearchException


class ESServiceForHoneyPot(BaseESServiceForThreathunter):
    """
    继承自BaseESServiceForAthena，专为Message的录入和查询服务
    """
    HONEY_POT_INDEX_PREFIX = "honey_pot"
    
    
    def add_records_in_buffer(self, timestamp, typee, record, id=None):
        timestamp = datetime.datetime.fromtimestamp(timestamp)
        datee = timestamp.strftime('%Y-%m-%d')
        index = "%s_%s" % (self.HONEY_POT_INDEX_PREFIX, datee)
        if id:
            ret = self._es_dao.add_records_in_bulk(index, typee, record, id)
        else:
            ret = self._es_dao.add_records_in_bulk(index, typee, record)
        if not ret:
            return True
        elif ret["errors"]:
            return False
        return True

    def flush_bulk(self):
        ret = self._es_dao.flush_bulk()
        if not ret:
            # ret_dict也有可能为None，那就是当buffer中的数量正好达到了400的整数倍的时候，数据自动提交了，这时候去flush bulk就会返回None
            return True
        elif ret["errors"]:
            return False
        return True

    def get_records_list(self, starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], size=10,sort=None,fields=None):
        """
        根据starttime和endtime得到indices
        同时，starttime和endtime也作为Range(start_timestamp, end_timestamp)去搜索记录
        """
        indices = self.get_existing_indices(index_prefix=self.HONEY_POT_INDEX_PREFIX, starttime=starttime, endtime=endtime)
        if not indices:
            return []
        result_set = self._es_dao.query_records(start_timestamp=starttime*1000, end_timestamp=endtime*1000, timestamp_field="@timestamp",
                                                indices=indices, type_names=type_names,fields=fields, must=must, must_not=must_not, should=should, size=size,sort=sort)
        results = []
        for result in result_set:
            results.append(dict(result))
        return results

    def query_records_count(self, starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], size=0):
        indices = self.get_existing_indices(index_prefix=self.HONEY_POT_INDEX_PREFIX, starttime=starttime, endtime=endtime)
        if not indices:
            return 0
        result_set = self._es_dao.query_records(start_timestamp=starttime*1000, end_timestamp=endtime*1000, timestamp_field="@timestamp",
                                                indices=indices, type_names=type_names, must=must, must_not=must_not, should=should, size=size)
        return result_set.total

    def get_term_facet_list(self, starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], facet_field=None, facet_order=None, facet_size=10):
        """
        根据starttime和endtime得到indices
        同时，starttime和endtime也作为Range(start_timestamp, end_timestamp)去搜索记录
        """
        if not facet_field:
            return []
        indices = self.get_existing_indices(index_prefix=self.HONEY_POT_INDEX_PREFIX, starttime=starttime, endtime=endtime)
        if not indices:
            return []
        result_set = self._es_dao.query_records(start_timestamp=starttime*1000, end_timestamp=endtime*1000, timestamp_field="@timestamp", indices=indices,
                                                type_names=type_names, must=must, must_not=must_not, should=should, size=0, search_type="term facet",
                                                facet_field=facet_field, facet_order=facet_order, facet_size=facet_size, connect_mode="original")
        if result_set.facets[facet_field].has_key("terms"):
            return [[x["term"], x["count"]] for x in result_set.facets[facet_field]["terms"]]
        return []

    def get_date_facet_list(self, starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], date_interval=5, key_field=None, value_field=None):
        indices = self.get_existing_indices(index_prefix=self.HONEY_POT_INDEX_PREFIX, starttime=starttime, endtime=endtime)
        if not indices:
            return []
        name = "fake"
        kwargs = {"date_facet": {"name": name, "interval": date_interval, "key_field": key_field, "value_field": value_field}}
        result_set = self._es_dao.query_records(start_timestamp=starttime*1000, end_timestamp=endtime*1000, timestamp_field="@timestamp", indices=indices,
                                                type_names=type_names, must=must, must_not=must_not, should=should, size=0, search_type="date facet",
                                                connect_mode="original", **kwargs)
        try:
            if result_set.facets[name].has_key("entries"):
                if value_field:
                    return [[x["time"], x["total"]] for x in result_set.facets[name]["entries"]]
                return [[x["time"], x["count"]] for x in result_set.facets[name]["entries"]]
            return []
        except ElasticSearchException:
            return []

    def get_statistical_facet_list(self, starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], fields=None):
        indices = self.get_existing_indices(index_prefix=self.HONEY_POT_INDEX_PREFIX, starttime=starttime, endtime=endtime)
        if not indices:
            return []

        facet_list = []
        for name in fields.keys():
            field = fields[name]
            facet_list.append({"name":name, "field": field})

        kwargs = {"statistical_facet": facet_list}
        result_set = self._es_dao.query_records(start_timestamp=starttime*1000, end_timestamp=endtime*1000, timestamp_field="@timestamp", indices=indices,
                                                types_name=type_names, must=must, must_not=must_not, should=should, size=0, search_type="statistical facet",
                                                connect_mode="original", **kwargs)
        try:
            results = dict()
            for facet in facet_list:
                name = facet["name"]
                result_map = dict(result_set.facets[name])
                results[name] = result_map
            return results
        except Exception:
            return {}
        
    def get_statistical_facet_list_total(self,starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], fields=None):
        retval=[]
        facet_list=self.get_statistical_facet_list(starttime=starttime,endtime=endtime,type_names=type_names,must=must,must_not=must_not,should=should,fields=fields)
        if facet_list:
            for k,v in facet_list.iteritems():
                retval.append([k,v["total"]])
        return retval
    
    def get_statistical_facet_list_total_singlevalue(self,starttime=None, endtime=None, type_names=[], must=[], must_not=[], should=[], fields=None):
        retval=[]
        facet_list=self.get_statistical_facet_list(starttime=starttime,endtime=endtime,type_names=type_names,must=must,must_not=must_not,should=should,fields=fields)
        if facet_list:
            for k,v in facet_list.iteritems():
                retval.append([k,v["total"]])
        if len(retval) > 0:
            return retval[0][1]
        else:
            return 0    









if __name__ == "__main__":
    #print AntibotService.gateway_rule1(1421980000, 1421980498)

    import time
    service = ESServiceForHoneyPot()
    for i in xrange(1000):
        time.sleep(0.1)
        i += 1
        print i
        timestamp = 1423292017
        record = {'class_type': 1, u'extend': {u'owner': u'vqb\u94b1\u658c', u'line': u'76', u'checkmarx_link': u'http://192.168.83.23/CxWebclient/ViewerMain.aspx?scanId=3400&ProjectID=4140', u'url_params': u'Text', u'filename': u'\\FaxSearch.aspx.cs'}, u'confirm': 0, 'bu': 3, 'TimeStamp': 1423124456000L, u'is_corr': 1, 'job_id': 0, u'name': u'pressure test', u'source': 4, u'host': u'sinfo.ctrip.com', u'id': 73374, 'user': u'jiangr', 'pd': 1L, 'subclass_type': 2, 'evt_type': 1, u'desc': u'pre test --cy', 'result_type': 16, u'severity': 2}
        rc = service.add_records_in_buffer(timestamp, "result", record)
        print "add records in buffer: ", rc
    rc = service.flush_bulk()
    print "flush bulk: ", rc

    # print service.get_date_facet_list(starttime=1420980000, endtime=1421980498, type_names=["result"], date_interval=5, key_field="TimeStamp",)


    #fields = {'XLSX': 'source_status.xlsx', 'DOCX': 'source_status.docx', 'EXE': 'source_status.exe', 'PPSX': 'source_status.ppsx', 'DOC': 'source_status.doc', 'DLL': 'source_status.dll', 'PDF': 'source_status.pdf', 'XLS': 'source_status.xls', 'RTF': 'source_status.rtf', 'RAR': 'source_status.rar', 'PPT': 'source_status.ppt'}
    #fields = {'srcip': 'srcip'}
    #results = service.get_term_aggr_list(starttime=1421638984, endtime=1421898184, type_names=["result"], must=[{'field': 'source', 'value': '8'}], facet_field="srcip")
    
    #results = service.get_term_facet_list(starttime=1421891225, endtime=1421894825, type_names=["result"], must=[{'field': 'source', 'value': '8'}], facet_field="srcip",facet_size=1000)
    #print results
    ### results = service.get_statistical_facet_list(starttime=1419843882, endtime=1419930282, type_names=["indicator"], must=[{"field": "source_status.source", "value": "12"}], fields={"stat1": "source_status.device_total_count", "stat2":"source_status.device_succ_count"})
    ##print results
    #record={"details":{"bu":"3"},}
    #record={"name":"酒店跨站",}
    #service = ESServiceForEvent()
    #service.update_record_by_id(id="b2G1wm6yR32Ue2teoQussg",typee="standard", record=record)

    # service.add_records_in_buffer(typee="standard", record={"evt_type":2, "subclass_type":3, "name": "testname22", "details": {"url":"testurl"}}, id=4)
    # service.flush_bulk()
    # CASE1: [PASS][Athena For Message] Message根据查询条件返回record list [{record},{record}]
    # service = ESServiceForMessage()
    # type_names 可以指定搜索的type
    # results = service.get_records_list(starttime=1418525155, endtime=1418695611, type_names=["jobs, result"], size=2)
    # 如果不指定type_names，则全type搜索
    # results = service.get_records_list(starttime=1418525155, endtime=1418695611, size=2)
    # print results

    # CASE2: [PASS][Athena For Message] Message的term facet功能，返回[[record],[record]]
    # service = ESServiceForMessage()
    # 根据省份做term facet
    # results = service.get_term_facet_list(starttime=1418525155, endtime=1418695611, type_names=["result"], must=[], must_not=[], should=[], facet_field="s_province", facet_order="count", facet_size=10)
    # results = service.get_term_facet_list(starttime=1418525155, endtime=1418695611, type_names=["result"], must=[], must_not=[], should=[], facet_order="count", facet_size=10)
    # print results

    # CASE3 [Athena For Message] Message的date facet功能
    # service = ESServiceForMessage()
    # 如果没有value_field，那么返回根据key_field聚合之后records的count
    # results = service.get_date_facet_list(starttime=1418525155, endtime=1418695611, type_names=["result"], date_interval=5, key_field="TimeStamp")
    # 如果有value_field，那么返回根据key_field聚合之后的value_field的区间平均值
    # results = service.get_date_facet_list(starttime=1418525155, endtime=1418695611, type_names=["result"], date_interval=5, key_field="TimeStamp", value_field="result.extend.value")
    # print results

    # CASE4 [OPS] OPS的date facet功能
    # 索引: [iislog-all-]YYYY.MM.DD[-hotels.ctrip.com]（OPS）查询方式: Histogram; 查询条件:1) 6小时
    # service = ESServiceForOPS()
    # rs = service.get_date_facet_list(index_prefix="iislog-all", index_suffix="agent.insurance.sh.ctriptravel.com", starttime=1418462250, endtime=1418635050,
                                            # must=[{"field": "c_ip", "value": "192.168.49.72"}], key_field="@timestamp", value_field="time_taken")

    # rs = service.get_date_facet_list(index_prefix="iislog-all", index_suffix="hotels.ctrip.com", starttime=1418659200, endtime=1418720697, key_field="@timestamp", date_interval=2000)
    # print rs

    # CASE5 [BIG SECURITY] BS的date facet功能
    # 索引: [pprobe-]YYYY.MM.DD（大安）; 查询方式: Histogram; 查询条件:1) attrs.URL:hotels.ctrip.com* 2) 6小时
    # service = ESServiceForBigSecurity()
    # rs = service.get_date_facet_list(index_prefix="pprobe", starttime=1418535822, endtime=1418536122,
    #                                  must=[{"field":"attrs.URL", "value":"hotels.ctrip.com"}], key_field="@timestamp")
    # rs = service.get_date_facet_list(index_prefix="pprobe", starttime=1418535822, endtime=1418536122, key_field="@timestamp")
    # print rs