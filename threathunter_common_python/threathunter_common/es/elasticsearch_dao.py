#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import

import logging

from ..third_libs.pyes_0_99_5 import ES, TermQuery, BoolQuery, WildcardQuery, Search, RangeQuery, ESRange, QueryStringQuery
from ..third_libs.pyes_0_99_5.exceptions import IndexMissingException, NoServerAvailable, ElasticSearchException, NotFoundException

logger = logging.getLogger('threathunter_common.es.dao')

class ESDao():
    """
    Dao层只做一些ES API的封装，跟业务逻辑无关
    """
    def __init__(self, conn=None):
        """
        connection对象是由外部传入的，这样使得connection的复用更灵活
        同时，适用多线程场景
        """
        self.conn = conn

    def create_index(self, index_name):
        """
        create_index is as create database
        """
        if not self.has_index(index_name):
            self.conn.indices.create_index(index_name)

    def has_index(self, index_name):
        """
        判断是否存在这样的index，如果存在返回True，否则返回False
        """
        logger.debug('ES: Checking for index {0}'.format(index_name))
        try:
            self.conn.indices.status(index_name)
        except IndexMissingException:
            return False
        return True

    def delete_index(self, index_name):
        """
        如果存在特定的index，则删除该index
        """
        if self.has_index(index_name):
            try:
                self.conn.indices.delete_index(index_name)
            except Exception, e:
                raise e

    def refresh_index(self, index_name, wait=1):
        """
        刷新index，将最新的index从server同步过来
        建议在取数据前refresh
        """
        logger.debug('ES: Refreshing index {0}'.format(index_name))
        self.conn.indices.refresh(index_name, timesleep=wait)

    def get_existing_indices(self, indices=[], connection_para=None):
        """
        @alies:是否返回别名
        @indices:需要查询的索引列表
        """
        existing_indices = []
        if connection_para:
            indices_dict = self.conn.indices.aliases(indices=indices, connection_para=connection_para)
        else:
            indices_dict = self.conn.indices.aliases(indices=indices)
        if indices_dict:
            for index in indices:
                if index in indices_dict.keys():
                    # 存在于原名list
                    existing_indices.append(index)
                for key, item in indices_dict.iteritems():
                    if item.has_key("aliases") and item["aliases"]:
                        if index in item["aliases"].keys():
                            # 存在于别名list
                            existing_indices.append(index)
        return existing_indices

    def put_mapping(self, index_name, type_name, mapping):
        """
        put_mapping is as create table schema in database
        paras:
            type is as table
            mapping is as columns definition in table
            index is as database
        """
        self.conn.indices.put_mapping(type_name, {'properties': mapping}, [index_name])

    def add_record(self, index_name, type_name, record, id=None):
        """
        实时增加单条记录
        :param index_name: 索引名称（数据库名称）
        :param type_name: 索引类型名称（表名）
        :param record: 数据（记录），格式为dict
        :param id: 如果不传id，那么在插入数据时，ES会随机一个hash作为id；如果传入id，则将record的id设为该值
        :return:
        """
        if id:
            self.conn.index(record, index_name, type_name, id)
        else:
            self.conn.index(record, index_name, type_name)

    def add_records_in_bulk(self, index_name, type_name, record, id=None):
        """
        批量添加数据到ES的buffer中，默认批量大小为400个doc，这个大小在配置文件中设置
        如果record/doc数量超过了阈值（默认400），将会提交前400个doc到ES server
        所以buffer中doc的数量永远少于阈值，最后的一批数据需要通过调用self.flush_bulk()去强制提交到ES server

        :param index_name: 索引名称（数据库名称）
        :param type_name: 索引类型名称（表名）
        :param record: 数据（记录）
        :param id: 如果不传id，那么在插入数据时，ES会随机一个hash作为id；如果传入id，则将record的id设为该值
        :return: 返回批量插入后的插入结果
        """
        response = None
        try:
            if id:
                response = self.conn.index(record, index_name, type_name, id, bulk=True)
            else:
                response = self.conn.index(record, index_name, type_name, bulk=True)
        except NoServerAvailable, e:
            logger.exception("add record in bulk failed: " + str(e.message))
            raise Exception("add record in bulk failed")
        except Exception, e:
            logger.exception("add record in bulk failed: " + str(e.message))
            raise Exception("add record in bulk failed")

        return response

    def flush_bulk(self):
        """
        强制提交缓冲区中数据到ES server
        :return: a dict，包含了errors字段，如果errors字段为false则flush成功，如果errors字段为true，则flush不成功。
                不成功时的报错信息存在dict["items"]["create"]["error"]
        """
        return self.conn.flush_bulk(forced=True)

    def query_records(self, start_timestamp=None, end_timestamp=None, timestamp_field=None, indices=[], type_names=[], must=[], must_not=[], should=[], start=0, size=10,
                      sort=None, search_type="all", fields=None,facet_field=None, facet_order=None, facet_size=10, connect_mode="original", **kwargs):
        """
        查找相关doc
        :param indices: index的列表，在这些index中搜索
        :param type_names: index中的type，类似于database中的table
        :param must: dict类型，一定得包含的条件
        :param must_not: dict类型，一定不能包含的条件
        :param should: dict类型，可以包含的条件，类似于or
        :param start: from
        :param fields: 指定返回字段  *modified by wxt 2015-2-10*
        :param size: offset
        :param connect_mode: 要么是original要么是via token。标志是否使用token去访问ES server。如果不使用，则通过ES server地址访问，如果使用，则通过token认证访问
        :param search_type: 可以是"all"、"term facet"、"date facet"。如果是"all"则返回查找到的results；如果是"term facet"则返回KV对，适合饼图；如果是"date term"则返回折线图数据
        :param kwargs: 非公共参数以字典的形势放在kwargs中传入
        :return: pyes.es.ResultSet object, reference: https://pyes.readthedocs.org/en/latest/manual/resultset.html?highlight=resultset
        """

        must_ = []
        for item in must:
            #临时补丁 by wxt 2015-1-13
            if item.get("type","") == "range":
                range_query = RangeQuery(ESRange(field=item["field"], from_value=item.get("from",None), to_value=item.get("to",None), include_lower=True, include_upper=True))
                must_.append(range_query)

            else:
                if isinstance(item["value"], int) or isinstance(item["value"], long):
                    pass
                else:
                    if -1 != item["value"].find("*") or -1 != item["value"].find("?"):
                        # 支持通配符查询 modified by wxt 2015-1-15
                        must_.append(QueryStringQuery(query=item["value"], default_field=item["field"]))
                        #must_.append(WildcardQuery(item["field"], item["value"]))
                        continue
                must_.append(TermQuery(item["field"], item["value"]))

        if None != start_timestamp and None != end_timestamp and None != timestamp_field:
            time_query = RangeQuery(ESRange(field=timestamp_field, from_value=start_timestamp, to_value=end_timestamp, include_lower=True, include_upper=True))
            must_.append(time_query)

        must_not_ = []
        for item in must_not:
            if isinstance(item["value"],int) or isinstance(item["value"],long):
                pass
            else:            
                if -1 != item["value"].find("*") or -1 != item["value"].find("?"):
                    must_not_.append(WildcardQuery(item["field"], item["value"]))
                    continue
            must_not_.append(TermQuery(item["field"], item["value"]))

        should_ = []
        for item in should:
            if isinstance(item["value"],int) or isinstance(item["value"],long):
                pass
            else:            
                if -1 != item["value"].find("*") or -1 != item["value"].find("?"):
                    should_.append(WildcardQuery(item["field"], item["value"]))
                    continue
            should_.append(TermQuery(item["field"], item["value"]))

        query = BoolQuery(must=must_, must_not=must_not_, should=should_)

        s = Search(query, start=start, size=size, fields=fields)
        # s = Search(query, start=start, size=size, fields=fields, sort={"last_1d_count":{"order":"desc"}})

        if "all" == search_type:
            pass
        
        elif "term aggr" == search_type:
            if None != facet_field:
                s.agg.add(TermQuery(field="srcip"))
        elif "term facet" == search_type:
            if None != facet_field:
                s.facet.add_term_facet(field=facet_field, order=facet_order, size=facet_size)
        elif "date facet" == search_type:
            if kwargs.has_key("date_facet"):
                _d = kwargs["date_facet"]
                if not (_d.has_key("name") and _d.has_key("interval") and _d.has_key("key_field")):
                    raise Exception("date facet has no para name, interval and key_field")
                name = _d["name"]
                interval = _d["interval"]
                key_field = _d["key_field"]
                if _d.has_key("value_field") and _d["value_field"]:
                    value_field = _d["value_field"]
                    s.facet.add_date_facet(name, interval=interval, key_field=key_field, value_field=value_field)
                else:
                    s.facet.add_date_facet(name, interval=interval, field=key_field)
        elif "statistical facet" == search_type:
            if kwargs.has_key("statistical_facet"):
                _facet_list = kwargs["statistical_facet"]
                for facet in _facet_list:
                    name = facet["name"]
                    field = facet["field"]
                    s.facet.add_statistical_facet(name, field=field)
        else:
            pass

        result_set = None
        if "via token" == connect_mode:
            if None == self.conn or not isinstance(self.conn, ES):
                # 这里的conn其实不连接任何ES，只是为了使用search函数而初始化的，真正的连接在connection_http那里
                self.conn = ES()
            if not kwargs.has_key("connection"):
                raise Exception("via token mode, but there's no connection para such as token for connnection_http")
            if sort:
                result_set = self.conn.search(s, indices=indices, doc_types=type_names, sort=sort, connection_para=kwargs["connection"])
            else:
                result_set = self.conn.search(s, indices=indices, doc_types=type_names, connection_para=kwargs["connection"])
        elif "original" == connect_mode:
            if sort:
                result_set = self.conn.search(s, indices=indices, doc_types=type_names, sort=sort)
            else:
                result_set = self.conn.search(s, indices=indices, doc_types=type_names)
        else:
            pass

        return result_set

    def update_record(self, index_name=None, type_name=None, id=None, script=None, doc=None, upsert=None,params=None):
        try:
            self.conn.partial_update(index_name, type_name, id, script, doc=doc, upsert=upsert,params=params)
        except ElasticSearchException, e:
            raise Exception("id doesn't exist, or update dict is null: " + e.message)

    def delete_record(self, index_name=None, type_name=None, id=None):
        try:
            self.conn.delete(index_name, type_name, id)
        except NotFoundException, e:
            raise Exception("item not found")

    def __del__(self):
        pass

if __name__ == "__main__":
    from common.es.elasticsearch_conn import es_big_security_conn
    es_dao = ESDao(conn=es_big_security_conn)
    print "get existing indices"
    existing_indices = es_dao.get_existing_indices(indices=["pprobe-2015.01.19"])
    print "existing indices: ", existing_indices

    # def __query_records_via_token(self, indices, type_names, must, must_not, should, start, size, search_type, **kwargs):
    #     must_list = []
    #     should_list = []
    #     must_not_list = []
    #
    #     timeitem = "@timestamp" # hardcode, 以后需要作为参数传入
    #     timerange = dict()
    #     timerange["range"] = {}
    #     timerange["range"][timeitem] = {}
    #     timerange["range"][timeitem]["from"] = 1418204900212
    #     timerange["range"][timeitem]["to"] = 1418205500212
    #     must_list.append(timerange)
    #
    #     for item in must:
    #         query = dict()
    #         query['query'] = {}
    #         query['query']['query_string'] = {}
    #         query['query']['query_string']['query'] = item["field"] + ':' + item["value"]
    #         must_list.append(query)
    #
    #     for item in must_not:
    #         query = dict()
    #         query['query'] = {}
    #         query['query']['query_string'] = {}
    #         query['query']['query_string']['query'] = item["field"] + ':' + item["value"]
    #         must_not_list.append(query)
    #
    #     for item in should:
    #         query = dict()
    #         query['query'] = {}
    #         query['query']['query_string'] = {}
    #         query['query']['query_string']['query'] = item["field"] + ':' + item["value"]
    #         should_list.append(query)
    #
    #     if "all" == search_type:
    #         search_body = {
    #             "from": start,
    #             "size": size,
    #             "query": {
    #                 "filtered": {
    #                     "query": {
    #                         "query_string": {
    #                             "query": "*"
    #                         }
    #                     },
    #                     "filter": {
    #                         "bool": {
    #                             "must": must_list,
    #                             "must_not": must_not_list,
    #                             "should": should_list
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     elif "facet" == search_type:
    #         search_body = {
    #             "facets": {
    #                 "terms": {
    #                     "terms": {
    #                         "field": kwargs["field"],
    #                         # 这个size是说，取50组统计数据如果total超过50组的话
    #                         "size": 50,
    #                         "order": "count",
    #                         "exclude": [""]
    #                     },
    #                     "facet_filter": {
    #                         "fquery": {
    #                             "query": {
    #                                 "filtered": {
    #                                     "query": {
    #                                         "query_string": {
    #                                             "query": "*"
    #                                         }
    #                                     },
    #                                     "filter": {
    #                                         "bool": {
    #                                             "must": must_list,
    #                                             "must_not": must_not_list,
    #                                             "should": should_list
    #                                         }
    #                                     }
    #                                 }
    #                             }
    #                         }
    #                     }
    #                 }
    #             },
    #             # size:0 意味着只显示统计数据，不显示hit到的具体数据
    #             "size": 0
    #         }
    #     elif "histogram" == search_type:
    #         search_body = {
    #             "facets": {
    #                 "0": {
    #                     "date_histogram": {
    #                         "key_field": "@timestamp",
    #                         "value_field": "time_taken",
    #                         "interval": "5s"
    #                     },
    #                     "global": True,
    #                     "facet_filter": {
    #                         "fquery": {
    #                             "query": {
    #                                 "filtered": {
    #                                     "query": {
    #                                         "query_string": {
    #                                             "query": "*"
    #                                         }
    #                                     },
    #                                     "filter": {
    #                                         "bool": {
    #                                             "must": must_list,
    #                                             "must_not": must_not_list,
    #                                             "should": should_list
    #                                         }
    #                                     }
    #                                 }
    #                             }
    #                         }
    #                     }
    #                 }
    #             },
    #             "size": 0
    #         }
    #     else:
    #         search_body = None
    #
    #     indices_for_url = ','.join(indices)
    #     url = 'http://osg.ops.ctripcorp.com/api/esapi/%s/_search' % indices_for_url
    #     ori_data = json.dumps(search_body)
    #     response = requests.post(url, data=json.dumps({'access_token':'413a918c78421e762637f634cc24f6aa', 'request_body': ori_data}))
    #     print "results: ", json.loads(response.text)
    #     return json.loads(response.text)

    # def __query_records_via_server_address(self, indices, type_names, must, must_not, should, start, size):
    #     must_ = []
    #     for item in must:
    #         _ = TermQuery(item["field"], item["value"])
    #         must_.append(_)
    #
    #     must_not_ = []
    #     for item in must_not:
    #         _ = TermQuery(item["field"], item["value"])
    #         must_not_.append(_)
    #
    #     should_ = []
    #     for item in should:
    #         _ = TermQuery(item["field"], item["value"])
    #         should_.append(_)
    #
    #     query = BoolQuery(must=must_, must_not=must_not_, should=should_)
    #     s = Search(query, start=start, size=size)
    #
    #     result_set = self.conn.search(s, indices=indices, doc_types=type_names)
    #     return result_set



    # def query_records(self, starttime=None, endtime=None, type_name=None, must={}, must_not={}, should={}, start=0, size=10, indices=None,
    #                   facet_field=None, facet_fields=None, facet_order="count", facet_size=10, return_type="records"):
    #     """
    #     not case-sensitive, fuzzy query
    #     @must 必要条件
    #     @must_not 否定条件
    #     @should 选择条件
    #     @facet_field 聚合字段
    #     @facet_fields 聚合字段列表  *目前不可用
    #     @facet_order 聚合排序
    #     @facet_size 返回聚合的个数
    #     @return_type:  facet or  records or count
    #     @indices: 如果不指定indices 则会使用默认的 athena or  athena_test + 相应日期
    #     """
    #     starttime = starttime if starttime else self.init_timestamp
    #     endtime = endtime if endtime else int(time.time())
    #     if not indices:
    #         indices = self.get_indices(starttime, endtime)
    #     if indices:
    #         must_ = []
    #         for key in must.keys():
    #             _ = TermQuery(key, must[key])
    #             must_.append(_)
    #
    #         must_not_ = []
    #         for key in must_not.keys():
    #             _ = TermQuery(key, must_not[key])
    #             must_not_.append(_)
    #
    #         should_ = []
    #         for key in should.keys():
    #             _ = TermQuery(key, should[key])
    #             should_.append(_)
    #         # TODO: 如果没有TimeStamp就会查不到任何数据
    #         # starttime = starttime*1000
    #         # endtime = endtime*1000
    #         # time_query = RangeQuery(ESRange(field="TimeStamp", from_value=starttime, to_value=endtime, include_lower=True, include_upper=True))
    #         # must_.append(time_query)
    #
    #         query = BoolQuery(must=must_, must_not=must_not_, should=should_)
    #         q = Search(query, start=start, size=size)
    #         if facet_field or facet_fields:
    #             q.facet.add_term_facet(field=facet_field, order=facet_order, size=facet_size)
    #         res = self.conn.search(q, indices=indices, doc_types=type_name)
    #
    #         if return_type == "records":
    #             return res
    #         elif return_type == "facet":
    #             res = res.facets[facet_field]["terms"]
    #             if res:
    #                 res = [[x["term"], x["count"]] for x in res]
    #                 return res
    #         # 返回命中的总记录数 -add by cy
    #         elif return_type == "count":
    #             return res.total
    #     if return_type == "count":
    #         return 0
    #     else:
    #         return []


    # def get_indices(self, starttime, endtime):
    #     exists_indices = self.conn.indices.get_indices()
    #     if starttime and endtime:
    #         t = datetime.datetime.fromtimestamp(starttime)
    #         t = t.strftime('%Y.%m.%d')
    #         start_index = "%s-%s" %(self.index_prefix, t)
    #         t = datetime.datetime.fromtimestamp(endtime)
    #         t = t.strftime('%Y.%m.%d')
    #         end_index = "%s-%s" %(self.index_prefix, t)
    #         indices = [x for x in exists_indices if x >= start_index and x <= end_index]
    #     else:
    #         indices = [x for x in exists_indices if x.startswith(self.index_prefix+"-")]
    #     return indices

    # 聚合参数
    # def facets(self, starttime=None, endtime=None, indices=None, facet_field=None, facet_fields=None, filters={}, type_name=None, order="count", size=10):
    #     """
    #     @facet_field:聚合字段
    #     @order:排序
    #     @size:返回数量
    #     """
    #     if not indices:
    #         indices = self.get_indices(starttime, endtime)
    #     if indices:
    #         must = []
    #         for key in filters.keys():
    #             _ = TermQuery(key, filters[key])
    #             must.append(_)
    #
    #         query = BoolQuery(must=must)
    #         q3 = Search(query)
    #         q3.facet.add_term_facet(field=facet_field, order=order, size=size)
    #
    #         res = self.conn.search(q3, indices=indices, doc_types=type_name)
    #         res = res.facets[facet_field]["terms"]
    #         if res:
    #             res = [[x["term"], x["count"]] for x in res]
    #             return res
    #     return []

    # TODO：下面这些方法是用原生的RESTful接口实现的，等PYES的接口稳定之后，会将这部分代码删除，统一通过PYES去操作ES
    # def _transmit_data_to_es(self, url, data_in_json):
    #     '''
    #     Transmit the data to ES server by using RESTful interface
    #     This function is serving for function "add_record_in_raw"
    #     :param url:
    #     :param data_in_json:
    #     :return:
    #     '''
    #     try:
    #         req = urllib2.Request(url,data_in_json)
    #         req.add_header('Content-type',"application/x-www-form-urlencoded")
    #
    #         opener=urllib2.build_opener()
    #         Res = opener.open(req,timeout=5)
    #         source = Res.read()
    #
    #         logger.debug(source)
    #         return source
    #
    #     except Exception, e:
    #         logger.exception(e)
    #
    # def _transform_timestamp(self, timestamp):
    #     '''
    #     Transform timestamp to '2014.10.04'format (serving for function 'add_record_in_raw')
    #     :param timestamp:
    #     :return:
    #     '''
    #     timestamp = datetime.datetime.fromtimestamp(timestamp)
    #     timestamp = timestamp.strftime('%Y.%m.%d')
    #     return timestamp
    #
    # def _generate_upload_url_path(self, db, timestamp, type_name):
    #     '''
    #     Generate the upload url for ES (serving for function 'add_record_in_raw')
    #     :param db:
    #     :param timestamp:
    #     :param type_name:
    #     :return:
    #     '''
    #     path = "%s-%s/%s" %(db, timestamp, type_name)
    #     url = ES_HOST + ":" + ES_PORT + path
    #     return url
    #
    # def add_record_in_raw(self, data, timestamp, type_name):
    #     '''
    #     Add a record(message) to ES using RESTful Interface provided by ES
    #     :param data: the data to be stored in mapping format( Here it will be translated to json format), e.g: {'name':a, 'age': 12}
    #     :param timestamp: the timestamp of this data, which will be used for splitting in ES, as an Integer, e.g: 1409799399
    #     :param type_name: table_type specify the type of the data, which is important. Same formatted data should be stored by same table_type, e.g : "athena_msg"
    #     :return:
    #     '''
    #
    #     try:
    #         timestamp = self._transform_timestamp(timestamp)
    #         url = self._generate_upload_url_path(ES_DB, timestamp, type_name)
    #         data_in_json=json.dumps(data)
    #         self._transmit_data_to_es(url, data_in_json)
    #
    #     except Exception,e:
    #         print e
    #         logger.exception(e)