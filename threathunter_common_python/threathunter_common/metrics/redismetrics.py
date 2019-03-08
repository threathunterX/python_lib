#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

from .metrics import Metrics
from ..redis.redisctx import RedisCtx
from ..util import millis_now

__author__ = 'lw'


class RedisMetrics(Metrics):

    def __init__(self, host, port, password=None, nodes=None):
        self.redisctx = RedisCtx()
        self.redisctx.host = host
        self.redisctx.port = port
        self.redisctx.password = password
        self.redisctx.nodes = nodes

    def add_metrics(self, db, metrics_name, tags, value, expire_seconds, timestamp=None):
        metrics_group = '{metrics.%s}' % db
        metrics_name = "%s.%s" % (db, metrics_name)

        r = self.redisctx.redis
        seq_key = "{}_seq".format(metrics_name)
        seq = r.incr(metrics_group+seq_key)
        metrics_id_key = "{}_{}".format(metrics_name, seq)

        tags = tags or dict()
        tags = {str(k): str(v) for k, v in tags.iteritems()}
        tags["value"] = str(value)
        ts = timestamp if timestamp else millis_now()
        tags["ts"] = str(ts)
        ts = float(ts)

        p = r.pipeline()
        p.hmset(metrics_group+metrics_id_key, tags)
        p.zadd(metrics_group+metrics_name, metrics_id_key, ts)
        p.zremrangebyscore(metrics_group+metrics_name, 0, ts - expire_seconds * 1000)
        p.expire(metrics_group+metrics_id_key, expire_seconds)
        p.expire(metrics_group+seq_key, expire_seconds)
        p.expire(metrics_group+metrics_name, expire_seconds)
        p.execute()

    def query(self, db, metrics_name, aggregation_type, time_start, time_end, time_interval, filter_tags=dict(),
              group_tags=list()):
        metrics_group = '{metrics.%s}' % db
        metrics_name = "%s.%s" % (db, metrics_name)
        results = self._get_redis_row_metrics(metrics_name, time_start, time_end, filter_tags, metrics_group)
        if not results:
            return dict()

        ts_base = float(results[0].get("ts"))
        ts_start = ts_base - ts_base % time_interval
        results = self._get_merged_query_rows(results, metrics_name, ts_start, time_interval, aggregation_type,
                                              group_tags)

        # normalize the result
        for legend_map in results.itervalues():
            for legend in legend_map:
                legend_map[legend] = legend_map[legend][0]

        return results

    def clear(self, db, metrics_name):
        metrics_group = '{metrics.%s}' % db
        metrics_name = "%s.%s" % (db, db, metrics_name)
        r = self.redisctx.redis
        seq_key = "{}{}_seq".format(metrics_group, metrics_name)
        entries = r.zrangebyscore(metrics_group+metrics_name, 0, 2 ** 64)
        entries = [metrics_group + _ for _ in entries]
        p = r.pipeline()
        p.delete(entries)
        p.delete(metrics_group+metrics_name)
        p.delete(seq_key)
        p.execute()

    def _get_merged_query_rows(self, results, metrics_name, ts_start, group_interval, aggregation, group_tags):
        ts_legend_map = dict()
        for result in results:
            legend = list()
            if group_tags:
                all_find = True
                for tag in group_tags:
                    if tag not in result:
                        all_find = False
                    legend.append(str(result.get(tag, "")))
                if not all_find:
                    continue
            legend = tuple(legend)

            ts = float(result["ts"])
            value = float(result["value"])
            ts = ts / group_interval * group_interval
            data = ts_legend_map.setdefault(ts, dict()).setdefault(legend, [None, 0])

            if data[0] is None:
                data[0] = value
            elif aggregation == "max":
                data[0] = max(data[0], value)
            elif aggregation == "min":
                data[0] = min(data[0], value)
            elif aggregation == "sum":
                data[0] += value
            elif aggregation == "mean":
                data[0] = float(data[0] * data[1] + value) / (data[1] + 1)

            data[1] += 1
        return ts_legend_map

    def _get_redis_row_metrics(self, metrics_name, time_start, time_end, tags, metrics_group):
        tags = {k: set(map(lambda x: str(x), v)) for k, v in tags.iteritems()}

        r = self.redisctx.redis
        results = list()

        entries = r.zrangebyscore(metrics_group+metrics_name, time_start, time_end)

        logging.error("zrangebyscore by %s, %s-%s, return %s", metrics_name, time_start, time_end, entries)
        if entries:
            p = r.pipeline()
            for entry in entries:
                p.hgetall(metrics_group + entry)

            for entry_data in p.execute():
                if not entry_data:
                    logging.error("no entry...")
                    continue

                if tags:
                    need_filter = False
                    for k, v in tags.iteritems():
                        if str(entry_data.get(k)) not in v:
                            need_filter = True
                            break
                    if need_filter:
                        continue

                results.append(entry_data)

        return results

