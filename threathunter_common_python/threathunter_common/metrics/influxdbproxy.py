#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import urlparse
import traceback

from .metricsagent import MetricsAgent
from .redismetrics import RedisMetrics
from ..util import millis_now

__author__ = "nebula"


path_pattern = re.compile("/db/(.*?)/series")
select_pattern = re.compile("select (.*?) from")
from_pattern = re.compile("from (.*?) where")
where_pattern = re.compile("where (.*?) group by")
groupby_pattern = re.compile("group by (.*?)$")
condition_pattern = re.compile("(.*?)(>|<|=)(.*)")
time_pattern = re.compile("([0-9]{1,})(h$|m$|s$|ms$|d$)")
interval_pattern = re.compile("time\((.*?)$")
aggregation_type_pattern = re.compile("^(.*)\(")


def trim_special_ch(item):
    while item:
        if item[0] in "\"' (":
            item = item[1:]
            continue

        if item[-1] in "\"' )":
            item = item[:-1]
            continue

        break

    return item


def extract_ts(time_data):
    if time_data.startswith("now()"):
        return millis_now() - extract_ts(time_data[5:])

    m = time_pattern.search(time_data)
    value = int(m.group(1))
    unit = m.group(2)
    if unit == "s":
        value *= 1000
    elif unit == "m":
        value *= (60 * 1000)
    elif unit == "h":
        value *= (3600 * 1000)
    elif unit == "d":
        value *= (86400 * 1000)

    return value


def _extract_metrics_params(url):
    path, url_query = url.split("?", 1)
    m = path_pattern.search(path)
    if not m:
        raise RuntimeError("invalid path")

    db = m.group(1)

    url_query = urlparse.parse_qs(url_query)
    query = url_query["q"][0]

    select_clause = select_pattern.search(query).group(1)
    from_clause = from_pattern.search(query).group(1)
    where_clause = where_pattern.search(query).group(1)
    groupby_clause = groupby_pattern.search(query).group(1)

    # process select clause
    for select in select_clause.split(","):
        select = select.strip()
        if "(" not in select:
            continue
        aggregation_type = aggregation_type_pattern.search(select).group(1)

    # process from_clause
    metrics_name = from_clause
    if metrics_name.startswith('"'):
        metrics_name = metrics_name[1:]
    if metrics_name.endswith('"'):
        metrics_name = metrics_name[:-1]

    # process where clause
    conditions = where_clause.split(" and ")
    conditions = [_.split(" or ") for _ in conditions]
    conditions = sum(conditions, [])
    time_start = 0
    time_end = millis_now()
    tags = dict()
    for condition in conditions:
        m = condition_pattern.match(condition)
        left, op, right = map(lambda x: trim_special_ch(x), m.groups()[:3])

        if left == "time":
            ts = extract_ts(right)
            if ">" in op:
                time_start = ts
            else:
                time_end = ts
        elif op == "=":
            tags.setdefault(left, list()).append(right)
        else:
            raise RuntimeError("condition is unsupported")

    group_tags = []
    interval = 0
    for groupby in groupby_clause.split(","):
        groupby = groupby.strip().split(" ")[0]
        groupby = trim_special_ch(groupby)
        if groupby.startswith("time"):
            time_str = interval_pattern.search(groupby).group(1)
            interval = extract_ts(time_str)
            continue
        group_tags.append(groupby)

    # interval is milliseconds
    return (db, metrics_name, aggregation_type, time_start, time_end, interval, tags, group_tags)


def get_metrics(url):
    try:
        db, metrics_name, aggregation_type, time_start, time_end, interval, tags, group_tags = _extract_metrics_params(url)
        data = MetricsAgent.get_instance().query(db, metrics_name, aggregation_type, time_start, time_end, interval, tags, group_tags)
        result = []
        if not data:
            return result

        item = dict()
        item["name"] = metrics_name
        item["columns"] = ["time"] + [aggregation_type] + group_tags
        item["points"] = list()
        ts_list = data.keys()
        ts_list.sort()
        for ts in ts_list:
            ts_data = data[ts]
            for legend, value in ts_data.iteritems():
                p = [ts] + [value] + list(legend)
                item["points"].append(p)
        return [item]

    except Exception as err:
        print err
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    m = RedisMetrics("localhost", 6379)
    m.add_metrics("test", "test", {"tag1": "tag1"}, 1.0, 60)
    m.add_metrics("test", "test", {"tag1": "tag2"}, 3.0, 60)
    get_metrics("/db/test/series?p=threathunterinfluxdb119&q=select sum(value) from \"test\" where time > now()-1h and (\"tag1\" = 'tag1' or \"tag1\" = 'tag2') group by time(60s)")


