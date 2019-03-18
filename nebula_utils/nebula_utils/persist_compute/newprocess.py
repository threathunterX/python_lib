#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "nebula"


import sys
import json
import cache
from functools import partial
import logging
logger = logging.getLogger("nebula_utils.persist_compute.newprocess")

from threathunter_common.util import curr_timestamp

from condition import eval_condition, eval_statement
from .utils import dict_merge, Scene_Variable_Names
from nebula_utils.persist_utils import utils

Dimension_Mappings = {
    "page": "page",
    "c_ip": "ip",
    "did": "did",
    "uid": "user",
    }


class Node(object):
    def __init__(self, variable, idx_map):
        self.variable = variable
        self.name = self.variable["name"]
        self.idx = idx_map[self.name]
        self.src_variable_names = [_[1] for _ in self.variable["srcVariablesID"]]
        self.src_variable_ids = [idx_map[_] for _ in self.src_variable_names]

        self.mappings = []
        self.resolve_mappings()

        self.dimension = self.get_dimension()

        if not self.variable:
            self.raise_exception("null variable")

        if not self.dimension:
            self.raise_exception("null dimension")

    def raise_exception(self, msg):
        # TODO use decorator
        logger.error("Node {}: {}. Variable: {}".format(self.name, msg, self.variable))
        raise Exception("Node {}: {}. Variable: {}".format(self.name, msg, self.variable))

    def get_dimension(self):
        name = self.name
        if name.startswith("_"):
            #内部变量_打头，去掉
            name = name[1:]

        dimension = name.split("__")[0]
        return dimension

    def resolve_mappings(self):
        mappings = self.variable.get("config", ).get("mappings", [])
        if mappings:
            for m in mappings:
                src_name = m["srcProperty"]["name"]
                dest_name = m["destProperty"]["name"]
                if src_name != dest_name:
                    # add mapping (src_name --> dest_name), not support complex one yet
                    self.mappings.append((src_name, dest_name))
                else:
                    # existing properties
                    pass

    def fix_mappings(self, event_context):
        if self.mappings:
            for src_name, dest_name in self.mappings:
                if dest_name in event_context:
                    self.raise_exception("conflict property name")
                event_context[dest_name] = event_context[src_name]

    def check_parent(self, variable_context):
        # whether the parents are computed
        if self.src_variable_ids:
            for src_idx in self.src_variable_ids:
                if variable_context[src_idx] is None:
                    return False

        return True

    def process(self, event_context, variable_context):
        if not self.check_parent(variable_context):
            return

        ret = self.do_process(event_context, variable_context)
        if self.mappings:
            self.fix_mappings(event_context)

        variable_context[self.idx] = ret

    def do_process(self, event_context, variable_context):
        """

        :param event_context: all the properties of the event data in this round
        :param variable_context: all the variables data in this round
        :return: this variable data calculated by subclasses
        """
        pass


class EventNode(Node):

    def __init__(self, variable, idx_map):
        super(EventNode, self).__init__(variable, idx_map)

    def do_process(self, event_context, variable_context):
        if self.name == event_context.get("name"):
            return 1
        else:
            return None


class FilterNode(Node):

    def __init__(self, variable, idx_map):
        super(FilterNode, self).__init__(variable, idx_map)
        self.condition = variable.get("config", {}).get("condition", {})
        if self.name.lower().find('incident') != -1:
            self.is_incident = True
        else:
            self.is_incident = False

    def do_process(self, event_context, variable_context):
        if not eval_condition(self.condition, event_context):
            return None

        if self.is_incident:
            if event_context.get('notices'):
                self.compute_risk_incident(event_context)

        return 1
      
    def check_parent(self, variable_context):
        if self.is_incident:
            # whether the parents are computed
            if self.src_variable_ids:
                computed = 0
                for src_idx in self.src_variable_ids:
                    if variable_context[src_idx] is not None:
                        computed += 1
                if computed == 1:
                    return True
                else:
                    return False
        else:
            # whether the parents are computed
            if self.src_variable_ids:
                for src_idx in self.src_variable_ids:
                    if variable_context[src_idx] is None:
                        return False
        return True

    def compute_risk_incident(self, event):
        strategies_weigh = utils.Strategies_Weigh
        second = 1000

        # 获取stat dict中incident维度的ip、did、user、total字典
        ip_dimension = cache.Stat_Dict.get('ip', {})
        did_dimension = cache.Stat_Dict.get('did', {})
        user_dimension = cache.Stat_Dict.get('user', {})
        total_dimension = cache.Stat_Dict.get('total', {})
        total_all_dict = total_dimension.get('all', {})

        # 获取ip维度计算变量
        ip = event.get("c_ip")
        ip_variables = ip_dimension.get(ip, {})
        ip_scene_var = 'ip__visit__scene_incident_count__1h__slot'
        ip_strategy_var = 'ip__visit__scene_incident_count_strategy__1h__slot'
        ip_tag_var = 'ip__visit__tag_incident_count__1h__slot'
        ip_peak_var = 'ip__visit__incident_max_rate__1h__slot'
        ip_normal_tag_var = "ip__visit__tag_dynamic_count__1h__slot" # 临时将2.8需求统计tag放到这里
        ip_scenes = ip_variables.get(ip_scene_var, {})
        ip_strategies = ip_variables.get(ip_strategy_var, {})
        ip_tags = ip_variables.get(ip_tag_var, {})
        ip_peak = ip_variables.get(ip_peak_var, {'max_count': 0, 'current_count': 0,
                                                 'current_timestamp': curr_timestamp() * second})

        # 获取did维度计算变量
        did = event.get("did")
        did_variables = did_dimension.get(did, {})
        did_normal_tag_var = "did__visit__tag_dynamic_count__1h__slot" # 临时将2.8需求统计tag放到这里
        did_scene_var = 'did__visit__scene_incident_count__1h__slot'
        did_scenes = did_variables.get(did_scene_var, {})
        did_tags = did_variables.get(did_normal_tag_var, {})

        # 获取user维度计算变量
        user = event.get("uid")
        user_variables = user_dimension.get(user, {})
        user_normal_tag_var = "user__visit__tag_dynamic_count__1h__slot" # 临时将2.8需求统计tag放到这里
        user_scene_var = 'user__visit__scene_incident_count__1h__slot'
        user_scenes = user_variables.get(user_scene_var, {})
        user_tags = user_variables.get(user_normal_tag_var, {})

        notices = event.get("notices").split(',')
        hit_scenes = []
        hit_tags = []
        for strategy_name in notices:
            # 获取策略对应的场景、标签、权重等
            if strategy_name in strategies_weigh:
                strategy_weigh = strategies_weigh.get(strategy_name)
                category = strategy_weigh['category']
            else:
                continue

            # 统计ip维度策略对应的场景及命中次数
            hit_scenes.append(category)
            ip_strategy_category = ip_strategies.get(category, {})
            if strategy_name in ip_strategy_category:
                ip_strategy_category[strategy_name] += 1
            else:
                ip_strategy_category[strategy_name] = 1
            ip_strategies[category] = ip_strategy_category
            # 统计ip维度策略命中的标签次数
            strategy_tags = strategy_weigh['tags']
            hit_tags.extend(strategy_tags)

        # 统计事件命中的场景，一个事件最多命中一次
        for s in list(set(hit_scenes)):
            ip_scenes[s] = ip_scenes.get(s, 0) + 1
            did_scenes[s] = did_scenes.get(s, 0) + 1
            user_scenes[s] = user_scenes.get(s, 0) + 1

            # 统计total维度策略对应的场景及命中次数
            total_scene_var = Scene_Variable_Names[s]
            total_all_dict[total_scene_var] = total_all_dict.get(total_scene_var, 0) + 1

        # 统计事件命中的tag，一个事件最多命中一次
        for tag in list(set(hit_tags)):
            if tag:
                ip_tags[tag] = ip_tags.get(tag, 0) + 1
                user_tags[tag] = user_tags.get(tag, 0) + 1
                did_tags[tag] = did_tags.get(tag, 0) + 1

        # 统计风险事件访问峰值,事件时间按分钟间隔统计
        if event.get("timestamp") - ip_peak['current_timestamp'] > second:
            ip_peak['current_timestamp'] += second
            ip_peak['current_count'] = 1
        else:
            ip_peak['current_count'] += 1
        if ip_peak['current_count'] > ip_peak['max_count']:
            ip_peak['max_count'] = ip_peak['current_count']

        # 统计结果放回ip维度
        ip_variables[ip_scene_var] = ip_scenes
        ip_variables[ip_strategy_var] = ip_strategies
        ip_variables[ip_tag_var] = ip_tags
        ip_variables[ip_normal_tag_var] = ip_tags
        ip_variables[ip_peak_var] = ip_peak
        ip_dimension[ip] = ip_variables

        # 统计结果放回did维度
        did_variables[did_scene_var] = did_scenes
        did_variables[did_normal_tag_var] = did_tags
        did_dimension[did] = did_variables

        # 统计结果返回user维度
        user_variables[user_scene_var] = user_scenes
        user_variables[user_normal_tag_var] = user_tags
        user_dimension[user] = user_variables

        # 统计结果放回total维度
        total_dimension['all'] = total_all_dict


class AggregateNode(Node):

    def __init__(self, variable, idx_map):
        super(AggregateNode, self).__init__(variable, idx_map)

        self.condition = variable.get("config", {}).get("condition", {})
        self.reduction_type = variable.get('config', {}).get('reductions', [dict()])[0].get('type', "")
        self.reduction_name = variable.get('config', {}).get('reductions', [dict()])[0].get('srcProperty', {}).get('name', '')
        self.group_keys = [_["name"] for _ in variable.get('config', {}).get('groupedKeys', [])]
        self.sanity_check()
        self.regist_keytopvalue_hook()
        self.regist_topvalue_hook()

    def regist_keytopvalue_hook(self):
        prop_name = 'keyTopValue'
        if not (prop_name in self.variable and self.variable[prop_name]):
            return

        if not self.name.startswith('geo_count'):
            # 现在只有地理位置的keytop统计还需要统计
            return

        func = partial(count_key_top, dimension=self.dimension, variable_name=self.name)
        cache.Hook_Functions.append(func)

    def regist_topvalue_hook(self):
        prop_name = 'topValue'

        if not (prop_name in self.variable and self.variable[prop_name]):
            return

        func = partial(count_top, dimension=self.dimension, variable_name=self.name)
        cache.Hook_Functions.append(func)

    def sanity_check(self):
        if self.group_keys:
            if Dimension_Mappings[self.group_keys[0]] != self.dimension:
                self.raise_exception("the first group key should equal to the dimension, group key: {} and dimension: {}".format(self.group_keys[0], self.dimension))

        if not self.reduction_type:
            self.raise_exception("no reduction type")

        if not self.reduction_name:
            self.raise_exception("no reduction name")

    def get_group_values(self, event_context):
        group_values = []
        for key in self.group_keys:
            # 这里会产生为None的属性统计的key, 不应该持久化? 但是最后报警log?
            value = event_context.get(key, None)
            group_values.append(value)

        return group_values

    def do_process(self, event_context, variable_context):
        if not eval_condition(self.condition, event_context):
            return None

        group_values = self.get_group_values(event_context)

        if self.group_keys:
            cache_keys = [self.dimension, group_values[0], self.name]
            cache_keys.extend(group_values[1:])
        else:
            cache_keys = [self.dimension, 'all', self.name]

        value_parent = cache.Stat_Dict
        for cache_key in cache_keys[:-1]:
            value_parent = value_parent.setdefault(cache_key, {})
        last_key = cache_keys[-1]

        reduction_value = event_context.get(self.reduction_name, None) or None

        result = None
        if self.reduction_type == "stringcount":
            result = value_parent[last_key] = value_parent.get(last_key, 0) + 1
        elif self.reduction_type == "doublesum":
            result = value_parent[last_key] = value_parent.get(last_key, 0) + event_context[self.reduction_name]
        elif self.reduction_type == "stringlistdistinctcount":
            result = value_parent.setdefault(last_key, set())
            if reduction_value:
                result.update(reduction_value.split(","))
        elif self.reduction_type.endswith('distinctcount'):
            result = value_parent.setdefault(last_key, set())
            if reduction_value:
                result.add(reduction_value)
        elif self.reduction_type.endswith('longmin'):
            result = value_parent[last_key] = min(value_parent.get(last_key, 99999999999999), reduction_value)
        elif self.reduction_type.endswith('latest'):
            if reduction_value and value_parent.get(last_key, {}).get('timestamp', 0) < event_context.get('timestamp', 0):
                result = value_parent[last_key] = {
                    'timestamp': event_context['timestamp'],
                    'value': reduction_value}
        else:
            self.raise_exception("invalid reduction type")

        return result


class DualNode(Node):

    def __init__(self, variable, idx_map):
        super(DualNode, self).__init__(variable, idx_map)
        self.first_variable = variable.get("config", {}).get("firstVariable", [("", "")])[1]
        self.first_variable_idx = idx_map[self.first_variable]
        self.second_variable = variable.get("config", {}).get("secondVariable", [("", "")])[1]
        self.second_variable_idx = idx_map[self.second_variable]

        self.value_property = variable.get("config", {}).get("valueProperty", {}).get("name", "")
        self.condition = variable.get("config", {}).get("condition", {})
        self.operation = variable.get("config", {}).get("operation", {})
        self.sanity_check()
        self.regist_hook()

    def regist_hook(self):
        eval_opts = dict(
            operator=self.operation,
            first_operate=self.first_variable,
            second_operate=self.second_variable,
        )
        func = partial(count_dualvar,
                       dimension=self.dimension,
                       variable_name=self.name,
                       eval_opts=eval_opts
                       )
        cache.Hook_Functions.append(func)

    def sanity_check(self):
        if not self.first_variable or not self.second_variable:
            self.raise_exception("null first or second variable")

        if not self.value_property:
            self.raise_exception("null value property")

        if not self.operation:
            self.raise_exception("null operation")

    def do_process(self, event_context, variable_context):
        if not eval_condition(self.condition, event_context):
            return None
        return None

        ret = eval_statement(self.operation, variable_context[self.first_variable_idx], variable_context[self.second_variable_idx])
        return ret


class DAG(object):
    def __init__(self):
        self.variables = []
        self.priority_map = dict()
        pass

    def add_variables(self, raw_variables):
        # remove sequence
        raw_variables_map = {_["name"]: _ for _ in raw_variables}

        # 1. generate priority map
        for v in raw_variables:
            name = v["name"]
            if name in self.priority_map:
                print ("redundant variable")
                sys.exit(-1)

            p = v["priority"]
            self.priority_map[name] = p

        # sanity check to see if there is circle in the dag
        for v in raw_variables:
            name = v["name"]
            src_names = [_[1] for _ in v.get("srcVariablesID", [])]

            this_priority = self.priority_map[name]
            if any(map(lambda src_name: self.priority_map[src_name] >= this_priority, src_names)):
                raise Exception("invalid priorities, there may be circle in the DAG")

        # sort the variables
        variables_with_priority = self.priority_map.items()
        variables_with_priority.sort(key=lambda x: x[1])

        # [(varname, index), ]
        variables_with_idx = [(var_and_priority[0], idx) for idx, var_and_priority in enumerate(variables_with_priority)]

        # {varname : idx}
        variables_with_idx_map = {varname: idx for varname, idx in variables_with_idx}

        # remove sequencevalue
        variables_with_priority = filter(lambda v: raw_variables_map[v[0]]["type"] != "sequencevalue", variables_with_priority)

        self.variables = [self._gen_variable_node(raw_variables_map.get(variable_priority[0]), variables_with_idx_map) for index, variable_priority in enumerate(variables_with_priority)]
        self.variable_names = [_.name for _ in self.variables]

    def _gen_variable_node(self, variable, idx_map):
        if not variable:
            raise Exception("null variable in dag")
        var_type = variable["type"]
        if var_type == "dualvar":
            return DualNode(variable, idx_map)
        elif var_type == "aggregate":
            return AggregateNode(variable, idx_map)
        elif var_type == "filter":
            return FilterNode(variable, idx_map)
        elif var_type == "event":
            return EventNode(variable, idx_map)
        else:
            raise Exception("unsupported variable type: {}, name: {}".format(var_type, variable["name"]))

    def get_variables(self):
        return self.variables

    def process_event(self, event):
        event_context = dict(event)
        variable_context = [None for _ in self.variable_names]
        for _ in self.variables:
            _.process(event_context, variable_context)

    def send_to_compute_flow(self, event):
        # compatible to old implementation
        self.process_event(event)


DEBUG_PREFIX = "==============="
ALL_STAT_KEY = 'all'
def count_dualvar( cache_dict, variable_name, dimension, eval_opts):
    #    logger.debug(u"进入双变量的钩子函数计算中..")
    dim_cache_dict = cache_dict[dimension]
    operator = eval_opts['operator']
    first_operate = eval_opts['first_operate']
    second_operate = eval_opts['second_operate']
    # 填充双变量计算结果
    for key, stat_dict in dim_cache_dict.iteritems():
        if stat_dict.has_key(variable_name):
            pass
        else:
            stat_dict[variable_name] = eval_statement(operator,
                                                      stat_dict.get(first_operate, 0),
                                                      stat_dict.get(second_operate, 0),)

def count_key_top( cache_dict, variable_name, dimension):
    logger.debug(DEBUG_PREFIX + u' 计算key top中...维度: %s, 统计的变量名:%s', dimension, variable_name)
    dim_cache_dict = cache_dict[dimension]

    # 统计生成中间结果
    # {var_name1: dict, var_name2:dict}
    temp_dict = dict()
    for key, stat_dict in dim_cache_dict.iteritems():
        if stat_dict.has_key(variable_name):
            if not temp_dict.has_key(variable_name):
                temp_dict[variable_name] = dict()
            dict_merge(temp_dict, {variable_name:stat_dict[variable_name]})

    # 填充统计结果
    for var_name, var_dict in temp_dict.iteritems():
        if dim_cache_dict.has_key('all') and dim_cache_dict['all'].has_key(variable_name):
            logger.error(u'维度: %s, 要统计的"all"key下面的变量 %s已经存在了, 原来:%s, 回调函数生成:%s', dimension, variable_name, dim_cache_dict['all'][variable_name], var_dict)
        else:
            if not dim_cache_dict.has_key('all'):
                dim_cache_dict['all'] = dict()
            # @todo 存储修改
            dim_cache_dict['all'][variable_name] = var_dict

def count_top(cache_dict, variable_name, dimension):
    logger.debug(DEBUG_PREFIX + u' 计算top中...维度: %s, 统计的变量名:%s', dimension, variable_name)
    dim_cache_dict = cache_dict[dimension]

    if variable_name == 'ip_distinctcount_top_user':
        variable_name

    # 统计生成中间结果
    # {key1: count, key2:count}
    temp_dict = dict()
    for key, stat_dict in dim_cache_dict.iteritems():
        if stat_dict.has_key(variable_name):
            if not temp_dict.has_key(key):
                temp_dict[key] = stat_dict[variable_name]
            else:
                dict_merge(temp_dict, {key:stat_dict[variable_name]})

    # 填充统计结果 @todo 存储修改
    if not dim_cache_dict.has_key('all'):
        dim_cache_dict['all'] = dict()
    if not dim_cache_dict['all'].has_key(variable_name):
        dim_cache_dict['all'][variable_name] = temp_dict
    else:
        if isinstance(dim_cache_dict['all'][variable_name], set):
            dim_cache_dict['all'][variable_name].update(temp_dict.get('all', set()))
        elif isinstance(dim_cache_dict['all'][variable_name], dict):
            dict_merge(dim_cache_dict['all'][variable_name], temp_dict)
#from data import *


#def main():
#    dag = DAG()
#    variables = json.loads(data)
#    dag.add_variables(variables)
#    variables.sort(key=lambda v: v["priority"])
#    for _ in variables:
#        Node(_)
#        # 初始化DAG
#        # dag = ComputeDAG()
#        # dag.add_nodes([])
#        #
#        # # 然后开始不断获取log
#        # record = None #获取一条log
#        # input_event = record
#        # compute_flow = dag.gen_compute_flow(record.app, record.name)
#        # for func in compute_flow:
#        #     output_event = func.compute(input_event)
#        #     input_event = output_event

#main()
