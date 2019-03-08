# -*- coding: utf-8 -*-

import logging, traceback, sys, heapq
from itertools import ifilter
from functools import partial
from threathunter_common.util import curr_timestamp

logging.basicConfig(level=logging.DEBUG)

from . import cache
from .utils import get_dimension, dict_merge
from .condition import eval_condition, eval_statement
from nebula_utils.persist_utils.utils import get_strategies_weigh
#from nebula_utils.persist_utils.utils import Storage

TOP_SIZE = 100 # top榜单存储的前n位

DEBUG_PREFIX = "==============="
ALL_STAT_KEY = 'all'
logger = logging.getLogger("nebula_utils.persist_compute.process")

class Storage(dict):
    """
    A Storage object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`.
    
        >>> o = storage(a=1)
        >>> o.a
        1
        >>> o['a']
        1
        >>> o.a = 2
        >>> o['a']
        2
        >>> del o.a
        >>> o.a
        Traceback (most recent call last):
            ...
        AttributeError: 'a'
    
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, key): 
        try:
            return self[key]
        except KeyError, k:
            raise AttributeError, k
    
    def __setattr__(self, key, value): 
        self[key] = value
    
    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError, k:
            raise AttributeError, k
    
    def __repr__(self):     
        return dict.__repr__(self)


VALID_COMPUTE_TYPE = ('filter', 'aggregate', 'dualvar', 'sequencevalue')

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
#    for key, count in temp_dict.iteritems():
#        if dim_cache_dict.has_key('all') and dim_cache_dict['all'].has_key(variable_name):
#            logger.error(u'维度: %s, 要统计的"all"key下面的变量 %s已经存在了, 原来:%s, 回调函数生成:%s', dimension, variable_name, dim_cache_dict['all'][variable_name], count)
#        else:
#            if not dim_cache_dict.has_key('all'):
#                dim_cache_dict['all'] = dict()
#            if not dim_cache_dict['all'].has_key(variable_name):
#                dim_cache_dict['all'][variable_name] = count
#            # @todo 存储修改
#            dim_cache_dict['all'][variable_name][key] = count
    
#    top = heapq.nlargest(TOP_SIZE, items, key=lambda x:x[1])

class Process(object):
    """
    计算处理的基础类, 只做公共运算过程的接口声明用
    """
    def __init__(self):
        pass
        
    def condition_fulfill(self, event):
        cond = self.get('config', {}).get('condition', None)
        return eval_condition(cond, event)

    def regist_keytopvalue_hook(self):
        prop_name = 'keyTopValue'
        if not (self.has_key(prop_name) and self[prop_name]):
            return
        
        if not self['name'].startswith('geo_count'):
            # 现在只有地理位置的keytop统计还需要统计
            return
        dimension = self['name'].split('__')[0]
        # need dimension, variable_name
        # 来源 Stat_Dict
        # dimension: key: variable_name: { key1:count2, key2:count2}
        # 结果 Stat_Dict
        # dimension: key: variable_name: [(key1:count1, key2:count2)]
        func = partial(count_key_top, dimension=dimension, variable_name=self['name'])
        cache.Hook_Functions.append(func)

    def regist_topvalue_hook(self):
        prop_name = 'topValue'
        
        if not (self.has_key(prop_name) and self[prop_name]):
            return
        
        dimension = self['name'].split('__')[0]
        # need dimension, variable_name
        # 来源 Stat_Dict
        # dimension: key: variable_name: count
        # 结果 Stat_Dict
        # dimension: 'all': variable_name: [(key1:count1, key2:count2)]

        func = partial(count_top, dimension=dimension, variable_name=self['name'])
        cache.Hook_Functions.append(func)
        
    def inherit_event_props(self, src_event):
        dst_event = Storage()
        dst_event.name = self['name']
        dst_event.app = src_event['app']
        dst_event.key = src_event.get('key', None)
        dst_event.value = src_event.get('value', None)
        dst_event.timestamp = src_event['timestamp']
        for prop in self.get('properties', []):
            prop_name = prop['name']
            dst_event[prop_name] = src_event.get(prop_name, None)
        
#        logger.debug('from to event props %s, %s', src_event.name, dst_event.name)
#        if src_event.name == 'httplog' and dst_event.name=='click':
#            logger.debug( 'http->click %s',dst_event)
#        if src_event.name == 'click' and dst_event.name=="click_timediff_ip":
#            logger.debug( 'click->timediff %s',dst_event)
#        if src_event.name == "click_timediff_ip" and dst_event.name=="click_timediff_tooshort_count_ip":
#            logger.debug('timediff %s',dst_event)

        return dst_event

    def get_compute_dict(self, event):
        """
        根据group_keys拿到需要统计的字典的引用和统计项的key
        
        依赖于cache的统计字典的每层的统计key的层次
        """
        try:
            group_keys = self['config']['groupedKeys']
        except KeyError as e:
            # 如果实例没有config属性时:
            logger.critical('Process计算类必须跟ComputeVariable mixin之后实例化之后才能使用')
            raise KeyError, e.message
        
        group_values = []
        for key in group_keys:
            value = event.get(key['name'], None) # 这里会产生为None的属性统计的key, 不应该持久化? 但是最后报警log?
            group_values.append(value)

        stat_name = self['name']
        # 这里以下假定group_keys至少有一个
        if group_keys:
            dimension = get_dimension(group_keys[0]['name'])
            if dimension is None:
                logger.error('该变量:%s 不能获得对应维度: %s', self['name'], group_keys[0]['name'])
            # 缓存的统计字典的各层的key
            cache_keys = [dimension, group_values[0], stat_name, ]
            cache_keys.extend(group_values[1:])
        else:
            dimension = self['name'].split('__')[0]
            if not dimension:
                logger.error('该变量:%s 没有groupKeys且不能从name中获得对应维度', self['name'])
            # 缓存的统计字典的各层的key
            cache_keys = [dimension, 'all', stat_name, ]

        temp_dict = cache.Stat_Dict
        next_counter = None
        next_key = None
        depth = len(cache_keys)
        index_start = 0
        stat_name = self['config']['reductions'][0]['srcProperty']['name']
        for index, key in enumerate(cache_keys, index_start):
            if index == depth - 1:
                # 拿到倒数第二层的统计数据结构就行
#                if self.reduction_type == "stringlistdistinctcount":
#                    next_key = key
                if self.reduction_type.endswith('distinctcount'):
                    if not temp_dict.has_key(key):
                        next_counter = temp_dict[key] = set()
                        next_key = event[stat_name]
                    else:
                        next_counter = temp_dict[key]
                        next_key = event[stat_name]

                if self.reduction_type.endswith('stringcount'):
                    #next_counter = temp_dict[key] = dict()
                    next_key = key

                if self.reduction_type.endswith('longmin'):
                    next_key = event[stat_name]
            else:
                if not temp_dict.has_key(key):
                    next_counter = temp_dict[key] = dict()
                else:
                    next_counter = temp_dict[key]
                temp_dict = next_counter
            
        return next_counter, next_key
        
    def compute(self):
        raise NotImplementedError
        
class EventProcess(Process):
    def compute(self, event):
#        ret_event = self.inherit_event_props(event)
#        logger.info('Event Process(app:%s, name:%s) in', self['app'], self['name'])
#        logger.info('Event: %s', event)
        return event

class FilterProcess(Process):
    """
    filter类型的变量计算类
    """
    def compute(self, event):
        """
        event 中的属性直接映射到输出的event中, 没有验证mapping两边类型
        """
#        logger.info('Filter Process(app:%s, name:%s) in', self['app'], self['name'])
        if event is None or not self.condition_fulfill(event):
            return None
        ret_event = self.inherit_event_props(event)
        
        mappings = self.get('config',dict()).get('mappings', [])

        for m in mappings:
            prop = getattr(event, m['srcProperty']['name'], '')
            dest_prop_name = m['destProperty']['name']
            setattr(ret_event, dest_prop_name, prop)
        
#        logger.info('Event: %s', ret_event)

        # 如果filter为incident类型,则统计风险事件命中策略、命中标签、风险值等
        if self['name'].lower().find('incident') != -1:
            self.compute_risk_incident(event)

        return ret_event

    def compute_risk_incident(self, event):
        minute = 60000

        # 获取stat dict中incident维度的ip、did、user字典
        ip_dimension = cache.Stat_Dict.get('ip', {})
        did_dimension = cache.Stat_Dict.get('did', {})
        user_dimension = cache.Stat_Dict.get('user', {})

        # 获取ip维度计算变量
        ip = event.c_ip
        ip_variables = ip_dimension.get(ip, {})
        ip_scene_var = 'ip__visit__scene_incident_count__1h__slot'
        ip_strategy_var = 'ip__visit__scene_incident_count_strategy__1h__slot'
        ip_tag_var = 'ip__visit__tag_incident_count__1h__slot'
        ip_peak_var = 'ip__visit__incident_max_rate__1h__slot'
        ip_scenes = ip_variables.get(ip_scene_var, {})
        ip_strategies = ip_variables.get(ip_strategy_var, {})
        ip_tags = ip_variables.get(ip_tag_var, {})
        ip_peak = ip_variables.get(ip_peak_var, {'max_count': 0, 'current_count': 0,
                                                 'current_timestamp': curr_timestamp() / 60 * minute})

        # 获取did维度计算变量
        did = event.did
        did_variables = did_dimension.get(did, {})
        did_scene_var = 'did__visit__scene_incident_count__1h__slot'
        did_scenes = did_variables.get(did_scene_var, {})

        # 获取user维度计算变量
        user = event.uid
        user_variables = user_dimension.get(user, {})
        user_scene_var = 'user__visit__scene_incident_count__1h__slot'
        user_scenes = user_variables.get(user_scene_var, {})

        strategies_weigh = get_strategies_weigh()
        notices = event.notices.split(',')
        for strategy_name in notices:
            # 获取策略对应的场景、标签、权重等
            if strategy_name in strategies_weigh:
                strategy_weigh = strategies_weigh.get(strategy_name)
                category = strategy_weigh['category']
            else:
                continue

            # 统计ip维度策略对应的场景及命中次数
            if category in ip_scenes:
                ip_scenes[category] += 1
            else:
                ip_scenes[category] = 1
            ip_strategy_category = ip_strategies.get(category, {})
            if strategy_name in ip_strategy_category:
                ip_strategy_category[strategy_name] += 1
            else:
                ip_strategy_category[strategy_name] = 1
            ip_strategies[category] = ip_strategy_category
            # 统计ip维度策略命中的标签次数
            strategy_tags = strategy_weigh['tags']
            for tag in strategy_tags:
                if tag in ip_tags:
                    ip_tags[tag] += 1
                else:
                    ip_tags[tag] = 1

            # 统计did维度策略对应的场景及命中次数
            if category in did_scenes:
                did_scenes[category] += 1
            else:
                did_scenes[category] = 1

            # 统计user维度策略对应的场景及命中次数
            if category in user_scenes:
                user_scenes[category] += 1
            else:
                user_scenes[category] = 1

        # 统计风险事件访问峰值,事件时间按分钟间隔统计
        if event.timestamp - ip_peak['current_timestamp'] > minute:
            ip_peak['current_timestamp'] += minute
            ip_peak['current_count'] = 1
        else:
            ip_peak['current_count'] += 1
        if ip_peak['current_count'] > ip_peak['max_count']:
            ip_peak['max_count'] = ip_peak['current_count']

        # 统计结果放回ip维度
        ip_variables[ip_scene_var] = ip_scenes
        ip_variables[ip_strategy_var] = ip_strategies
        ip_variables[ip_tag_var] = ip_tags
        ip_variables[ip_peak_var] = ip_peak
        ip_dimension[ip] = ip_variables

        # 统计结果放回did维度
        did_variables[did_scene_var] = did_scenes
        did_dimension[did] = did_variables

        # 统计结果返回user维度
        user_variables[user_scene_var] = user_scenes
        user_dimension[user] = user_variables


class DualVarProcess(Process):
    def isFirstVariable(self, event):
        event_id_list = ComputeVariable.get_id_list(event)
        return ComputeVariable.is_id_match( event_id_list, self['config']['firstVariable'])
        
    def isSecondVariable(self, event):
        event_id_list = ComputeVariable.get_id_list(event)
        return ComputeVariable.is_id_match( event_id_list, self['config']['secondVariable'])
        
    def getDualvarPropName(self):
        return self['config']['valueProperty']['name']
        
    def regist_dualvar_hook(self):
        if not self['type'] == 'dualvar':
            return
        dimension = self['name'].split('__')[0]
        
        eval_opts = dict(
            operator = self['config']['operation'],
            first_operate = self['config']['firstVariable'][-1],
            second_operate = self['config']['secondVariable'][-1],
        )
        func = partial(count_dualvar,
                       dimension=dimension,
                       variable_name=self['name'],
                       eval_opts=eval_opts
        )
#        logger.debug(DEBUG_PREFIX+u"增加了双变量 %s 的钩子函数, 参数是dimension:%s, eval_opts:%s", self['name'], dimension, eval_opts)
        
        cache.Hook_Functions.append(func)
        
        
    def compute(self, event):
#        logger.info('DualVar Process(app:%s, name:%s) in', self['app'], self['name'])
        # 不同于java是注册一个查询时计算的函数
        # 离线有时只需要注册一个全局的钩子, 最后算一次就行了
        if event is None or not self.condition_fulfill(event) \
           or self.get('config', None) is None:
            return None
        
        ret_event = self.inherit_event_props(event)

        if self['registed']:
            return

#
#        prop_name = self.getDualvarPropName()
#        # 找去哪些key 找数据源
#        if self.isFirstVariable(event):
#            if not self['first_value_set']:
#                self['first_value'] = event.get(prop_name)
#                self['first_value_set'] = True
#                
#        if self.isSecondVariable(event):
#            if not self['second_value_set']:
#                self['second_value'] = event.get(prop_name)
#                self['second_value_set'] = True
#
#        # 计算
#        if self['first_value_set'] and self['second_value_set']:
#            operator = self['config']['operation']
#            ret_event[prop_name] = eval_statement(operator, self['first_value'], self['second_value'])
#            self['first_value_set'] = False
#            self['second_value_set'] = False
            
        
        # 如果是可以一次性计算的
        # 注册钩子函数
        if not self['registed']:
            self.regist_dualvar_hook()
            self['registed'] = True
        
#        logger.info('Event: %s', ret_event)
        return ret_event

class AggregateProcess(Process):
    def compute(self, event):
#        logger.info('Aggregate Process(app:%s, name:%s) in', self['app'], self['name'])
        
        # 调试特定的变量的计算方法, 然后在if内打个断点来:
        if self['name'] == "alarm_distinctcount_top_ip":
            self['name']

        if event is None or not self.condition_fulfill(event) \
           or self.get('config', None) is None:
            # 聚合变量没有config来做聚合就肯定pass
            return None
        
        ret_event = self.inherit_event_props(event)
        
        self.reduction_type = self['config']['reductions'][0]['type']
        
        stat_counter, stat_key = self.get_compute_dict(event)
        
        if self.reduction_type == 'stringcount':
            try:
                # stat_counter is a dict
                if not stat_counter.has_key(stat_key):
                    stat_counter[stat_key] = 1
                else:
                    stat_counter[stat_key] += 1
            except AttributeError as e:
                raise Exception, e.message
                
        elif self.reduction_type == "stringlistdistinctcount":
            if stat_key:
                stat_counter.update(stat_key.split(','))
        elif self.reduction_type.endswith('distinctcount'):
            # stat_counter is a set
            # @未来 如果存储需要优化，且不需要合并, 这里可以只存个值，加个钩子函数
            stat_counter.add(stat_key)
        elif self.reduction_type.endswith('longmin'):
            if stat_counter.has_key(self['name']):
                stat_counter[self['name']] = min(stat_counter[self['name']], stat_key)
            else:
                stat_counter[self['name']] = stat_key
            
        # 如果是可以一次性计算的
        # 注册钩子函数
        if not self['registed']:
            self.regist_keytopvalue_hook()
            self.regist_topvalue_hook()
            self['registed'] = True
            
#        logger.info('Event: %s', ret_event)
        return ret_event

class SequenceValueProcess(Process):
        
    def fullfill_condition(self, operator, left, right):
        return eval_statement(operator, left, right)

    def fullfill_first_condition(self, last_value, event):
        try:
            if self['config']['secondCondition'] == '>0':
                return self.fullfill_condition('>', last_value, 0)
        except KeyError:
            # 如果对双值计算变量没有设置变量就默认返回True
            return True

    def fullfill_second_condition(self, value, event):
        # @未来 更通用
        try:
            if self['config']['firstCondition'] == '>0':
                return self.fullfill_condition('>', value, 0)
        except KeyError:
            # 如果对双值计算变量没有设置变量就默认返回True
            return True

    def compute(self, event):
#        logger.info('SequenceValue Process(app:%s, name:%s) in', self['app'], self['name'])
        if event is None or not self.condition_fulfill(event) \
           or self.get('config', None) is None:
            # sequence变量没有config来说明前后值如何计算肯定就pass
            return None
            
            
        ret_event = self.inherit_event_props(event)
        
        self.reduction_type = None
#        self.reduction_type = self['config']['reductions'][0]['type']
        # 怎么存cache? groupedkeys @todo 特殊的赋值类型

        prop_name = self['config']['valueProperty']['name']
        if self.last_value is None:
            if self.fullfill_first_condition(event[prop_name], event):
                self.last_value = event[prop_name]
        else:
            if self.fullfill_second_condition(event[prop_name], event):
                time_diff = eval_statement(
                    self['config']['operation'],
                    event[prop_name],
                    self.last_value,)
                ret_event.value = time_diff
            self.last_value = event[prop_name]

#        logger.info('Event: %s', ret_event)
        return ret_event

class ComputeVariable(dict):
    """
    A ComputeVariable object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`.
    
        >>> o = ComputeVariable(a=1)
        >>> o.a
        1
        >>> o['a']
        1
        >>> o.a = 2
        >>> o['a']
        2
        >>> del o.a
        >>> o.a
        Traceback (most recent call last):
            ...
        AttributeError: 'a'
    
    """
    def __init__(self, *args, **kwargs):
        
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, key): 
        try:
            return self[key]
        except KeyError, k:
            raise AttributeError, k
    
    def __setattr__(self, key, value): 
        self[key] = value
    
    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError, k:
            raise AttributeError, k
    def __repr__(self):     
        return '<Compute_Variable ' + dict.__repr__(self) + '>'
    
    @classmethod
    def get_id_list(cls, variable):
        if not variable.has_key('app') or not variable.has_key('name'):
            return None
        return [variable['app'], variable['name']]
        
    @classmethod
    def is_id_match(cls, src_id_list, dst_id_list):
        if not (len(src_id_list) == 2 and isinstance(src_id_list, list)):
            raise ValueError, 'src_id_list is not valid variable id format' 
        if not (len(dst_id_list) == 2 and isinstance(dst_id_list, list)):
            raise ValueError, 'dst_id_list is not valid variable id format' 
        
        if src_id_list[0] == dst_id_list[0] and \
           src_id_list[1] == dst_id_list[1]:
            return True
            
        return False
        
    def get_identity(self, ):
        """
        identity: app_name
        """
        if not self.has_key('app') or not self.has_key('name'):
            return None
        return '%s_%s' % (self['app'], self['name'])
        
    @classmethod
    def get_identity_srcVariablesID(cls, srcVariablesID):
        """
        返回identity从srcVariablesID list: app_name
        """
        if not isinstance(srcVariablesID, list) or len(srcVariablesID) != 2:
            raise ValueError, '不是已知的srcVariabklesID格式'
        
        return '%s_%s' % (srcVariablesID[0], srcVariablesID[1])

    @classmethod
    def get_app_name_from_identity(cls, identity):
        if not isinstance(identity,basestring):
            raise ValueError, 'invalid identity: %s type:%s' % (identity, type(identity))
        segs = identity.split('_')
        return segs[0], '_'.join(segs[1:])

class EventVariable(ComputeVariable, EventProcess ):
    def __init__(self, *args, **kwargs):
        super(ComputeVariable, self).__init__(*args, **kwargs)

class FilterVariable(ComputeVariable, FilterProcess ):
    def __init__(self, *args, **kwargs):
        super(ComputeVariable, self).__init__(*args, **kwargs)

class SequenceVariable(ComputeVariable, SequenceValueProcess ):
    def __init__(self, *args, **kwargs):
        super(ComputeVariable, self).__init__(*args, **kwargs)
        self['last_value'] = None
    
class DualVarVariable(ComputeVariable, DualVarProcess ):
    def __init__(self, *args, **kwargs):
        super(ComputeVariable, self).__init__(*args, **kwargs)
        self['registed'] = False
        self['first_value'] = None
        self['first_value_set'] = False
        self['second_value'] = None
        self['second_value_set'] = False

class AggregateVariable(ComputeVariable, AggregateProcess ):
    def __init__(self, *args, **kwargs):
        super(ComputeVariable, self).__init__(*args, **kwargs)
        self['registed'] = False

class ComputeVariableHandler(object):
    Func_Dict = {
        'event': EventVariable,
        'filter': FilterVariable,
        'aggregate': AggregateVariable,
        'dualvar': DualVarVariable,
        'sequencevalue': SequenceVariable,
    }
    @classmethod
    def get_compute_variable(cls, *args, **kwargs):
        if kwargs['type'] not in cls.Func_Dict.keys():
            raise RuntimeError, 'unsupport ComputeVariable type: %s , app: %s, name: %s' % (kwargs['type'], kwargs['app'], kwargs['name'])
        
        cla = cls.Func_Dict.get(kwargs['type'], None)
        return cla(*args, **kwargs)
    
class ComputeDAG(object):
    def __init__(self):
        """
        id format: app_name
        """
        self.ifcycle = False
        self.nodes = dict() # id: compute_variable
        self.edges = dict() # from_node_id: to_node_id_list
        self.compute_flow = dict() # id: compute_flow_list
        self.compute_cache = dict() # id: output_event
        
    def _add_node(self, node):
        """
        """
        node_id = node.get_identity()
        if self.nodes.has_key(node_id):
            # 重复
            logger.error('compute variable name: %s app: %s 已经存在当前DAG', node.app, node.name)
        else:
            self.nodes[node_id] = node

    def _add_edge_from_id(self, from_node_id, to_node_id):
        if not self.edges.has_key(from_node_id):
            self.edges[from_node_id] = [ to_node_id, ]
        else:
            self.edges[from_node_id].append(to_node_id)
        
    def _add_edge(self, from_node, to_node):
        """
        """
        from_id = from_node.get_identity()
        if not self.edges.has_key(from_id):
            self.edges[from_id] = [ to_node.get_identity(), ]
        else:
            self.edges[from_id].append(to_node.get_identity())
            
    def add_nodes(self, nodes):
        """
        API
        """
        edges = []
        # 增加node
        for node in nodes:
            self._add_node(node)
            # 先将edge记录下来, nodes 列表不一定有序
            srcVariablesID = node.get('srcVariablesID', [])
            if srcVariablesID is None:
                continue
            for src_var_id in srcVariablesID:
                try:
                    from_id = ComputeVariable.get_identity_srcVariablesID(src_var_id)
                except ValueError as e:
                    logger.error('node: %s, srcVariablesID内: %s type: %s不符合格式', node, src_var_id, type(src_var_id))
                edges.append( (from_id, node.get_identity()) )
        
#        logger.debug(DEBUG_PREFIX+u"获取的边们: %s", edges)
        # 增加 edge
        for (from_node_id, to_node_id) in edges:
            if self.nodes.has_key(from_node_id):
                if self.nodes.has_key(to_node_id):
                    # 当from_node 和 to_node都存在的时候才加这条边
                    self._add_edge_from_id(from_node_id, to_node_id)
                else:
                    app, name = ComputeVariable.get_app_name_from_identity(to_node_id)
                    logger.error('app: %s name:%s 的变量没有加入到ComputeDAG.nodes内', app, name )
            else:
                from_app, from_name = ComputeVariable.get_app_name_from_identity(from_node_id)
                to_app, to_name = ComputeVariable.get_app_name_from_identity(to_node_id)
#                logger.debug(DEBUG_PREFIX+"from:%s, to:%s", from_node_id, to_node_id)
                logger.error(u'app: %s name:%s 的变量没有加入到ComputeDAG.nodes内, 不能被app:%s name: %s 的变量引用', from_app, from_name, to_app, to_name )
        
        # 每个node的指向节点队列按优先级从小到大排序
        for from_node_id, to_node_list in self.edges.iteritems():
            to_node_list.sort(key=lambda x:self.nodes.get(x).priority)

        # 最后检查
        if self.if_cycle():
            self.ifcycle = True

    def send_to_compute_flow(self, event):
        """
        计算入口函数, 将解析的日志扔进来
        """
        if self.ifcycle:
            logger.error(u'该计算DAG很有可能有环存在，需要检查')
            return None
        
        identity = ComputeVariable.get_identity_srcVariablesID([event['app'], event['name']])
        if not self.nodes.has_key(identity):
            logger.error('app: %s, name:%s do not have compute flow', event['app'], event['name'])
            logger.error('compute DAG: %s', self.edges)
            return
            
        compute_variable = self.nodes.get(identity)
        src_events = { identity:compute_variable.compute(Storage(**event)) }
        next_level_events = dict()
        
#        logger.debug(u'派发完root节点,开始往下派发前')
#        logger.debug(u'起始派发的事件们: %s', src_events)
        # 逐层计算, 缓存每层的结果再向下传递结果继续计算, 避免发生计算过程优先级翻转的问题
        while src_events:
            for i, src_event in src_events.iteritems():
                childs = self.edges.get(i, [])
#                logger.debug(u'拿到的下级节点们: %s', childs)
                for ch_id in childs:
                    ch = self.nodes.get(ch_id)
                    try:
                        if ch.name.endswith('page'):
                            # 只为了快速定位调试的计算variable
                            ch.name
                        output_event = ch.compute(src_event)
#                        if ch.name == 'click':
#                            if output_event is None:
#                                logger.debug(DEBUG_PREFIX+DEBUG_PREFIX+ '输入给click的event:%s 但是click log产生为空... ', src_event)
#                            else:
#                                logger.debug(DEBUG_PREFIX+DEBUG_PREFIX+ '输入给click的event:%s 产生为:%s ', src_event, output_event)
                    except Exception:
                        logger.error('Error happen, Input Event data is:%s, Compute Variable is %s', src_event, ch.name)
                        traceback.print_exc()
                        raise Exception
                    next_level_events[ch_id] = output_event
#                logger.debug(u'往下派发的事件们: %s', next_level_events)
            src_events = next_level_events
            next_level_events = dict()

    def gen_compute_flows(self, app, name):
        """
        是不是改拿的时候, 拿全了再按优先级排序
        """
        if self.ifcycle:
            logger.error('该计算DAG很有可能有环存在，需要检查')
            return None

        identity = ComputeVariable.get_identity_srcVariablesID([app, name])
        if self.compute_flow.has_key(identity):
            # 如果已经生成过了则返回cache
            return self.compute_flow[identity]
            
        if not self.edges.has_key(identity):
            logger.error('对应variable没有统计变量,app: %s name: %s', app, name)
            logger.error('允许的 %s', self.edges)
            return
            
        compute_flow_identitys = [ _ for _ in self.get_sub_compute_flows(identity) if _]
        # 计算流按优先级 从小到大排列
        compute_flow = [ self.nodes[_] for _ in compute_flow_identitys ]
        compute_flow.sort(key=lambda x: x.priority)
        self.compute_flow[identity] = compute_flow
        return compute_flow
        
    def get_sub_compute_flows(self, identity):
        if not self.edges.has_key(identity):
            return None
        else:
            sub_nodes = self.edges.get(identity)
            return map(self.get_sub_compute_flows, sub_nodes)

    def if_cycle(self):
        """
        测试是否有环
        from的优先级应该总是小于 to的优先级
        """
        for from_node_id, to_node_ids in self.edges.iteritems():
            from_node = self.nodes[from_node_id]
            to_nodes = ( self.nodes[_] for _ in to_node_ids )
#            logger.debug('parent node: \napp:%s name:%s priority:%s', from_node.app, from_node.name, from_node.priority)
#            logger.debug('child nodes: ')
#            [ logger.debug('app:%s name:%s priority:%s', node.app, node.name, node.priority) for node in to_nodes]
            for to_node in to_nodes:
                if from_node.priority >= to_node.priority:
                    # 如果有任何from节点的优先级大于等于to节点的优先级, 则很有可能有环
                    logger.critical(u'父计算变量:app: %s name: %s 的优先级: %s 大于子计算变量 app: %s name: %s priority: %s, 有可能成一个环, 建立失败.', from_node.app, from_node.name, from_node.priority, to_node.app, to_node.name, to_node.priority)
                    
                    return True
                
        return False
        
def main():
    # 初始化DAG
    dag = ComputeDAG()
    dag.add_nodes([])
    
    # 然后开始不断获取log
    record = None #获取一条log
    input_event = record
    compute_flow = dag.gen_compute_flow(record.app, record.name)
    for func in compute_flow:
        output_event = func.compute(input_event)
        input_event = output_event