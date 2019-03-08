#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
变量的定义
"""

import json
from collections import OrderedDict

from .util import text, unicode_list, unicode_dict
from .variable_filter import Filter
from .variable_function import Function, get_calculator_result_type, get_supported_period_types
from .value_type import is_value_type_supported
from .event_model import get_event_fields_mapping_from_registry, get_all_events_in_dict
from .property_model import Property


OBJECT_CATEGORY_MAPPING = {
    'uid': 'uid',
    'c_ip': 'ip',
    'did': 'did',
}


class VariableModel(object):

    def __init__(self, module, app, name, remark, visible_name, dimension, status, type, value_type, value_subtype,
                 value_category, source, filter, period, function, groupbykeys, hint=None):
        self._module = text(module or '')
        self._app = text(app or '')
        self._name = text(name or '')
        self._remark = text(remark or '')
        self._visible_name = text(visible_name or '')
        self._dimension = text(dimension or '')
        self._status = text(status or '')
        self._type = text(type or '')
        self._value_type = text(value_type or '')
        self._value_subtype = text(value_subtype or '')
        self._value_category = text(value_category or '')
        self._source = unicode_list(source or list())
        self._filter = Filter.from_dict(filter or {})
        self._period = unicode_dict(period or dict())
        self._function = Function.from_dict(function or {})
        self._groupbykeys = unicode_list(groupbykeys or [])
        self._hint = unicode_dict(hint or dict())

        # 内部使用
        self._source_mappings = dict()

        # 内部自动检查和修正
        self._normalize()

    def sanity_check(self):
        if self.module not in {'base', 'realtime', 'slot', 'profile'}:
            raise RuntimeError('{}不是正确的模块配置'.format(self.module))

        if not self.name:
            raise RuntimeError('变量名称不能为空')

        if self.dimension not in {'did', 'uid', 'ip', 'page', 'others', 'global', ''}:
            raise RuntimeError('维度{}不正确'.format(self.dimension))

        if self.status not in {'enable', 'disable'}:
            raise RuntimeError('变量状态{}不正确'.format(self.status))

        if self.type not in {'event', 'aggregate', 'top', 'filter', 'dual', 'sequence', 'collector', 'delaycollector'}:
            raise RuntimeError('暂不支持变量类型{}'.format(self.type))

        if self.type not in {'event', 'filter', 'collector', 'delaycollector'} and not \
                is_value_type_supported(self.value_type, self.value_subtype):
            raise RuntimeError('变量数据类型({}, {})暂不支持'.format(self.value_type, self.value_subtype))

        if self.value_category not in {'did', 'uid', 'ip', ''}:
            raise RuntimeError('变量类别{}不正确'.format(self.value_category))

        self.function.sanity_check()
        self.filter.sanity_check()
        if self.period and self.period.get('type'):
            if self.period['type'] not in get_supported_period_types():
                raise RuntimeError('窗口类型{}不正确'.format(self.period['type']))

    @property
    def module(self):
        return self._module

    @module.setter
    def module(self, module):
        self._module = text(module or '')

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = text(app or '')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = text(name or '')

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, remark):
        self._remark = text(remark or '')

    @property
    def visible_name(self):
        return self._visible_name

    @visible_name.setter
    def visible_name(self, visible_name):
        self._visible_name = text(visible_name or '')

    @property
    def dimension(self):
        return self._dimension

    @dimension.setter
    def dimension(self, dimension):
        self._dimension = text(dimension or '')

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = text(status or '')

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = text(type or '')

    @property
    def value_type(self):
        return self._value_type

    @value_type.setter
    def value_type(self, value_type):
        self._value_type = text(value_type or '')

    @property
    def value_subtype(self):
        return self._value_subtype

    @value_subtype.setter
    def value_subtype(self, value_subtype):
        self._value_subtype = text(value_subtype or '')

    @property
    def value_category(self):
        return self._value_category

    @value_category.setter
    def value_category(self, value_category):
        self._value_category = text(value_category or '')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = text(source or list())

    @property
    def filter(self):
        return self._filter

    @filter.setter
    def filter(self, filter):
        if isinstance(filter, Filter):
            self._filter = filter
        else:
            self._filter = Filter.from_dict(filter or {})

    @property
    def period(self):
        return self._period

    @period.setter
    def period(self, period):
        self._period = unicode_dict(period or dict())

    @property
    def function(self):
        return self._function

    @function.setter
    def function(self, function):
        if isinstance(function, Function):
            self._function = function
        else:
            self._function = Function.from_dict(function or {})

    @property
    def groupbykeys(self):
        return self._groupbykeys

    @groupbykeys.setter
    def groupbykeys(self, groupbykeys):
        self._groupbykeys = unicode_list(groupbykeys or list())

    @property
    def hint(self):
        return self._hint

    @hint.setter
    def hint(self, hint):
        self._hint = unicode_dict(hint or dict())

    def get_dict(self):
        result = dict()
        result['module'] = self.module
        result['app'] = self.app
        result['name'] = self.name
        result['remark'] = self.remark
        result['visible_name'] = self.visible_name
        result['dimension'] = self.dimension
        result['status'] = self.status
        result['type'] = self.type
        result['value_type'] = self.value_type
        result['value_subtype'] = self.value_subtype
        result['value_category'] = self.value_category
        result['source'] = self.source
        result['filter'] = self.filter.get_dict()
        result['period'] = self.period
        result['function'] = self.function.get_dict()
        result['groupbykeys'] = self.groupbykeys
        result['hint'] = self.hint
        return result

    def get_ordered_dict(self):
        return OrderedDict([
            ('module', self.module),
            ('app', self.app),
            ('name', self.name),
            ('remark', self.remark),
            ('visible_name', self.visible_name),
            ('dimension', self.dimension),
            ('status', self.status),
            ('type', self.type),
            ('value_type', self.value_type),
            ('value_subtype', self.value_subtype),
            ('value_category', self.value_category),
            ('source', self.source),
            ('filter', self.filter.get_ordered_dict()),
            ('period', self.period),
            ('function', self.function.get_ordered_dict()),
            ('groupbykeys', self.groupbykeys),
            ('hint', self.hint),
        ])

    def get_simplified_ordered_dict(self):
        if self.source:
            can_ignore_source = self.source[0]['name']
        else:
            can_ignore_source = ''

        tuples = list()
        tuples.append(('module', self.module))
        tuples.append(('app', self.app))
        tuples.append(('name', self.name))
        tuples.append(('remark', self.remark))
        tuples.append(('visible_name', self.visible_name))
        tuples.append(('status', self.status))
        tuples.append(('type', self.type))
        if self.source:
            tuples.append(('source', self.source))

        filter_dict = self.filter.get_simplified_ordered_dict(can_ignore_source)
        if filter_dict:
            tuples.append(('filter', filter_dict))

        tuples.append(('period', self.period))

        function_dict = self.function.get_simplified_ordered_dict(can_ignore_source)
        if function_dict:
            tuples.append(('function', function_dict))

        if self.groupbykeys:
            tuples.append(('groupbykeys', self.groupbykeys))
        if self.hint:
            tuples.append(('hint', self.hint))
        return OrderedDict(tuples)

    def get_json(self):
        return json.dumps(self.get_ordered_dict())

    @staticmethod
    def from_dict(d):
        try:
            return VariableModel(d.get('module'), d.get('app'), d.get('name'), d.get('remark', ''),
                                 d.get('visible_name', ''), d.get('dimension', ''), d.get('status'), d.get('type'),
                                 d.get('value_type', ''), d.get('value_subtype', ''), d.get('value_category', ''),
                                 d.get('source', list()), d.get('filter', dict()), d.get('period', dict()),
                                 d.get('function', dict()), d.get('groupbykeys', list()), d.get('hint', dict()))
        except RuntimeError as err:
            if d and 'name' in d:
                raise RuntimeError('创建变量{}失败: {}'.format(d.get('name'), err.message))
            else:
                raise err

    @staticmethod
    def from_json(json_str):
        return VariableModel.from_dict(json.loads(json_str))

    def copy(self):
        return VariableModel.from_dict(self.get_dict())

    def __str__(self):
        return "VariableModel[{}]".format(self.get_dict())

    def __repr__(self):
        return "VariableModel[{}]".format(self.get_dict())

    def __eq__(self, other):
        return self.get_dict() == other.get_dict()

    def __ne__(self, other):
        return not self == other

    def _normalize(self):
        """
        验证变量数据是否准确, 并进行自动推导
        """

        # source可能被省略
        self._check_source()
        self._check_dimension()
        # 数据类型推导
        self._check_value_type()
        self._check_value_category()
        self._check_special_logic()

        # filter 需要对一些参数做些处理
        if self.filter:
            self.filter.check_value()

    def _check_dimension(self):
        """
        自动推导dimension
        """

        if self.type == 'event':
            self.dimension = ''
            return

        if self._is_inner():
            return

        if self.source:
            source_variable_dimension_set = set()
            for source_variable_name in self.source:
                source_app = source_variable_name['app']
                source_name = source_variable_name['name']
                source_variable = Global_Variable_Registry.get((source_app, source_name))
                if not source_variable:
                    raise RuntimeError('变量{}定义不存在'.format(source_name))
                source_variable_dimension_set.add(source_variable.dimension)

            source_variable_dimension_set = filter(bool, source_variable_dimension_set)
            if len(source_variable_dimension_set) > 1:
                raise RuntimeError('来源变量的维度必须相同')

            if source_variable_dimension_set:
                self.dimension = source_variable_dimension_set.pop()
            else:
                self.dimension = ''

            if self.dimension:
                return

        if self.groupbykeys:
            first_key = self.groupbykeys[0]
            self.dimension = {
                'c_ip': 'ip',
                'uid': 'uid',
                'did': 'did',
                'page': 'page',
                'uri_stem': 'page',
            }.get(first_key, 'others')
        else:
            if self.type in {'event', 'filter'}:
                self.dimension = ''
            else:
                self.dimension = 'global'

    def _infer_value_type(self):
        """
        自动推导value_type和value_subtype
        """

        if self._is_inner():
            # 必须由配置给出
            return

        if self.type in {'event', 'collector', 'delaycollector', 'filter'}:
            self.value_type = self.value_subtype = ''
            return

        # self.type in {'aggregate', 'top', 'sequence', 'dual'}
        function = self.function
        method = function.method
        method_object = function.object
        object_type = function.object_type, function.object_subtype
        period_type = self.period.get('type', '')

        if not period_type:
            raise RuntimeError('变量配置不正确，无法推导变量类型')

        # count可以不需要object，其他都需要
        if 'count' not in method and (not method_object or not object_type):
            raise RuntimeError('变量配置不正确，无法推导变量类型')

        result = get_calculator_result_type(period_type, object_type, method)
        self.value_type, self.value_subtype = result

    def _check_value_category(self):
        """
        自动推导value_category
        """

        if self._is_inner():
            self.value_category = ''
            return

        if self.type in {'event', 'collector', 'delaycollector'}:
            self.value_category = ''
            return

        # 依赖于type
        # self.infer_value_type()

        category = ''

        function = self.function
        method = function.method
        object = function.object

        # 对于source是父变量, 可能取决于父变量的category
        if object == 'value':
            # 取对应的source， 必须有
            source = filter(lambda _: _['name'] == function.source, self.source)[0]
            source_variable = Global_Variable_Registry[(source['app'], source['name'])]
            source_category = source_variable.value_category
        else:
            source_category = ''

        if self.type == 'filter':
            self.value_category = source_category
            return

        if (self.value_type, self.value_subtype) in {
            ('string', ''),
            ('list', 'string'),
            ('map', 'string'),
            ('mlist', 'string'),
            ('mmap', 'string'),
        }:
            # 一种是直接distinct的list，一种是加上时间戳的mlist
            if method in {'distinct', 'lastn', 'collection', 'first', 'last', 'last_value', 'merge_value', 'merge',
                          'global_latest', 'global_map_merge_topn'}:
                if object == 'value':
                    category = source_category
                else:
                    category = OBJECT_CATEGORY_MAPPING.get(object, '')
        else:
            pass

        self.value_category = category

    def _get_source_mappings(self):
        """
        从全局数据中获取source变量的mapping
        :return:
        """

        if self._source_mappings:
            return self._source_mappings

        self._source_mappings = dict()
        for source_variable in self.source:
            key = (source_variable['app'], source_variable['name'])
            if key not in Global_Variable_Fields_Mapping_Registry:
                raise RuntimeError('变量{}的定义不存在'.format(key[1]))

            self._source_mappings[key[1]] = Global_Variable_Fields_Mapping_Registry[key]
        return self._source_mappings

    def get_properties(self):
        """
        返回可用的properties
        :return:
        """

        result = list()
        if self.type == 'event':
            if not self.source:
                raise RuntimeError('缺乏来源事件的指定')
            app, name = self.source[0]['app'], self.source[0]['name']
            all_events_in_dict = get_all_events_in_dict()
            if (app, name) not in all_events_in_dict:
                raise RuntimeError('缺乏事件{}的定义'.format(name))
            result = all_events_in_dict[(app, name)].properties
            return result
        elif self.type == 'filter':
            if not self.source:
                raise RuntimeError('缺乏来源变量的指定')
            app, name = self.source[0]['app'], self.source[0]['name']
            if (app, name) not in Global_Variable_Registry:
                raise RuntimeError('缺乏变量{}的定义'.format(name))

            result = Global_Variable_Properties_Registry[(app, name)]
            return result
        elif self.type in {'aggregate', 'top', 'dual', 'sequence'}:
            result.append(Property('value', self.value_type, self.value_subtype, self.visible_name, self.remark))

            # add group key
            if self.groupbykeys:
                app, name = self.source[0]['app'], self.source[0]['name']
                one_source_properties = Global_Variable_Properties_Registry.get((app, name))
                if not one_source_properties:
                    raise RuntimeError('缺乏变量{}的定义'.format(name))
                one_source_properties_dict = {_.name: _ for _ in one_source_properties}
                for key in self.groupbykeys:
                    if key not in one_source_properties_dict:
                        raise RuntimeError('变量{}使用的字段{}不存在来源变量中'.format(self.name, key))
                    result.append(one_source_properties_dict[key])

            return result
        elif self.type in {'collector', 'delaycollector'}:
            if not self.source:
                raise RuntimeError('缺乏来源变量的指定')

            for source in self.source:
                name = source['name']
                source_variable = Global_Variable_Registry.get((source['app'], source['name']))
                if not source_variable:
                    raise RuntimeError('缺乏变量{}的定义'.format(name))

                result.append(Property(name, source_variable.value_type, source_variable.value_subtype,
                                       source_variable.visible_name, source_variable.remark))

            # add group key
            if self.groupbykeys:
                app, name = self.source[0]['app'], self.source[0]['name']
                one_source_properties = Global_Variable_Properties_Registry.get((app, name))
                if not one_source_properties:
                    raise RuntimeError('缺乏变量{}的定义'.format(name))
                one_source_properties_dict = {_.name: _ for _ in one_source_properties}
                for key in self.groupbykeys:
                    if key not in one_source_properties_dict:
                        raise RuntimeError('变量{}使用的字段{}不存在来源变量中'.format(self.name, key))
                    result.append(one_source_properties_dict[key])
        elif self._is_inner():
            result.append(Property('value', self.value_type, self.value_subtype, self.visible_name, self.remark))
            if self.groupbykeys:
                for key in self.groupbykeys:
                    result.append(Property(key, 'string', '', '', ''))

        return result

    def get_fields_mapping(self):
        result = dict()
        if self.type == 'event':
            if not self.source:
                raise RuntimeError('缺乏来源事件的指定')
            app, name = self.source[0]['app'], self.source[0]['name']
            if (app, name) not in get_all_events_in_dict():
                raise RuntimeError('缺乏事件{}的定义'.format(name))
            result.update(get_event_fields_mapping_from_registry(app, name))
            return result
        elif self.type == 'filter':
            if not self.source:
                raise RuntimeError('缺乏来源变量的指定')
            app, name = self.source[0]['app'], self.source[0]['name']
            source_mapping = self._get_source_mappings().get(name)
            if not source_mapping:
                raise RuntimeError('缺乏变量{}的定义'.format(name))

            result.update(source_mapping)
            return result
        elif self.type in {'aggregate', 'top', 'dual', 'sequence'}:
            result['value'] = (self.value_type, self.value_subtype)

            # add group key
            if self.groupbykeys:
                one_source_mapping = self._get_source_mappings().values()[0]
                for key in self.groupbykeys:
                    if key not in one_source_mapping:
                        raise RuntimeError('变量{}使用的字段{}不存在来源变量中'.format(self.name, key))
                    result[key] = one_source_mapping[key]

            return result
        elif self.type in {'collector', 'delaycollector'}:
            if not self.source:
                raise RuntimeError('缺乏来源变量的指定')

            for source in self.source:
                name = source['name']
                source_field_type = self._get_source_mappings()[name].get('value')
                if source_field_type:
                    result[name] = source_field_type
                result[name] = source_field_type

            one_source_mapping = self._get_source_mappings().values()[0]

            for key in self.groupbykeys:
                if key not in one_source_mapping:
                    raise RuntimeError('变量{}使用的字段{}不存在来源变量中'.format(self.name, key))
                result[key] = one_source_mapping[key]
        elif self._is_inner():
            result['value'] = (self.value_type, self.value_subtype)
            for key in self.groupbykeys:
                result[key] = ('string', '')

        return result

    def _is_inner(self):
        """
        变量是否为内置变量，有特殊用途
        :return:
        """

        return self.type == 'internal' or self.name.startswith('__inner')

    def _check_special_logic(self):
        """
        不同类型的变量可能需要不同的特殊推导
        :return:

        """
        if self.type == 'filter':
            first_source = self.source[0]
            app, name = first_source['app'], first_source['name']
            source_variable = Global_Variable_Registry.get((app, name))
            if not source_variable:
                raise RuntimeError('缺乏变量{}的定义'.format(name))

            self.value_type = source_variable.value_type
            self.value_subtype = source_variable.value_subtype
            self.value_category = source_variable.value_category
            # self.groupbykeys = source_variable.groupbykeys
            # self.dimension = source_variable.dimension

    def _check_source(self):
        """
        对source进行检查，并利用source信息对filter和function中的内容进行自动填充
        :return:
        """

        if self._is_inner():
            return

        if not self.source:
            raise RuntimeError('缺乏来源')

        if self.type == 'event':
            source_app = self.source[0].get('app')
            source_name = self.source[0].get('name')
            if (source_app, source_name) not in get_all_events_in_dict():
                raise RuntimeError('来源事件({}, {})找不到'.format(source_app, source_name))
            return

        for one_source in self.source:
            source_app = one_source['app']
            source_name = one_source['name']
            if (source_app, source_name) not in Global_Variable_Registry:
                raise RuntimeError('来源变量({}, {})找不到'.format(source_app, source_name))

        # source 为空时，默认取第一个source的name
        default_source = self.source[0]['name']
        if self.filter and not self.filter.is_empty():
            self.filter.fix_source(default_source)
        if self.function and not self.function.is_empty():
            self.function.fix_source(default_source)

    def _check_value_type(self):
        """
        将变量中涉及到数据类型推导的地方覆盖
        :return:
        """

        if self.type == 'event':
            return

        if self._is_inner():
            if not self._value_type:
                raise RuntimeError("内置变量的数据类型无法推导获得，必须给出")

        source_mappings = self._get_source_mappings()
        if self.filter and not self.filter.is_empty():
            self.filter.check_type(source_mappings)
        if self.function and not self.function.is_empty():
            self.function.check_type(source_mappings)
        self._infer_value_type()


Global_Variable_Registry = dict()
Global_Variable_Fields_Mapping_Registry = dict()
Global_Variable_Properties_Registry = dict()


def add_variable_to_registry(variable):
    Global_Variable_Registry[(variable.app, variable.name)] = variable
    Global_Variable_Fields_Mapping_Registry[(variable.app, variable.name)] = variable.get_fields_mapping()
    Global_Variable_Properties_Registry[(variable.app, variable.name)] = variable.get_properties()


def add_variables_to_registry(variables):
    for variable in variables:
        add_variable_to_registry(variable)


def update_variables_to_registry(variables):
    new_variable_registry = dict()
    new_variable_fields_mapping_registry = dict()
    new_variable_properties_registry = dict()
    for variable in variables:
        new_variable_registry[(variable.app, variable.name)] = variable
        new_variable_fields_mapping_registry[(variable.app, variable.name)] = variable.get_fields_mapping()
        new_variable_properties_registry[(variable.app, variable.name)] = variable.get_properties()

    global Global_Variable_Registry
    global Global_Variable_Fields_Mapping_Registry
    global Global_Variable_Properties_Registry

    Global_Variable_Registry = new_variable_registry
    Global_Variable_Fields_Mapping_Registry = new_variable_fields_mapping_registry
    Global_Variable_Properties_Registry = new_variable_properties_registry


def get_variable_from_registry(variable_app, variable_name):
    return Global_Variable_Registry.get((variable_app, variable_name))


def get_variable_fields_mapping_from_registry(variable_app, variable_name):
    return Global_Variable_Fields_Mapping_Registry.get((variable_app, variable_name))


def get_variable_properties_from_registry(variable_app, variable_name):
    return Global_Variable_Properties_Registry.get((variable_app, variable_name))


def get_all_variables():
    return Global_Variable_Registry.values()


def get_all_variables_in_dict():
    return dict(Global_Variable_Registry)


def sort_variable_models(variable_models):
    """
    对所有的variable进行排序，按照下面的顺序
    1. 先按dimension排序，因为event variable dimension为空，所以排在前面
    2. source variable在先
    3. 同顺序的variable, 先按照source的名称，然后按照自己的名称排序
    :param variable_models:
    :return: sorted variable_models
    """

    if not variable_models:
        return list()

    # 1. 初始化权重列表
    weight_dict = {_.name: 1 for _ in variable_models}

    # 2. 迭代，将衍生variable值抬高，直到无变化
    while True:
        modify_in_iteration = False
        for v in variable_models:
            if not v.source or v.type == 'event':
                continue

            source_weights = [weight_dict[_['name']] for _ in v.source]
            max_source_weight = max(source_weights)
            if weight_dict[v.name] > max_source_weight:
                pass
            else:
                weight_dict[v.name] = max_source_weight + 1
                modify_in_iteration = True

        if not modify_in_iteration:
            break

    def sort_variable_cmp(left, right):
        result = cmp(left.dimension, right.dimension) or cmp(weight_dict[left.name], weight_dict[right.name])
        if result:
            return result

        if left.source and right.source:
            result = cmp(left.source[0]['name'], right.source[0]['name'])

        if result:
            return result

        result = cmp(left.name, right.name)
        return result

    variable_models.sort(cmp=sort_variable_cmp)
    return variable_models
