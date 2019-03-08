#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from nebula_meta.model.strategy import Strategy

from ..fixtures import *
from ..setup_test import load_strategy_content, load_config
from nebula_strategy.generator.online_gen import *


def setup_module(module):
    print ('start testing')
    load_config()


def teardown_module(module):
    print ('finish testing')


def get_strategy_example(filename='data/strategy.json'):
    return Strategy.from_json(load_strategy_content(filename))


def get_content(filename):
    return load_strategy_content(filename)


def extract_variable_dimension(v):
    name = v.name
    if name.startswith('_'):
        name = name[1:]
    return name.split('__')[0]


def test_strategy_generator():
    try:
        result = gen_variables_from_strategy(get_strategy_example(), effective_check=False)[0]
        variable_dimension_set = {extract_variable_dimension(variable) for variable in result}

        # should generate 3 dimensions
        assert 'uid' in variable_dimension_set
        assert 'ip' in variable_dimension_set
        assert 'did' in variable_dimension_set

        pass
    except Exception as t:
        pass
        raise t


def test_distinct_count_empty_logic():
    # distinct的对象不能为空，第五个term就是
    try:
        result = gen_variables_from_strategy(get_strategy_example(), effective_check=False)[0]
        import json
        # for _ in result:
        #     print json.dumps(_.get_ordered_dict(), indent=4, separators=(',', ': '))
        result = [_ for _ in result if _.type == 'aggregate']
        result = [_ for _ in result if '__5__rt' in _.name][0]
        condition = result.filter
        assert condition.type == 'and'
        for c in condition.condition:
            if c.type == 'simple' and c.operation == '!=' and c.value == '':
                # 一定有一个不为空的命中
                break
        else:
            assert False, 'distinct 缺少不为空的条件'
    except Exception as t:
        import traceback; traceback.print_exc()
        pass
        raise t


def test_single_ip_dimension_simple_strategy():
    result = gen_variables_from_strategy(get_strategy_example('data/ip_dimension_simple_strategy.json'),
                                         effective_check=False)[0]
    # import json
    # result_dict = [_.get_ordered_dict() for _ in result]
    # print json.dumps(result_dict, indent=4, separators=(',', ': '))

    ip_trigger = result[0]
    assert ip_trigger.type == 'filter'
    assert ip_trigger.dimension == 'ip'
    assert ip_trigger.source == [{'app': 'nebula', 'name': 'ACCOUNT_LOGIN'}]
    assert ip_trigger.groupbykeys == ['c_ip']
    ip_trigger_condition = ip_trigger.filter.condition
    assert len(ip_trigger_condition) == 3
    assert ip_trigger_condition[0].source == 'ACCOUNT_LOGIN'
    assert ip_trigger_condition[0].object == 'c_ip'
    assert ip_trigger_condition[0].operation == 'contains'
    assert ip_trigger_condition[0].value == '.'
    assert ip_trigger_condition[0].type == 'simple'
    # second event term
    assert ip_trigger_condition[1].source == 'ACCOUNT_LOGIN'
    assert ip_trigger_condition[1].object == 'useragent'
    assert ip_trigger_condition[1].operation == '!match'
    assert ip_trigger_condition[2].source == 'ACCOUNT_LOGIN'
    assert ip_trigger_condition[0].object == 'c_ip'
    assert ip_trigger_condition[2].operation == '!locationcontainsby'
    # todo
    # assert ip_trigger_condition[2].value == '上海市'

    counter = result[1]
    assert counter.type == 'aggregate'
    assert counter.dimension == 'ip'
    assert counter.source == [{'app': 'nebula', 'name': 'ACCOUNT_LOGIN'}]
    assert counter.groupbykeys == ['c_ip']
    counter_condition = counter.filter.condition[0].condition
    assert len(counter_condition) == 2

    collector = result[2]
    assert collector.type == 'collector'
    assert collector.dimension == 'ip'
    assert collector.groupbykeys == ['c_ip']
    assert {'app': 'nebula', 'name': ip_trigger.name} in collector.source
    assert {'app': 'nebula', 'name': counter.name} in collector.source
    assert {'app': 'nebula', 'name': 'ip__account_login_count__5m__rt'} in collector.source

    assert collector.function.method == 'setblacklist'
    assert collector.function.param == u'策略'
    assert collector.function.config['trigger'] == ip_trigger.name


def test_all_strategy():
    all_strategy_content = get_content('data/all_strategy.json')
    all_strategy_content = json.loads(all_strategy_content)
    result = list()
    used_vars = list()
    for _ in all_strategy_content:
        s = Strategy.from_dict(_)
        gen, used = gen_variables_from_strategy(s, effective_check=False)
        result.extend(gen)
        used_vars.extend(used)
        # result_dict = [_.get_ordered_dict() for _ in result]
        # print json.dumps(result_dict, indent=4, separators=(',', ': '))
        # print
        # print
        # print

    result = [_.get_dict() for _ in result]
    print json.dumps(result)
    print len(used_vars), used_vars


def test_interval_strategy():
    """
    测试interval 逻辑
    :return:
    """

    result = gen_variables_from_strategy(get_strategy_example('data/interval_strategy.json'),
                                         effective_check=False)[0]

    # ip trigger contains .
    ip_trigger = result[0]
    assert ip_trigger.type == 'filter'
    assert ip_trigger.dimension == 'ip'
    assert ip_trigger.source == [{'app': 'nebula', 'name': 'ACCOUNT_LOGIN'}]
    assert ip_trigger.groupbykeys == ['c_ip']
    ip_trigger_condition = ip_trigger.filter.condition
    assert len(ip_trigger_condition) == 1
    assert ip_trigger_condition[0].source == 'ACCOUNT_LOGIN'
    assert ip_trigger_condition[0].object == 'c_ip'
    assert ip_trigger_condition[0].operation == 'contains'
    assert ip_trigger_condition[0].value == '.'
    assert ip_trigger_condition[0].type == 'simple'

    ip_collector = result[1]

    # did trigger, did is not null
    did_trigger = result[2]
    did_trigger_condition = did_trigger.filter
    assert did_trigger_condition.source == 'ACCOUNT_LOGIN'
    assert did_trigger_condition.object == 'did'
    assert did_trigger_condition.operation == '!='
    assert did_trigger_condition.value == ''
    assert did_trigger_condition.type == 'simple'

    # counter1, last timestamp
    counter1 = result[3]
    print counter1
    assert counter1.type == 'aggregate'
    assert counter1.dimension == 'did'
    assert counter1.source == [{'app': 'nebula', 'name': 'HTTP_DYNAMIC'}]
    assert counter1.groupbykeys == ['did']
    assert counter1.function.method == 'last'
    assert counter1.function.object == 'timestamp'

    # counter2, last timestamp of trigger
    counter2 = result[4]
    assert counter2.type == 'aggregate'
    assert counter2.dimension == 'did'
    assert counter2.source == [{'app': 'nebula', 'name': did_trigger.name}]
    assert counter2.groupbykeys == ['did']
    assert counter2.function.method == 'last'
    assert counter2.function.object == 'timestamp'

    # counter3, dual var of -
    counter3 = result[5]
    assert counter3.type == 'dual'
    assert counter3.dimension == 'did'
    assert counter3.source == [{'app': 'nebula', 'name': counter2.name}, {'app': 'nebula', 'name': counter1.name}]
    assert counter3.groupbykeys == ['did']
    assert counter3.function.method == '-'
    assert counter3.function.object == 'value'

    # did collector, 2 condition, 我们自动加了一个大于0
    did_collector = result[-1]
    assert 2 == len(did_collector.filter.condition)


def test_inner_variable():
    """
    2.18新增了内部变量，进行测试

    :return:
    """

    result = gen_variables_from_strategy(get_strategy_example('data/inner_strategy.json'),
                                         effective_check=False)[0]
    print 'success'
