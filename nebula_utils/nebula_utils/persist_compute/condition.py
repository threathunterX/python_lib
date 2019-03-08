# -*- coding: utf-8 -*-
import re
import logging
from functools import partial

logger = logging.getLogger('nebula_utils.persist_compute.condition')

COMPOUND_CONDITIONS = ('and', 'or')

def eval_statement(operator, left, right):
    # @todo 类型检查 
    # @todo 数据缺失时
    # 缺少左值时 数据variable没有对应字段值,
    # 缺少右指时 配置的variable有错,
    # 就是判断为空值的条件
    if left is None or right is None:
        # 应该默认是不为None, 不然下面很多为None处理(下一步也应该这么做)
        return False
    
    # @todo 增加这些判断们
    if operator == 'stringequals':
        return left == right
    elif operator == 'stringcontains':
        return left.find(right) != -1
    elif operator == 'contain':
        return left.find(right) != -1
    elif operator == '!contain':
        return left.find(right) == -1
    elif operator == '!startwith':
        return not left.startswith(right)
    elif operator == 'startwith':
        return left.startswith(right)
    elif operator == '!endwith':
        return not left.endswith(right)
    elif operator == 'endwith':
        return left.endswith(right)
    elif operator == 'longbiggerthan':
        return left > right
    elif operator == 'and':
        eval_sub_condition = partial(eval_condition, variable=right)
        return all(map(eval_sub_condition, left))
    elif operator == 'or':
        eval_sub_condition = partial(eval_condition, variable=right)
        return any(map(eval_sub_condition, left))
    elif operator == '/':
        if left == 0 or right ==0:
            return 0
        if left is None:
            return 0
        if right is None:
            return ValueError
        return float(left) / right
    elif operator == '*':
        return left * right
    elif operator == '+':
        return left + right
    elif operator == '-':
        return left - right
    elif operator == '>':
        return left > right
    elif operator == '>=':
        return left >= right
    elif operator == '<':
        return left < right
    elif operator == '<=':
        return left <= right
    elif operator == '==':
        return left == right
    elif operator == '!=':
        return left == right
    elif operator == 'between':
        if not isinstance(right, list) or len(right) < 2:
            return ValueError
        return right[0] <= left <= right[1]
    elif operator == 'doublesmallerthan':
        return int(left) < right
    elif operator == 'in':
        if not isinstance(right, list):
            return ValueError
        return left in right
    elif operator == '!in':
        if not isinstance(right, list):
            return ValueError
        return left not in right
    elif operator == 'regex':
        try:
            m = re.match(right, left)
            return True if m else False
        except:
            return ValueError
    elif operator == '!regex':
        try:
            m = re.match(right, left)
            return True if not m else False
        except:
            return ValueError
    else:
        return ValueError
        logger.error('未知的操作符: %s', operator)

def eval_condition(condition, variable):
    """
    condition : dict
    variable: dict
    """
    if condition is None:
        return True

    condition_type = condition['type']
    if condition_type in COMPOUND_CONDITIONS:
        return eval_statement(condition_type, condition['conditions'], variable)
    else:
        property_name = condition['srcProperty']['name']
        return eval_statement(condition_type, variable.get(property_name, None) ,condition['param'])


def test_condition():
    
    condition1 = {
            "srcProperty": {
                "identifier": [],
                "type": "string",
                "name": "result"
            },
            "type": "stringequals",
            "param": "F"
        }
    
    variable1 = {
        "result" : "F"
    }
    
    # 简单的条件判断
    assert eval_condition(condition1, variable1) is True
    
    condition2 =  {
        "conditions": [
          {
            "srcProperty": {
              "identifier": [
                "nebula",
                "httplog"
              ],
              "type": "string",
              "name": "method"
            },
            "type": "stringequals",
            "param": "POST"
          },
            {
            "srcProperty": {
                "identifier": [
                    "nebula",
                    "httplog"
                ],
                  "type": "string",
                "name": "s_type"
            },
                "type": "stringcontains",
                "param": "text/html"
            },
        ],
        "type": "or"
    }
    variable2 = {
        "method":"POST",
        "s_type":"text/json",
    }
    
    variable21 = {
        "method":"GET",
        "s_type":"text/json",
    }
    
    # 符合条件判断测试
    assert eval_condition(condition2, variable2) is True
    assert eval_condition(condition2, variable21) is False
    
    condition3 =  {
        "conditions": [
          {
            "srcProperty": {
              "identifier": [
                "nebula",
                "httplog"
              ],
              "type": "string",
              "name": "method"
            },
            "type": "stringequals",
            "param": "POST"
          },
          {
            "conditions": [
              {
                "srcProperty": {
                  "identifier": [
                    "nebula",
                    "httplog"
                  ],
                  "type": "string",
                  "name": "method"
                },
                "type": "stringequals",
                "param": "GET"
              },
              {
                "srcProperty": {
                  "identifier": [
                    "nebula",
                    "httplog"
                  ],
                  "type": "long",
                  "name": "s_bytes"
                },
                "type": "longbiggerthan",
                "param": 1000
              }
            ],
            "type": "and"
          }
        ],
        "type": "or"
      }
    
    variable3 = {
        "method":"POST",
        "s_type":"text/json",
    }
    variable31 = {
        "method":"GET",
        "s_type":"text/json",
    }
    variable32 = {
        "method":"GET",
        "s_type":"text/json",
        "s_bytes":1001
    }

    # 复杂的条件组合测试
    assert eval_condition(condition3, variable3) is True
    assert eval_condition(condition3, variable31) is False
    assert eval_condition(condition3, variable32) is True

if __name__ == '__main__':
    test_condition()