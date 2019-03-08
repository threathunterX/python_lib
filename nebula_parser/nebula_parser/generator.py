#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .eventschema import get_event_field_type
from .generator_model import *
from .validate import *


def gen_generator(config):
    src_event_name = config["source"]
    dst_event_name = config["dest"]

    # check
    check_event_exist(src_event_name, True)
    check_event_exist(dst_event_name, False)

    pre_conditions = []
    mappings = DEFAULT_MAPPINGS[:]
    exp = config["terms"]
    for w in exp["when"]:
        # validate
        # when doesn't need sub col
        c = gen_condition(src_event_name, w["op"], w["src_col"], "",  w["op_string"])
        pre_conditions.append(c)

    for then in exp["then"]:
        # validate
        m_list = gen_mapping(src_event_name, dst_event_name, then["tar_col"], then["src_col"], then["op"],
                             then["op_string"])
        mappings.extend(m_list)

    existing_properties = {m.dst_field for m in mappings}
    for property_name, property_type in get_event_schema(dst_event_name).items():
        if property_name in existing_properties:
            continue

        # no mapping given, use default value
        m = Mapping(get_default_extractor(property_type), property_name, property_type, [])
        mappings.append(m)

    return Generator(pre_conditions, mappings, src_event_name, dst_event_name)


directly_field_mapping = lambda property_name, property_type: Mapping(DirectDataExtractor(property_name), property_name,
                                                                      property_type)
DEFAULT_MAPPINGS = [
    directly_field_mapping("timestamp", "long"),
    directly_field_mapping("c_ip", "string"),
    directly_field_mapping("sid", "string"),
    directly_field_mapping("uid", "string"),
    directly_field_mapping("did", "string"),
    directly_field_mapping("platform", "string"),
    directly_field_mapping("page", "string"),
    directly_field_mapping("c_port", "long"),
    directly_field_mapping("c_bytes", "long"),
    directly_field_mapping("c_body", "string"),
    directly_field_mapping("c_type", "string"),
    directly_field_mapping("s_ip", "string"),
    directly_field_mapping("s_port", "long"),
    directly_field_mapping("s_bytes", "long"),
    directly_field_mapping("s_body", "string"),
    directly_field_mapping("s_type", "string"),
    directly_field_mapping("host", "string"),
    directly_field_mapping("uri_stem", "string"),
    directly_field_mapping("uri_query", "string"),
    directly_field_mapping("referer", "string"),
    directly_field_mapping("method", "string"),
    directly_field_mapping("status", "long"),
    directly_field_mapping("cookie", "string"),
    directly_field_mapping("useragent", "string"),
    directly_field_mapping("xforward", "string"),
    directly_field_mapping("request_time", "long"),
    directly_field_mapping("request_type", "string"),
    Mapping(DefaultStringDataExtractor(), "referer_hit", "string")  # null value
]


def gen_condition(src_event_name, op, src_col, sub_col, op_string):
    """
    生成条件

    :param src_event_name: 来源日志名称
    :param op: 比较操作
    :param src_col: 来源字段名
    :param sub_col: 子字段名称，用于从某个字段里面取值
    :param op_string: 操作数
    :return: condition对象
    """
    check_field_exist(src_event_name, src_col, True)
    if sub_col:
        # 取原始日志字段的某个子字段
        # !!!!!! very very ugly hack, 因为我们不知道子字段类型，反过来靠op来猜吧
        if op in expressions_supported_on_string:
            # 优先string
            src_field_type = "string"
        elif op in expressions_supported_on_number:
            src_field_type = "double"
        else:
            raise RuntimeError(u"类型(%s)不支持" % op)

        left_extractor = FieldDataExtractor(src_col, sub_col, src_field_type)
    else:
        # 直接取原始日志的字段做比较
        check_operation_on_field(src_event_name, src_col, op)
        src_field_type = get_event_field_type(src_event_name, src_col)
        left_extractor = DirectDataExtractor(src_col)

    if op in {"regex", "!regex"}:
        oprand = check_and_return_pattern(op_string)
    elif op in {"between", "in", "!in"}:
        oprand = op_string.split(",")
    else:
        oprand = op_string

    oprand = check_and_return_value(oprand, src_field_type)
    right_extractor = ConstantDataExtractor(oprand)
    op = gen_operation(op)
    return Condition(left_extractor, right_extractor, op)


def gen_mapping(src_event_name, dst_event_name, tar_col, src_col, op, op_string):
    """
    生成mapping

    :param src_event_name: 源事件名
    :param dst_event_name: 目标事件名
    :param tar_col: 目标字段名
    :param src_col: 来源字段名
    :param op: 来源操作名
    :param op_string: 来源具体配置
    :return: 生成mapping列表
    """

    result = []
    check_field_exist(dst_event_name, tar_col, False)

    dst_field_type = get_event_field_type(dst_event_name, tar_col)
    if op == "switch":
        check_field_exist(dst_event_name, tar_col, False)

        for clause in op_string:
            clause_op = clause["op"]
            if clause_op == "default":
                oprand = check_and_return_value(clause["op_value"], dst_field_type)
                extractor = ConstantDataExtractor(oprand)
                m = Mapping(extractor, tar_col, get_event_schema(dst_event_name)[tar_col])

                # default is at the first, so that it can be overwrite
                result.insert(0, m)
            else:
                conditions_config = clause["op_string"]
                conditions = []
                for condition_config in conditions_config:
                    c = gen_condition(src_event_name, condition_config["op"], condition_config["src_col"],
                                      condition_config.get("sub_col", ""), condition_config["op_string"])
                    conditions.append(c)
                oprand = check_and_return_value(clause["op_value"], dst_field_type)
                extractor = ConstantDataExtractor(oprand)
                m = Mapping(extractor, tar_col, get_event_schema(dst_event_name)[tar_col], conditions)
                result.append(m)

    else:
        if op == "set":
            oprand = check_and_return_value(op_string, dst_field_type)
            extractor = ConstantDataExtractor(oprand)
        elif op == "extract_set":
            # extract set has two meaning

            # hack for "set", 当extract_context为空时，临时当做self用
            if not op_string["extract_context"]:
                check_field_exist(src_event_name, src_col, True)
                src_field_type = get_event_field_type(src_event_name, src_col)
                # 直接字段拷贝,要求类型匹配
                if src_field_type != dst_field_type:
                    raise RuntimeError(u"(%s.%s)由于类型原因不能转化为(%s.%s)" % (src_event_name, src_col, dst_event_name, tar_col))
                extractor = DirectDataExtractor(src_col)
            else:
                check_field_exist(src_event_name, src_col, True)
                extractor = FieldDataExtractor(src_col, op_string["extract_context"], dst_field_type)
        elif op == "self":
            check_field_exist(src_event_name, src_col, True)
            extractor = FieldDataExtractor(src_col, op_string["extract_context"], dst_field_type)
        else:
            raise RuntimeError(u"不支持的取值类型")

        result = [Mapping(extractor, tar_col, get_event_schema(dst_event_name)[tar_col])]
    return result


_supported_operations_map = {
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "between": lambda a, b: b[0] <= a <= b[1],
    "in": lambda a, b: a in b,
    "!in": lambda a, b: a not in b,
    "contain": lambda a, b: b in a,
    "!contain": lambda a, b: b not in a,
    "startwith": lambda a, b: a.startswith(b),
    "!startwith": lambda a, b: not a.startswith(b),
    "endwith": lambda a, b: a.endswith(b),
    "!endwith": lambda a, b: not a.endswith(b),
    "regex": lambda a, b: b.match(a),
    "!regex": lambda a, b: not b.match(a)
}
supported_operations = {name: Operation(name, func) for name, func in _supported_operations_map.items()}


def gen_operation(op_type):
    """
    取得比较类型
    """

    result = supported_operations.get(op_type)

    if not result:
        return RuntimeError(u"操作类型(%s)不支持" % op_type)

    return result
