#!/usr/bin/env python
# -*- coding: utf-8 -*-


from threathunter_common.event import Event

from .bson.objectid import ObjectId
from .httpdatacontext import HttpDataContext
from .eventschema import get_event_schema


class Generator(object):
    """
    日志生成器, 从一个事件生成另一个事件
    """

    def __init__(self, pre_conditions, mappings, src_event_name, dst_event_name):
        """
        :param pre_conditions: 必须满足的条件，如不满足，则不生成
        :param mappings: 字段生成
        :param src_event_name: 原始事件名
        :param dst_event_name: 生成事件名
        """

        self.pre_conditions = pre_conditions
        self.mappings = mappings
        self.src_event_name = src_event_name
        self.dst_event_name = dst_event_name
        dst_event_schema = get_event_schema(dst_event_name)

        # 为转化日志准备默认值
        self.dst_field_default_values = {
            field_name: "" if field_type in {"str", "string"} else 0
            for field_name, field_type in dst_event_schema.items()
        }

    def parse_event(self, src_event, http_msg):
        """
        :param src_event: 原始事件
        :param http_msg: 关联的http数据
        :return: 生成的事件；如果条件不满足，返回None
        """

        src_event_properties = src_event.property_values
        http_data_context = HttpDataContext()
        http_data_context.from_http_msg(http_msg)

        for c in self.pre_conditions:
            if not c.eval(src_event_properties, http_msg, http_data_context):
                return None

        dst_properties = dict()
        for m in self.mappings:
            name, value = m.map(src_event_properties, http_msg, http_data_context)
            if name is not None:
                dst_properties[name] = value

        for field_name, default_value in self.dst_field_default_values.items():
            if field_name not in dst_properties:
                dst_properties[field_name] = default_value

        return Event("nebula", self.dst_event_name, dst_properties["c_ip"], dst_properties["timestamp"], dst_properties)

    def __str__(self):
        return """Generator{source: %s, dest: %s, pre_conditions:[%s], mappings: [%s]}""" \
               % (self.src_event_name, self.dst_event_name,
                  ";".join(map(str, self.pre_conditions)), ";".join(map(str, self.mappings)))


class Operation(object):

    def __init__(self, name, func):
        self._name = name
        self._func = func

    @property
    def name(self):
        return self._name

    @property
    def func(self):
        return self._func

    def __str__(self):
        return self._name


class Condition(object):
    """
    日志解析中的条件语句
    """

    def __init__(self, left_extractor, right_extractor, op):
        """
        :param left_extractor: 左变量生成器
        :param right_extractor: 右变量生成器
        :param op: 比较操作
        """

        self.left_extractor = left_extractor
        self.right_extractor = right_extractor
        self.op = op

    def eval(self, src_event_properties, http_msg, http_data_context):
        """
        得到转换后的值

        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: boolean，条件是否满足
        """

        left = self.left_extractor.extract(src_event_properties, http_msg, http_data_context)
        right = self.right_extractor.extract(src_event_properties, http_msg, http_data_context)

        # for null value
        if left is None or right is None:
            return False
        return self.op.func(left, right)

    def __str__(self):
        return """Condition{left: %s, op: %s, right: %s}""" % (self.left_extractor, self.op, self.right_extractor)


class Mapping(object):
    """
    从原始事件生成新事件的一个字段
    """

    def __init__(self, extractor, dst_field, dst_field_type, conditions=[]):
        """
        :param extractor: 从原始事件出取出值
        :param dst_field: 生成的字段名
        :param dst_field_type: 生成的字段类型
        :param conditions: mapping需要满足的条件
        :return: 获取的某个数据
        """

        self.extractor = extractor
        self.dst_field = dst_field
        self.dst_field_type = dst_field_type
        self.conditions = conditions

    def map(self, src_event_properties, http_msg, http_data_context):
        """
        得到转换后的值

        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 转换后的属性名和值
        """

        for c in self.conditions:
            if not c.eval(src_event_properties, http_msg, http_data_context):
                return None, None

        value = self.extractor.extract(src_event_properties, http_msg, http_data_context)
        return self.dst_field, value

    def __str__(self):
        return """Mapping{extractor: %s, dst_field: %s, dst_field_type: %s, conditions: [%s]}""" % \
               (self.extractor, self.dst_field, self.dst_field_type, ";".join(map(str, self.conditions)))


class DataExtractor(object):
    """
    从原始事件中获取数据
    """

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        raise RuntimeError(u"not implement")

    def __str__(self):
        return "abstract extractor"


class ConstantDataExtractor(DataExtractor):
    """
    返回常量值
    """

    def __init__(self, constant_value):
        """
        :param constant_value: 待返回的常量值
        """

        self.constant_value = constant_value

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return self.constant_value

    def __str__(self):
        return """ConstantDataExtractor(%s)""" % self.constant_value


class DirectDataExtractor(DataExtractor):
    """
    从原始事件中的某个字段获取数据
    """

    def __init__(self, src_property_name, default=None):
        """
        :param src_property_name: 原始事件的字段名，将取该字段的值
        :param default: 默认值
        """

        self.src_property_name = src_property_name
        self.default = default

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return src_event_properties.get(self.src_property_name, self.default)

    def __str__(self):
        return """DirectDataExtractor{src_property_name: %s}""" % self.src_property_name


class FieldDataExtractor(DataExtractor):
    """
    从原始事件中的query/cookie/response里面的某个字段获取数据
    """

    def __init__(self, src_property_name, field_name, dst_field_type, default=None):
        """
        :param src_property_name: 原始事件中的某个字段，只能是c_body/s_body/cookie/uri_quer   y
        :param field_name: 属性中的某个字段，对应到表单的一个键，json的一个字段，cookie的一项
        :param default: 默认值
        """

        self.src_property_name = src_property_name
        self.field_name = field_name
        self.default = default
        self.dst_field_type = dst_field_type

        self.retrieve_method = {
            "c_body": HttpDataContext.get_data_from_request_body,
            "s_body": HttpDataContext.get_data_from_response_body,
            "cookie": HttpDataContext.get_data_from_cookie,
            "uri_query": HttpDataContext.get_data_from_query
        }.get(self.src_property_name)
        if not self.retrieve_method:
            raise RuntimeError(u"字段(%s)不支持变量抽取" % src_property_name)

        self.cast_method = {
            "int": int,
            "long": int,
            "double": float,
            "float": float,
            "string": str,
        }.get(dst_field_type)
        if not self.cast_method:
            raise RuntimeError(u"目标字段类型不支持")

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        result = self.retrieve_method(http_data_context, self.field_name, self.default)
        if result is not None:
            try:
                result = self.cast_method(result)
            except:
                # 转化日志时遇到不支持的类型
                pass

        return result

    def __str__(self):
        return """FieldDataExtractor{src_property_name: %s, field_name: %s, default: %s}""" % \
               (self.src_property_name, self.field_name, self.default)


class DefaultNumberDataExtractor(DataExtractor):
    """
    返回数字类型的默认值
    """

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return 0

    def __str__(self):
        return """DefaultNumberDataExtractor(0)"""


class DefaultStringDataExtractor(DataExtractor):
    """
    返回字符串类型的默认值
    """

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return ""

    def __str__(self):
        return """DefaultStringDataExtractor("")"""


class ObjectIDDataExtractor(DataExtractor):
    """
    返回一个ObjectID
    """

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return str(ObjectId())

    def __str__(self):
        return """ObjectIDDataExtractor"""


class ListDataExtractor(DataExtractor):
    """
    返回一个包含列表.
    """

    def __init__(self, raw_value, default=[]):
        self.data = (raw_value or "").split(",")
        self.data = filter(lambda x: x != "", self.data)

    def extract(self, src_event_properties, http_msg, http_data_context):
        """
        :param src_event_properties: 原始事件的属性列表
        :param http_msg: 当前关联的http访问日志
        :param http_data_context: 当前关联的http数据（包括cookie/query/请求响应内容）
        :return: 获取的某个数据
        """

        return self.data

    def __str__(self):
        return """ListDataExtractor(%s)""" % (",".join(self.data))


def get_default_extractor(property_type):
    if property_type in ("int", "long", "double"):
        return DefaultNumberDataExtractor()

    if property_type in ("str", "string"):
        return DefaultStringDataExtractor()

    raise RuntimeError(u"类型%s没有默认值" % property_type)
