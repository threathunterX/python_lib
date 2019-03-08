# -*- coding: utf-8 -*-


import six, base64, time
from six import iteritems

'''
除了eventmeta， nebula_meta去掉了threathunter_common的依赖
'''


def unicode_string(string):
    """
    Make sure string is unicode, try to default with utf8, or base64 if failed.
    can been decode by `decode_unicode_string`
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string
    try:
        return string.decode("utf8")
    except UnicodeDecodeError:
        return '[BASE64-DATA]' + base64.b64encode(string) + '[/BASE64-DATA]'


def unicode_dict(_dict):
    """
    Make sure keys and values of dict is unicode.
    """
    r = {}
    for k, v in iteritems(_dict):
        r[unicode_string(k)] = unicode_obj(v)
    return r


def unicode_list(_list):
    """
    Make sure every element in list is unicode. bytes will encode in base64
    """
    return [unicode_obj(x) for x in _list]


def unicode_obj(obj):
    """
    Make sure keys and values of dict/list/tuple is unicode. bytes will encode in base64.
    Can been decode by `decode_unicode_obj`
    """
    if obj is None:
        return obj
    if isinstance(obj, dict):
        return unicode_dict(obj)
    elif isinstance(obj, (list, tuple)):
        return unicode_list(obj)
    elif isinstance(obj, six.string_types):
        return unicode_string(obj)
    elif isinstance(obj, (int, float)):
        return obj
    elif obj is None:
        return obj
    else:
        return obj


def text(string, encoding='utf8'):
    """
    Make sure string is unicode type, decode with given encoding if it's not.
    If parameter is a object, object.__str__ will been called
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string
    elif isinstance(string, six.binary_type):
        return string.decode(encoding)
    elif isinstance(string, bytearray):
        return six.binary_type(string).decode(encoding)
    else:
        return six.text_type(string)


def millis_now():
    return int(time.time() * 1000)


def utf8(string):
    """
    Make sure string is utf8 encoded bytes.
    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string.encode('utf8')
    elif isinstance(string, six.binary_type):
        return string
    elif isinstance(string, bytearray):
        return six.binary_type(string)
    else:
        return unicode(string).encode('utf8')
