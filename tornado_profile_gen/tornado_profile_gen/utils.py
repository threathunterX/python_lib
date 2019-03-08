# -*- coding: utf-8 -*-

from __future__ import absolute_import
import json
import urlparse
from traceback import print_exc

import yaml

from . import settings

def attach_api_url(setting):
    """
    只在注册api 的profile接口的时候，修改默认发送的json的host 和schemes字段
    
    """
#    print setting
    api_host_url = urlparse.urlparse("%s:%s" % (setting.get(settings.TORNADO_ADDRESS_NAME),
                                                setting.get(settings.TORNADO_PORT_NAME),
                                            ))
#    settings.DEFAULT_JSON_FORMATTER["host"] = api_host_url.geturl()
    
    if setting.has_key(settings.TORNADO_PROTOCOL_NAME):
        settings.DEFAULT_JSON_FORMATTER["schemes"].append( setting.get(settings.TORNADO_PROTOCOL_NAME) )

def get_rest_url(url_spec):
    """
    获取application里面一个路由对应的handler里面注册的restful api url, 如果没有的话会显示注册的正则
    
    in: tornado URL_Spec object
    Return: restful api url or [url regex pattern]
    """
    handler = url_spec.handler_class
    #print handler.__dict__
    if handler.__dict__.has_key('REST_URL'):
        return handler.__dict__.get('REST_URL')
    else:
        return url_spec.regex.pattern.replace('$', '')
    
def get_app_urls(app):
    """
    拿到tornado application实例的路由和handler的对应关系
    
    in: tornado application object
    Return: url-pattern : handler (dict)
    """
    url_specs = ( up[1] for up in app.handlers ) # may be rely on tornado version

    # [ [url_spec, url_spec], [url_spec]] -> [url_spec, url_spec, url_spec]
    url_patterns = ( _ for up_list in url_specs for _ in up_list)
#    print 'app handlers', app.handlers
#    print 'url_patterns:', url_patterns
    
    return dict( (get_rest_url(up), up.handler_class) for up in url_patterns)
    
def get_api_method_doc(routes):
    """
    查找所有api接口的文档
    
    注册api的方式: handler类变量 REST_METHODS 注册接口 ex. {'get', } set类型 > list类型 其他可迭代对象
    忽略接口的方式: handler类变量 REST_METHODS_EXCLUDE 注册忽略接口 ex. {'get', } set类型 > list类型 其他可迭代对象
    两种手动注册api的方式皆是可选, REST_METHODS优先级高
    
    in: url-pattern : handler(dict)
    Return: {url_pattern:{ method:method_doc, method:method_doc }} (dict)
    """
    route_docs = dict( (pattern, dict()) for pattern,_ in routes.iteritems() )
    for pattern, handler in routes.iteritems():
        # 获取需要解析的方法 (REST_METHODS or DEFAULT_METHODS) - REST_METHODS_EXCLUDE
        search_method = handler.REST_METHODS if handler.__dict__.has_key('REST_METHODS') else settings.DEFAULT_METHODS 
        if handler.__dict__.has_key('REST_METHODS_EXCLUDE'):
            exclude_apis = set(handler.REST_METHODS_EXCLUDE)
            search_method = [ _ for _ in search_method if _ not in exclude_apis]
        # 获取handler里面所需的接口方法的doc @todo 获取handler继承树中所需的接口方法的doc
        for method in search_method:
#            if handler.__dict__.has_key(method):
            if hasattr(handler, method):
                class_method = getattr(handler, method)
                route_docs[pattern][method] = class_method.__doc__
    return route_docs

def get_custmize_classs(cla):
    from tornado.web import RequestHandler
    return (_ for _ in cla.__mro__ if _ not in (object, RequestHandler))
    
def get_docs(search_method, classs):
    d = dict()
    # method: doc
    #@todo
    
def parse_doc_before(doc):
    """
    只是将doc中的@API标识之后的json字符串load出来而已,
    对外也可以用来检测你的__doc__是否能被正确解析
    
    in: 函数的__doc__ 属性, 其实任何字符串皆可(str)
    Return: doc属性字典(dict)
    """
    if doc is None:
        return None
    lines_gen = (_.strip() for _ in doc.split('\n'))
    
    api_begin_pattern = settings.API_BEGIN_PA
    is_api_begin = False
    
    api_lines = []
    
    for l in lines_gen:
        if l == api_begin_pattern:
            is_api_begin = True
            continue
        if is_api_begin:
            api_lines.append(l)

    if api_lines:
        print 'api_lines', api_lines
        return json.loads(''.join(api_lines))
    return None

def parse_doc(doc):
    """
    只是将doc中的@API标识之后的yaml配置load出来而已,
    对外也可以用来检测你的__doc__是否能被正确解析
    
    in: 函数的__doc__ 属性, 其实任何字符串皆可(str)
    Return: doc属性字典(dict)
    """
    if doc is None:
        return None
    api_begin_pattern = '@API'#settings.API_BEGIN_PA
    
    api_doc_index = doc.find(api_begin_pattern)
    
    try:
        if api_doc_index != -1:
            return yaml.load(doc[api_doc_index+len(api_begin_pattern):])
    except Exception:
        print '下面这段__doc__属性不是合法的yaml格式(正常应该没有@API标识)',doc[api_doc_index+len(api_begin_pattern):]
    return None

def default_formatter(route_docs):
    """
    解析所有路由中方法的doc成为字典
    
    in: {url_pattern:{ method:method_doc, method:method_doc }}
    Return:{url_pattern:{ method:method_dict, method:method_dict }}
    """
    paths_dict = dict( (up, dict()) for up in route_docs.iterkeys())
    for up, method_dict in route_docs.iteritems():
        for method, doc in method_dict.iteritems():
            try:
                parsed_doc = parse_doc(doc)
            except ValueError:
                print_exc()
                print up, method, 'invalid doc to parse api.'
                continue
            if parsed_doc is None:
                # bugfix: 'get':null swagger can not handle
                continue
            paths_dict[up][method] = parsed_doc

    return paths_dict
