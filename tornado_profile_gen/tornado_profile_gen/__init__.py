# -*- coding: utf-8 -*-

from __future__ import absolute_import
import copy, json

from tornado.web import RequestHandler

from . import settings, utils
from tornado_cors import CorsMixin
__all__ = ['application_api_wrapper', 'API_Doc_Gen_Handler']

def application_api_wrapper(app, auth_wrapper, need_auth=True):
    utils.attach_api_url(app.settings)
    
    # 增加视图函数
    app.settings['PROFILE_API_URL'] = settings.PROFILE_API_URL
    profile_api = settings.PROFILE_API_URL
    # @todo warden_web settings API_VERSION
#    settings.API_VERSION = app.settings['API_VERSION']
    handler = API_Doc_Gen_Handler
    if need_auth:
        API_Doc_Gen_Handler.get = auth_wrapper(API_Doc_Gen_Handler.get)
    
    app.add_handlers('.*', [(profile_api, handler),])

class API_Doc_Gen_Handler(CorsMixin, RequestHandler):
    __apis__ = set()
    # Value for the Access-Control-Allow-Origin header.
    # Default: None (no header).
    CORS_ORIGIN = '*'

    # Value for the Access-Control-Allow-Headers header.
    # Default: None (no header).
    CORS_HEADERS = 'Content-Type'

    # Value for the Access-Control-Allow-Methods header.
    # Default: Methods defined in handler class.
    # None means no header.
    CORS_METHODS = 'POST'

    # Value for the Access-Control-Allow-Credentials header.
    # Default: None (no header).
    # None means no header.
    CORS_CREDENTIALS = True

    # Value for the Access-Control-Max-Age header.
    # Default: 86400.
    # None means no header.
    CORS_MAX_AGE = 21600

    # Value for the Access-Control-Expose-Headers header.
    # Default: None
    CORS_EXPOSE_HEADERS = 'Location, X-WP-TotalPages'    

    def get(self):
        # 拿到application所有的路由、handler对应关系
        routes = utils.get_app_urls(self.application)
#        print routes
    
        # 获取所有接口的文档
        # {url_pattern:{ method:method_doc, method:method_doc }}
        route_docs = utils.get_api_method_doc(routes)
#        print 'route:docs', route_docs
    
        return_json = copy.deepcopy(settings.DEFAULT_JSON_FORMATTER)
        # 解析接口的文档，格式化返回
        paths_dict = utils.default_formatter(route_docs)
        return_json[settings.DEFAULT_API_COL] = paths_dict
        return_json['info']['version'] = settings.API_VERSION or ''
#        print return_json
        
        # 格式化返回http报文
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(return_json))
