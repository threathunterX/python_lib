# -*- coding: utf-8 -*-

Version = '1.0.2'

PROFILE_API_URL = '/api/profile'
API_VERSION = None

API_BEGIN_PA = '@API'

TORNADO_ADDRESS_NAME = 'host_address'
TORNADO_PORT_NAME = 'host_port'
TORNADO_PROTOCOL_NAME = 'host_scheme'

DEFAULT_API_COL = 'paths' # 出去的json放api们的字段

DEFAULT_METHODS = ['get', 'post', 'put', 'delete', 'head', 'options']

DEFAULT_JSON_FORMATTER = {
    "swagger": "2.0",
    "info": {
        "version": "",
        "title": ""
    },
    "host": "", # @todo
    "basePath": "",
    "tags": [
        {
            "name": "redqapi"
        },
    ],
    "schemes": [
        "http"
    ],
    "paths": None,
    "definitions": {
        "pageStatistics": {
            "type": "object",
            "description": "页面访问统计",
            "properties": {
                "total_page": {
                    "type": "integer",
                    "format": "int32"
                },
                "data": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/pageStatisticsDetail"
                    }
                }
            }
        },
        "pageStatisticsDetail": {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string"
                },
                "url": {
                    "type": "string",
                    "description": "url（不包括host）"
                },
                "alarm_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "报警数"
                },
                "click_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "点击数"
                },
                "ip_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "ip总数"
                },
                "top_3_ip_click": {
                    "type": "integer",
                    "format": "int32"
                },
                "top_3_ip_click_percent": {
                    "type": "integer",
                    "format": "int32"
                },
                "user_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "ip总数"
                },
                "top_3_user_click": {
                    "type": "integer",
                    "format": "int32"
                },
                "top_3_user_click_percent": {
                    "type": "integer",
                    "format": "int32"
                },
                "did_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "ip总数"
                },
                "top_3_did_click": {
                    "type": "integer",
                    "format": "int32"
                },
                "top_3_did_click_percent": {
                    "type": "integer",
                    "format": "int32"
                }
            }
        },
        "relatedStatistics": {
            "type": "object",
            "description": "关联数据，只返回对应的object，若key_type为ip，只返回ip的数据",
            "properties": {
                "ip": {
                    "type": "array",
                    "description": "关联IP数据排行",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "ip"
                            },
                            "country": {
                                "type": "string",
                                "description": "国家"
                            },
                            "province": {
                                "type": "string",
                                "description": "省"
                            },
                            "city": {
                                "type": "string",
                                "description": "市"
                            },
                            "related_key_type": {
                                "type": "string",
                                "enum": [
                                    "ip",
                                    "user",
                                    "did",
                                    "click",
                                    "alarm"
                                ],
                                "description": "关联值类型"
                            },
                            "count": {
                                "type": "integer",
                                "format": "int32",
                                "description": "关联值数量"
                            }
                        }
                    }
                },
                "user": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "user"
                            },
                            "related_key_type": {
                                "type": "string",
                                "enum": [
                                    "ip",
                                    "user",
                                    "did",
                                    "click",
                                    "alarm"
                                ],
                                "description": "关联值类型"
                            },
                            "count": {
                                "type": "integer",
                                "description": "关联值数量"
                            }
                        }
                    }
                },
                "did": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "did"
                            },
                            "os": {
                                "type": "string",
                                "description": "操作系统"
                            },
                            "device_type": {
                                "type": "string",
                                "descritpion": "设备型号"
                            },
                            "related_key_type": {
                                "type": "string",
                                "enum": [
                                    "ip",
                                    "user",
                                    "did",
                                    "click",
                                    "alarm"
                                ],
                                "description": "关联值类型"
                            },
                            "count": {
                                "type": "integer",
                                "description": "关联值数量"
                            }
                        }
                    }
                },
                "page": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "对应类型的值"
                            },
                            "related_key_type": {
                                "type": "string",
                                "enum": [
                                    "ip",
                                    "user",
                                    "did",
                                    "parameter"
                                ],
                                "description": "关联值类型"
                            },
                            "count": {
                                "type": "integer",
                                "description": "关联值数量"
                            }
                        }
                    }
                }
            }
        },
        "validCount": {
            "type": "object",
            "properties": {
                "count": {
                    "type": "integer",
                    "format": "int32"
                }
            }
        },
        "alarmList": {
            "type": "object",
            "properties": {
                "total_page": {
                    "type": "integer",
                    "format": "int32"
                },
                "data": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/alarmDetail"
                    }
                }
            }
        },
        "alarmStatistics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "time_frame": {
                        "type": "integer",
                        "format": "int64"
                    },
                    "test_count": {
                        "type": "integer",
                        "format": "int32"
                    },
                    "production_count": {
                        "type": "integer",
                        "format": "int32"
                    }
                }
            }
        },
        "networkStatistics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "time_frame": {
                        "type": "integer",
                        "format": "int64"
                    },
                    "count": {
                        "type": "integer",
                        "format": "int32"
                    }
                }
            }
        },
        "alarmStatisticsDetail": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "array",
                    "description": "地点和报警数",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "地址"
                            },
                            "count": {
                                "type": "integer",
                                "format": "int32"
                            }
                        }
                    }
                },
                "strategy": {
                    "type": "array",
                    "description": "触发规则排行",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "规则名称"
                            },
                            "test": {
                                "type": "boolean",
                                "description": "是否为测试策略"
                            },
                            "count": {
                                "type": "integer",
                                "description": "数量"
                            }
                        }
                    }
                },
                "url": {
                    "type": "array",
                    "description": "攻击URL统计",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string",
                                "description": "URL地址"
                            },
                            "count": {
                                "type": "integer",
                                "description": "数量"
                            }
                        }
                    }
                }
            }
        },
        "behaviorVariables": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "变量名称",
                        "enum": [
                            "related_users",
                            "browswer_types",
                            "click_interval",
                            "signup",
                            "signin"
                        ]
                    },
                    "text": {
                        "type": "string",
                        "description": "用于显示的中文名称"
                    },
                    "value": {
                        "type": "integer",
                        "description": "变量的值"
                    }
                }
            }
        },
        "clickStatistics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "time_frame": {
                        "type": "integer",
                        "format": "int64",
                        "description": "时间段，用开始时间表示"
                    },
                    "count": {
                        "type": "integer",
                        "format": "int32",
                        "description": "点击数量"
                    },
                    "key_count": {
                        "type": "integer",
                        "format": "int32",
                        "description": "不同key数量（如不同的IP数、User数）"
                    },
                    "strategy_count": {
                        "type": "integer",
                        "format": "int32",
                        "description": "触发不同规则数"
                    }
                }
            }
        },
        "statisticsItems": {
            "type": "object",
            "properties": {
                "host_top": {
                    "type": "array",
                    "description": "ip,user,did",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item": {
                                "type": "string",
                                "description": ""
                            },
                            "count": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                },
                "url_top": {
                    "type": "array",
                    "description": "ip,user,did",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item": {
                                "type": "string",
                                "description": ""
                            },
                            "count": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                },
                "ip_top": {
                    "type": "array",
                    "description": "user,did",
                    "items": {
                        "type": "object",
                        "properties": {
                            "ip": {
                                "type": "string",
                                "description": ""
                            },
                            "country": {
                                "type": "string",
                                "description": ""
                            },
                            "province": {
                                "type": "string",
                                "description": ""
                            },
                            "city": {
                                "type": "string",
                                "description": ""
                            },
                            "count": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                },
                "user_top": {
                    "type": "array",
                    "description": "ip,did",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item": {
                                "type": "string",
                                "description": ""
                            },
                            "count": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                },
                "device_top": {
                    "type": "array",
                    "description": "ip,user",
                    "items": {
                        "type": "object",
                        "properties": {
                            "fp": {
                                "type": "string",
                                "description": ""
                            },
                            "os": {
                                "type": "string",
                                "description": ""
                            },
                            "device_type": {
                                "type": "string",
                                "description": ""
                            },
                            "count": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                },
                "variables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "related_users,related_ip,related_device,browser_types,click_interval,signup,signin,rules-trigger,risks-trigger"
                            },
                            "text": {
                                "type": "string",
                                "description": ""
                            },
                            "value": {
                                "type": "integer",
                                "format": "int64",
                                "description": ""
                            }
                        }
                    }
                }
            }
        },
        "clickItems": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp": {
                        "type": "integer",
                        "format": "int64",
                        "description": "点击时间"
                    },
                    "click_interval": {
                        "type": "integer",
                        "format": "int64",
                        "description": "点击间隔"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Session ID"
                    },
                    "url": {
                        "type": "string",
                        "description": "访问的页面URL"
                    },
                    "user": {
                        "type": "string",
                        "description": "用户手机号或其他标识"
                    },
                    "notice": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    },
                    "risk_score": {
                        "type": "integer",
                        "format": "int32",
                        "description": "风险分值"
                    },
                    "ip": {
                        "type": "string",
                        "description": "when type=did"
                    },
                    "did": {
                        "type": "string",
                        "description": "when type=ip,user"
                    }
                }
            }
        },
        "clickDetail": {
            "type": "object",
            "properties": {
                "timestamp": {
                    "type": "integer",
                    "format": "int64",
                    "description": "点击时间"
                },
                "client_host": {
                    "type": "string",
                    "description": "客户端"
                },
                "server_host": {
                    "type": "string",
                    "description": "服务端"
                },
                "http": {
                    "type": "object",
                    "description": "http报文",
                    "properties": {
                        "http_method": {
                            "type": "string",
                            "description": "请求类型"
                        },
                        "http_header": {
                            "type": "object",
                            "description": "Http请求Header"
                        },
                        "http_reponse": {
                            "type": "object",
                            "description": "Http响应报文"
                        },
                        "http_request": {
                            "type": "object",
                            "description": "Http请求报文"
                        }
                    }
                },
                "log": {
                    "type": "array",
                    "description": "转换后的日志",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "字段"
                            },
                            "remark": {
                                "type": "string",
                                "description": "字段说明"
                            }
                        }
                    }
                }
            }
        },
        "topRelatedUsers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item": {
                        "type": "string",
                        "description": "用户"
                    },
                    "count": {
                        "type": "string",
                        "description": "出现次数"
                    }
                }
            }
        },
        "Liscense": {
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "description": "版本信息"
                },
                "expire": {
                    "type": "integer",
                    "description": "过期日期",
                    "format": "int64"
                }
            }
        },
        "Digest": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/digestDetail"
                    }
                }
            }
        },
        "digestDetail": {
            "type": "object",
            "properties": {
                "mem": {
                    "$ref": "#/definitions/ratio"
                },
                "cpu": {
                    "$ref": "#/definitions/load"
                },
                "space": {
                    "$ref": "#/definitions/ratio"
                }
            }
        },
        "ratio": {
            "type": "object",
            "properties": {
                "total": {
                    "type": "integer",
                    "format": "int32",
                    "description": "已用内存实大小"
                },
                "ratio": {
                    "type": "number",
                    "format": "double",
                    "description": "已用内存占比"
                },
                "free": {
                    "type": "integer",
                    "format": "int32",
                    "description": "未使用内存大小"
                }
            }
        },
        "load": {
            "type": "object",
            "properties": {
                "load": {
                    "type": "number",
                    "format": "double",
                    "description": "CPU占比"
                }
            }
        },
        "Notice": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "description": "警报详情",
                    "items": {
                        "$ref": "#/definitions/warnDetail"
                    }
                }
            }
        },
        "alarmDetail": {
            "type": "object",
            "properties": {
                "timestamp": {
                    "type": "integer",
                    "description": "规则命中时间"
                },
                "geo_city": {
                    "type": "string",
                    "description": "定位的城市"
                },
                "geo_province": {
                    "type": "string",
                    "description": "城市所属的地区"
                },
                "test": {
                    "type": "boolean",
                    "description": "是否是测试名单"
                },
                "whitelist": {
                    "type": "boolean",
                    "description": "是否是白名单"
                },
                "url": {
                    "type": "string",
                    "description": "攻击URL"
                },
                "expire": {
                    "type": "integer",
                    "description": "过期时间"
                },
                "key": {
                    "type": "integer",
                    "description": "ip地址"
                },
                "tip": {
                    "type": "string",
                    "description": "待定"
                },
                "strategy_name": {
                    "type": "string",
                    "description": "策略名称"
                },
                "scene_name": {
                    "type": "string",
                    "description": "场景名"
                }
            }
        },
        "warnDetail": {
            "type": "object",
            "properties": {
                "geo_city": {
                    "type": "string",
                    "description": "定位的城市"
                },
                "check_type": {
                    "type": "string",
                    "description": "待定"
                },
                "timestamp": {
                    "type": "integer",
                    "description": "两天前至今之间的任意时间点(毫秒为单位)"
                },
                "geo_province": {
                    "type": "string",
                    "description": "城市所属的地区"
                },
                "test": {
                    "type": "boolean",
                    "description": "待定"
                },
                "expire": {
                    "type": "integer",
                    "description": "待定"
                },
                "key": {
                    "type": "integer",
                    "description": "ip地址"
                },
                "remark": {
                    "type": "string",
                    "description": "检测是否爬虫"
                },
                "decision": {
                    "type": "string",
                    "description": "待定"
                },
                "tip": {
                    "type": "string",
                    "description": "待定"
                },
                "variable_values": {
                    "$ref": "#/definitions/httpDetail"
                },
                "risk_score": {
                    "type": "integer",
                    "format": "int32",
                    "description": "风险评估得分"
                },
                "strategy_name": {
                    "type": "string",
                    "description": "策略名称"
                },
                "scene_name": {
                    "type": "string",
                    "description": "场景名"
                }
            }
        },
        "httpDetail": {
            "type": "object",
            "properties": {
                "http_refererhit_count_ip": {
                    "type": "string",
                    "description": "object"
                },
                "http_sbytes_cv_ip": {
                    "type": "number",
                    "format": "double",
                    "description": "待定"
                },
                "http_count_ip": {
                    "type": "integer",
                    "format": "int32",
                    "description": "ip统计"
                },
                "http_static_count_ratio_ip": {
                    "type": "integer",
                    "format": "int32",
                    "description": "静态ip占比"
                },
                "http_dynamic_count_ratio_ip": {
                    "type": "number",
                    "format": "double",
                    "description": "动态ip占比"
                },
                "http_dynamic_distincturl_ip": {
                    "type": "integer",
                    "format": "int32",
                    "description": "待定"
                }
            }
        },
        "Riskyitem": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "$ref": "#/definitions/riskDetail"
                }
            }
        },
        "riskDetail": {
            "type": "object",
            "properties": {
                "account": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/sameDetail"
                    }
                },
                "fraud": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/sameDetail"
                    }
                },
                "visit": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/sameDetail"
                    }
                },
                "other": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/sameDetail"
                    }
                }
            }
        },
        "sameDetail": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "随机定位"
                },
                "strategy": {
                    "type": "string",
                    "description": "策略"
                },
                "count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "计数"
                }
            }
        },
        "Noticestats": {
            "type": "object",
            "properties": {
                "fraud_attack-top": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/accountAtkDetail"
                    },
                    "description": "欺骗攻击排名"
                },
                "account_attack_top": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/accountAtkDetail"
                    },
                    "description": "账户攻击排名"
                },
                "http_attack_top": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/accountAtkDetail"
                    },
                    "description": "http攻击排名"
                },
                "other_attack_top": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/accountAtkDetail"
                    },
                    "description": "其它类型攻击排名"
                }
            }
        },
        "accountAtkDetail": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "被攻击的城市"
                },
                "value": {
                    "type": "integer",
                    "format": "int32",
                    "description": "被攻击的次数"
                }
            }
        },
        "HttpCount": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "description": "攻击总计数",
                    "items": {
                        "$ref": "#/definitions/httpCountDetail"
                    }
                }
            }
        },
        "httpCountDetail": {
            "type": "object",
            "properties": {
                "http_count": {
                    "type": "integer",
                    "format": "int32",
                    "description": "http总计"
                }
            }
        },
        "HttpCountIp": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "description": "IP地址的访问量",
                    "items": {
                        "$ref": "#/definitions/httpCountIpDetail"
                    }
                }
            }
        },
        "httpCountIpDetail": {
            "type": "object",
            "properties": {
                "ipAddress": {
                    "type": "integer",
                    "format": "int32",
                    "description": "ipAddress是个变量，存放前十的ip，值是访问量。"
                }
            }
        },
        "Strategies": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/strategiesDetail"
                    }
                }
            }
        },
        "strategiesDetail": {
            "type": "object",
            "properties": {
                "app": {
                    "type": "string",
                    "description": "app名"
                },
                "createtime": {
                    "type": "integer",
                    "format": "int32",
                    "description": "创建时间"
                },
                "endeffect": {
                    "type": "integer",
                    "format": "int32",
                    "description": "过期时间"
                },
                "modifytime": {
                    "type": "integer",
                    "format": "int32",
                    "description": "修改时间"
                },
                "name": {
                    "type": "string",
                    "description": "规则名称"
                },
                "remark": {
                    "type": "string",
                    "description": "规则说明"
                },
                "starteffect": {
                    "type": "integer",
                    "format": "int32",
                    "description": ""
                },
                "status": {
                    "type": "string",
                    "description": "状态"
                },
                "terms": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/termsDetail"
                    },
                    "description": ""
                },
                "version": {
                    "type": "integer",
                    "format": "int32",
                    "description": ""
                }
            }
        },
        "termsDetail": {
            "type": "object",
            "properties": {
                "left": {
                    "$ref": "#/definitions/leftDetail"
                },
                "op": {
                    "type": "string",
                    "description": ""
                },
                "remark": {
                    "type": "object",
                    "description": "null Value"
                },
                "right": {
                    "$ref": "#/definitions/rightDetail"
                }
            }
        },
        "leftDetail": {
            "type": "object",
            "properties": {
                "subtype": {
                    "type": "string",
                    "description": "子类型"
                },
                "type": {
                    "type": "string",
                    "description": "",
                    "enum": [
                        "event"
                    ]
                },
                "config": {
                    "$ref": "#/definitions/leftConfigDetail"
                }
            }
        },
        "leftConfigDetail": {
            "type": "object",
            "properties": {
                "event": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "field": {
                    "type": "string"
                }
            }
        },
        "rightDetail": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": ""
                },
                "subtype": {
                    "type": "string",
                    "description": ""
                },
                "config": {
                    "$ref": "#/definitions/rightConfigDetail"
                }
            }
        },
        "rightConfigDetail": {
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "description": ""
                }
            }
        },
        "Glossary": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "状态码"
                },
                "msg": {
                    "type": "string",
                    "description": "状态信息"
                },
                "values": {
                    "type": "array",
                    "description": "",
                    "items": {
                        "$ref": "#/definitions/glossaryDetail"
                    }
                }
            }
        },
        "glossaryDetail": {
            "type": "object",
            "properties": {
                "app": {
                    "type": "string",
                    "description": "名"
                },
                "fields": {
                    "type": "array",
                    "description": "",
                    "items": {
                        "$ref": "#/definitions/filedsDetail"
                    }
                },
                "name": {
                    "type": "string",
                    "description": "英文名"
                },
                "remark": {
                    "type": "string",
                    "description": "查询请求"
                },
                "srcId": {
                    "type": "array",
                    "description": "",
                    "items": {
                        "$ref": "#/definitions/srcIdDetail"
                    }
                }
            }
        },
        "filedsDetail": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "请求ID"
                },
                "remark": {
                    "type": "string",
                    "description": "评论"
                },
                "type": {
                    "type": "string",
                    "description": "类型"
                }
            }
        },
        "srcIdDetail": {
            "type": "object",
            "properties": {
                "app": {
                    "type": "string",
                    "description": "名"
                },
                "name": {
                    "type": "string",
                    "description": "英文名"
                }
            }
        },
        "Error": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "format": "int32"
                },
                "error": {
                    "type": "string"
                }
            }
        }
    }
}
