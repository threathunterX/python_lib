#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from os import path as opath

from complexconfig.loader.file_loader import FileLoader
from complexconfig.parser.properties_parser import PropertiesParser
from complexconfig.config import Config, CascadingConfig
from complexconfig.configcontainer import configcontainer

# path
Base_Path = opath.dirname(__file__)

# build conf path
Conf_Local_Path = opath.join(Base_Path, "conf", "settings.conf")
Conf_Global_Path = "/etc/nebula/nebula.conf"
Conf_Web_Path = "/etc/nebula/web/settings.conf"
if not os.path.exists(Conf_Global_Path) or not os.path.isfile(Conf_Global_Path):
    print "!!!!Fatal, global conf path {} doesn't exist, using the local path {}".format(Conf_Global_Path, Conf_Local_Path)
    Conf_Global_Path = Conf_Local_Path
if not os.path.exists(Conf_Web_Path) or not os.path.isfile(Conf_Web_Path):
    print "!!!!Fatal, web conf path {} doesn't exist, using the local path {}".format(Conf_Web_Path, Conf_Local_Path)

# init the global config
global_config_loader = FileLoader("global_config_loader", Conf_Global_Path)
global_config_parser = PropertiesParser("global_config_parser")
global_config = Config(global_config_loader, global_config_parser)
global_config.load_config(sync=True)

# init the web config
web_config_loader = FileLoader("web_config_loader", Conf_Web_Path)
web_config_parser = PropertiesParser("web_config_parser")
web_config = Config(web_config_loader, web_config_parser)
web_config.load_config(sync=True)

# build the cascading config
# file config will be updated every half an hour, while the web config
# will be updated every 5 minute
cascading_config = CascadingConfig(global_config, web_config)
configcontainer.set_config("nebula", cascading_config)
_config = configcontainer.get_config("nebula")

OFFLINE_QUERY_ADDR = '0.0.0.0'
OFFLINE_QUERY_PORT = 9451
LogPath_Format = '%Y%m%d%H'
Working_TS = None
Working_DAY = None
Auth_Code = "196ca0c6b74ad61597e3357261e80caf"

OfflineStatService_rmq = """
{
    "name": "offline_stat_query",
    "callmode": "rpc",
    "delivermode": "sharding",
    "serverimpl": "rabbitmq",
    "coder": "mail",
    "options": {
        "serversubname": "",
        "servercardinality": 2,
        "serverseq": 1,
        "sdc":"nebula",
        "cdc":"nebula"
    }
}
"""

OfflineStatService_redis = """
{
    "name": "offline_stat_query",
    "callmode": "rpc",
    "delivermode": "sharding",
    "serverimpl": "redis",
    "coder": "mail",
    "options": {
        "serversubname": "",
        "servercardinality": 1,
        "serverseq": 1,
        "sdc":"nebula",
        "cdc":"nebula"
    }
}
"""

ProfileSendService_redis = """{
    "name": "profile_send",
    "callmode": "notify",
    "delivermode": "sharding",
    "serverimpl": "redis",
    "coder": "mail",
    "options": {
        "cdc": "sh",
        "sdc": "sh",
        "serversubname": "onlinekv",
        "serverseq": 1,
        "servercardinality": 1
    }
}"""

ProfileSendService_rmq = """{
    "name": "profile_send",
    "callmode": "notify",
    "delivermode": "sharding",
    "serverimpl": "rabbitmq",
    "coder": "mail",
    "options": {
        "cdc": "sh",
        "sdc": "sh",
        "serversubname": "onlinekv",
        "serverseq": 1,
        "servercardinality": 1
    }
}"""

WebUI_Address = _config.get_string("webui_address", "127.0.0.1")
WebUI_Port = _config.get_int("webui_port", 9001)
Redis_Host = _config.get_string("redis_host", "127.0.0.1")
Redis_Port = _config.get_int("redis_port", 6379)
Babel_Mode = _config.get_string("babel_server", "redis")
Metrics_Server = _config.get_string("metrics_server", "redis")
Influxdb_Url = _config.get_string("influxdb_url", "http://127.0.0.1:8086/")
DB_ROOT_PATH = _config.get_string("persist_path", "./")
Expire_Days = _config.get_int("persist_expire_days", 10)
AeroSpike_Address = _config.get_string("aerospike_address", "127.0.0.1")
AeroSpike_Port = _config.get_int("aerospike_port", 3000)
AeroSpike_Timeout = _config.get_int("aerospike_timeout", 2000)
AeroSpike_DB_Name = _config.get_string("aerospike_offline_db", "offline")
AeroSpike_DB_Expire = _config.get_int("aerospike_offline_expire", 1)
Temp_Query_Path = _config.get_string("logquery_path", "./")
MySQL_Host = _config.get_string("mysql_host", "127.0.0.1")
MySQL_Port = _config.get_int("mysql_port", 3306)
MySQL_User = _config.get_string("mysql_user", "root")
MySQL_Passwd = _config.get_string("mysql_passwd", "passwd")
Nebula_Data_DB = _config.get_string("nebula_data_db", "nebula_data")
ContinuousDB_Config = {
    'hosts': [
        (AeroSpike_Address, AeroSpike_Port)
    ],
    'policies': {
        'timeout': AeroSpike_Timeout
    }
}
