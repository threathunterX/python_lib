#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from os import path

from babel_python.servicemeta import ServiceMeta
from babel_python.serviceclient import ServiceClient
from threathunter_common.event import Event
from threathunter_common.util import millis_now

from nebula_utils import settings
from nebula_utils.persist_utils.utils import get_db_path
from nebula_utils.persist_utils.metrics import catch_latency


logger = logging.getLogger('nebula_utils.persist_compute.profile')
expire_time = 60 * 60 * 24
sep = '\n'


def get_profile_client():
    meta = ServiceMeta.from_json(
        settings.ProfileSendService_redis) if settings.Babel_Mode == 'redis' else ServiceMeta.from_json(settings.ProfileSendService_rmq)
    client = ServiceClient(meta)
    client.start()
    return client


@catch_latency("统计profile变量")
def gen_visit_profile(Stat_Dict):
    """
    统计user维度的变量，发送给Java profile_send server
    需要数据唯一性，已发送过的数据不会再次发送，数据状态持久化在文件
    """
    db_path = get_db_path(settings.Working_TS)
    hour = db_path.split('/')[-1]

    # 初始化获得当前统计日志目录，创建或者读取存储文件
    logger.debug('开始统计用户档案')
    start = millis_now()
    profile_path = path.join(db_path, 'profile.txt')
    if path.isfile(profile_path):
        profile_file = open(profile_path, 'r+')
        profile_list = []
        lines = profile_file.readlines()
        for line in lines:
            profile_list.append(line.replace(sep, ''))
    else:
        profile_file = open(profile_path, 'w')
        profile_list = []

    profile_dict = {'did': [], 'ip': [], 'user': []}

    did_dimension = Stat_Dict.get('did', {})
    for key, variables in did_dimension.iteritems():
        if key and key != 'all' and key not in profile_list:
            did_dict = {}

            # did关联ip列表
            did_ip_var = 'did__visit__dynamic_distinct_ip__1h__slot'
            if variables.get(did_ip_var, []):
                did_dict['did__visit__ip_detail_distinct__profile'] = {
                    hour: list(variables[did_ip_var])}

            if did_dict:
                profile_dict['did'].append((key, did_dict))

    ip_dimension = Stat_Dict.get('ip', {})
    for key, variables in ip_dimension.iteritems():
        if key and key != 'all' and key not in profile_list:
            ip_dict = {}

            # ip关联did列表
            ip_did_var = 'ip__visit__dynamic_distinct_did__1h__slot'
            if variables.get(ip_did_var, []):
                ip_dict['ip__visit__did_detail_distinct__profile'] = {
                    hour: list(variables[ip_did_var])}

            # ip关联user列表
            ip_user_var = 'ip__visit__dynamic_distinct_user__1h__slot'
            if variables.get(ip_user_var, []):
                ip_dict['ip__visit__uid_detail_distinct__profile'] = {
                    hour: list(variables[ip_user_var])}

            # ip小时打开简历次数
            ip_open_var = 'ip__visit__dynamic_open_resume_count__1h__slot'
            if variables.get(ip_open_var, 0):
                ip_dict['ip__visit__open_resume_count__profile'] = {
                    hour: variables[ip_open_var]}

            # ip小时下载简历次数
            ip_download_var = 'ip__visit__dynamic_download_resume_count__1h__slot'
            if variables.get(ip_download_var, 0):
                ip_dict['ip__visit__download_resume_count__profile'] = {
                    hour: variables[ip_download_var]}

            # ip小时导出简历次数
            ip_export_var = 'ip__visit__dynamic_export_resume_count__1h__slot'
            if variables.get(ip_export_var, 0):
                ip_dict['ip__visit__export_resume_count__profile'] = {
                    hour: variables[ip_export_var]}

            # ip小时搜索简历次数
            ip_search_var = 'ip__visit__dynamic_search_resume_count__1h__slot'
            if variables.get(ip_search_var, 0):
                ip_dict['ip__visit__search_resume_count__profile'] = {
                    hour: variables[ip_search_var]}

            if ip_dict:
                profile_dict['ip'].append((key, ip_dict))

    user_dimension = Stat_Dict.get('user', {})
    # user_profiles统计未发送profile_send的用户档案数据
    for key, variables in user_dimension.iteritems():
        if key and key != 'all' and key not in profile_list:
            user_dict = {}

            # 用户最后访问IP
            user_last_ip_var = 'user__visit__dynamic_latest_ip__1h__slot'
            if variables.get(user_last_ip_var, {}).get('value', None):
                user_dict['user__account__last_visit_ip__profile'] = variables[
                    user_last_ip_var]['value']

            # 用户最后访问时间戳
            user_last_timestamp_var = 'user__visit__dynamic_latest_timestamp__1h__slot'
            if variables.get(user_last_timestamp_var, {}).get('value', 0):
                user_dict['user__account__last_visit_timestamp__profile'] = variables[
                    user_last_timestamp_var]['value']

            # 用户当前小时访问地区次数
            user_city_var = 'user__visit__geo_dynamic_count__1h__slot'
            if variables.get(user_city_var, {}):
                user_dict['user__visit__city__profile'] = variables[
                    user_city_var]

            # 用户当前小时访问设备分布
            user_did_var = 'user__visit__did_dynamic_count__1h__slot'
            if variables.get(user_did_var, {}):
                user_dict['user__visit__device__profile'] = variables[
                    user_did_var]

            # 用户小时下载简历次数
            user_download_var = 'user__visit__dynamic_download_resume_count__1h__slot'
            if variables.get(user_download_var, 0):
                user_dict['user__visit__download_resume_count__profile'] = {
                    hour: variables[user_download_var]}

            # 用户当前小时访问访问次数,小时为两位数，不足两位以0补齐
            user_hour_var = 'user__visit__dynamic_count__1h__slot'
            if variables.get(user_hour_var, 0):
                user_dict['user__visit__hour_merge__profile'] = {
                    hour[-2:]: variables[user_hour_var]}

            # 用户按小时IP去重详细列表
            user_ip_var = 'user__visit__dynamic_distinct_ip__1h__slot'
            if variables.get(user_ip_var, []):
                user_dict['user__visit__ip_detail_distinct__profile'] = {hour: list(variables[
                    user_ip_var])}

            # 用户小时打开简历次数
            user_open_var = 'user__visit__dynamic_open_resume_count__1h__slot'
            if variables.get(user_open_var, 0):
                user_dict['user__visit__open_resume_count__profile'] = {
                    hour: variables[user_open_var]}

            # 用户小时导出简历次数
            user_export_var = 'user__visit__dynamic_export_resume_count__1h__slot'
            if variables.get(user_export_var, 0):
                user_dict['user__visit__export_resume_count__profile'] = {
                    hour: variables[user_export_var]}

            # 用户小时搜索简历次数
            user_search_var = 'user__visit__dynamic_search_resume_count__1h__slot'
            if variables.get(user_search_var, 0):
                user_dict['user__visit__search_resume_count__profile'] = {
                    hour: variables[user_search_var]}

            # 用户当前小时访问User Agent分布
            user_ua_var = 'user__visit__ua_dynamic_count__1h__slot'
            if variables.get(user_ua_var, {}):
                user_dict['user__visit__ua__profile'] = variables[user_ua_var]

            if user_dict:
                profile_dict['user'].append((key, user_dict))

    # 发送user_profiles，每100个user发送一次，防止数据量大发送失败，发送完成或发生异常则已发送的数据保存文件
    # 防止profile_send超时消息丢弃，expire time设置较长时间
    client = get_profile_client()
    send_list = []
    try:
        for dimension in ['did', 'ip', 'user']:
            # 保存发送成功的profile，一次性存入文件

            for i in range(0, len(profile_dict[dimension]), 100):
                send_profiles = {dimension: {key: profile_variables for key,
                                             profile_variables in profile_dict[dimension][i:i + 100]}}
                event = Event('nebula', 'VISIT',
                              '', millis_now(), send_profiles)
                bbc, _ = client.send(
                    event, '', True, timeout=10, expire=expire_time)
                if bbc:
                    send_list.extend(send_profiles[dimension].keys())
                else:
                    logger.error('profile send event失败,失败原因: %s' % str(_))
    except Exception as err:
        logger.error(err)
    finally:
        profile_file.write(sep.join(send_list))
        profile_file.close()

    end = millis_now()
    logger.debug('用户档案统计完成')
    logger.debug('时间消耗：{}ms'.format(end - start))
