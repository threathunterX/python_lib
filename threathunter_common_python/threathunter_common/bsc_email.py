#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# wxt on 2015-06-02.

"""
Email 发送模块
"""

import smtplib,logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger()

class Email(object):
    
    def __init__(self,mail_host,mail_from_addr,mail_from_pwd,mail_from_user):
        """
        @param mail_host: 发送邮件服务器
        @param mail_from_addr: 发送的邮件地址
        @param mail_from_pwd: 密码
        @param mail_from_user: 登陆账号
        """
        self.mail_host = mail_host
        self.mail_from_addr = mail_from_addr
        self.mail_from_pwd = mail_from_pwd
        self.mail_from_user = mail_from_user

    def send(self,subject, content, recipients, carbon_copys=[], attachment=None, attach_send_name='attach.zip'):
        """
        邮件发送接口
        @param subject: 主题
        @param content: 邮件内容
        @param recipients: 收件人（list）
        @param carbon_copys: 抄送人(list)
        @param attachment: 附件名
        @param attach_send_name: 附件发送名
        @return: None
        """
        if len(recipients) == 0:
            logger.warn("No notifiers")
            return False
        # 邮件信息
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.mail_from_addr
        msg['To'] = ';'.join(recipients)
        msg['CC'] = ';'.join(carbon_copys)
        # 邮件内容
        contents = MIMEText(content.encode('UTF-8'), "html", 'utf-8')
        msg.attach(contents)
        # 附件
        if attachment is not None:
            att = MIMEText(open(attachment, 'rb').read(), 'base64', 'gb2312')
            att["Content-Type"] = 'application/octet-stream'
            att["Accept-Language"]="zh-CN"
            att["Accept-Charset"]="ISO-8859-1,utf-8"
            att.add_header('content-disposition', 'attachment', filename=attach_send_name)
            msg.attach(att)
        # 发送
        try:
            send_smtp = smtplib.SMTP(self.mail_host)
            send_smtp.login(self.mail_from_user, self.mail_from_pwd)
            send_smtp.sendmail(self.mail_from_addr, recipients+carbon_copys, msg.as_string())
            send_smtp.close()
        except Exception, e:
            logger.exception('Send email error: %s' % e)
            return False
        return True
    
    
if __name__ == '__main__':
    email = Email("smtp.exmail.qq.com", "app@threathunter.cn", "0QFrEYJV", "app@threathunter.cn")
    email.send(subject="123", content="123", recipients=["xt.wang@threathunter.cn"], carbon_copys=[], attachment=None, 
              attach_send_name='')