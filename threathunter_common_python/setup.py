#!/usr/bin/env python

from setuptools import setup, find_packages


setup(name='threathunter_common',
      version='1.1.1',
      description='common python framwork code in threathunter',
      author='nebula',
      author_email='nebula@threathunter.cn',
      url='https://www.threathunter.cn',
      packages=find_packages(exclude=["test",'*.pyc']),
      install_requires=[
            "ipaddr==2.2.0", "redis==2.10.6", "pyyaml==3.13", "lxml==4.2.5", "chardet===3.0.4", "sqlalchemy==1.2.15", "influxdb==5.2.1", "thrift==0.11.0",  "ipcalc==1.99.0",
            "six==1.12.0", "redis-py-cluster==1.3.6"
      ],
      package_data={'': ['threathunter_common/geo/*.dat']},
      include_package_data=True)
