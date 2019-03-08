#!/usr/bin/env python


from setuptools import setup, find_packages

setup(name='babel_python',
      version='1.1.1',
      description='python implementation of babel client and server',
      author='nebula',
      author_email='nebula@threathunter.cn',
      url='https://www.threathunter.cn',
      install_requires=[
          'cffi', 'pika==0.9.14', 'haigha', 'gevent', 'six', 'mmh3==2.3.1', 'kombu'
      ],
      packages=find_packages(exclude=["test",'*.pyc']),
      include_package_data=True
     )
