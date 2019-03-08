#!/usr/bin/env python

from distutils.core import setup


setup(name='nebula_meta',
      version='1.1.1',
      description='nebula meta including common utility, rpc framework and other facilities, replace the nebula_backend repo',
      author='nebula',
      author_email='nebula@threathunter.cn',
      maintainer='nebula',
      maintainer_email='nebula@threathunter.cn',
      url='https://www.threathunter.cn',
      packages=['nebula_meta', 'nebula_meta.model'],
     )
