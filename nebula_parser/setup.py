#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='nebula_parser',
    version='1.1.1',
    description=('nebula parser parse event logs'),
    author='nebula',
    author_email='nebula@threathunter.cn',
    url='https://www.threathunter.cn',
    packages=find_packages(exclude=["tests"]),
    package_data={'': []},
    install_requires=["requests"],
    include_package_data=True
)
