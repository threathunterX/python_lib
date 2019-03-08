#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='nebula_strategy',
    version='1.1.1',
    description=('nebula strategy for strategy checking and variable generating'),
    author='nebula',
    author_email='nubula@threathunter.cn',
    url='https://www.threathunter.cn',
    packages=find_packages(exclude=["test"]),
    package_data={'': []},
    install_requires=["requests"],
    include_package_data=True
)
