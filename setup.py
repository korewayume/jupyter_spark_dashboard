#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from setuptools import setup, find_packages

with open('package.json') as pkg_file:
    version = json.load(pkg_file)['version']

setup(
    name='jupyter_spark_monitor',
    version=version,
    description='Spark Monitor Extension for Jupyter',
    author='korewayume',
    author_email='korewayume@foxmail.com',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
