# -*- coding: utf-8 -*-
from pkg_resources import resource_filename
from pyspark.conf import SparkConf


class JupyterSparkConf(SparkConf):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set("spark.extraListeners", "sparkmonitor.listener.JupyterSparkMonitorListener")
        self.set("spark.driver.extraClassPath", resource_filename('jupyter_spark_monitor', "/listener.jar"))


def patch_jupyter_spark_conf():
    from pyspark import conf
    conf.SparkConf = JupyterSparkConf
    import pyspark
    pyspark.SparkConf = JupyterSparkConf


patch_jupyter_spark_conf()


def init_extension(ipython):
    with open(__file__) as f:
        source = f.read()
    ipython.ex(source)
