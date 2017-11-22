# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
spark = SparkSession()

# 读取hive的表
data1 = spark.sql("query")

# 读取mysql
data2 = spark.read.format("jdbc").options(
    url = "jdbc:mysql://url:port/db",
    driver="com.mysql.jdbc.Driver",
    dbtable="tableName",
    user="root",
    password="").load()
