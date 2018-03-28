#!/home/hbonline/anaconda3/bin/python
# -*- coding:utf-8 -*-
import pandas as pd
from pyhive import hive
from sqlalchemy import create_engine
import fire


class MyExporter:
    def __init__(self):
        # 配置hive的连接
        self.engine_hive = hive.Connection(host="夏天夏天过去留下小咪咪", port=10000, username="夏天夏天过去留下小咪咪")
        # 配置mysql的数据库连接
        self.engine_mysql = create_engine("mysql+pymysql://root:夏天夏天过去留下小咪咪@夏天夏天过去留下小咪咪:夏天夏天过去留下小咪咪/夏天夏天过去留下小咪咪?charset=utf8", echo=False, encoding="utf8")

    def byquery(self, query, mysql_tablename, ckz=None):
        if ckz:
            qdata = pd.read_sql(query, self.engine_mysql, chunksize=ckz)
            for sub in qdata:
                sub.to_sql(mysql_tablename, self.engine_mysql, index=False)
        else:
            qdata = pd.read_sql(query, self.engine_mysql)
            qdata.to_sql(mysql_tablename, self.engine_mysql, index=False)

    def byfile(self, filename, mysql_tablename, ckz=None):
        current_query = open(filename, "r+").read()
        if ckz:
            qdata = pd.read_sql(current_query, self.engine_mysql, chunksize=ckz)
            for sub in qdata:
                sub.to_sql(mysql_tablename, self.engine_mysql, index=False)
        else:
            qdata = pd.read_sql(current_query, self.engine_mysql)
            qdata.to_sql(mysql_tablename, self.engine_mysql, index=False)

    def bytable(self, hive_tablename, mysql_tablename, ckz=None):
        current_query = "select * from {}".format(hive_tablename)
        if ckz:
            qdata = pd.read_sql(current_query, self.engine_mysql, chunksize=ckz)
            cols = list(qdata.columns)
            columns = {}
            for i in cols:
                columns[i] = str(i).split(".")[1]
            for sub in qdata:
                sub.rename(columns=columns, inplace=True)
                sub.to_sql(mysql_tablename, self.engine_mysql, index=False)
        else:
            qdata = pd.read_sql(current_query, self.engine_mysql)
            cols = list(qdata.columns)
            columns = {}
            for i in cols:
                columns[i] = str(i).split(".")[1]
            qdata.rename(columns=columns, inplace=True)
            qdata.to_sql(mysql_tablename, self.engine_mysql, index=False)


if __name__ == '__main__':
    fire.Fire(MyExporter)
