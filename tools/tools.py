# -*- coding:utf-8 -*-
import pandas as pd
from pyhive import hive
import time
import datetime
import os


def rfail(s, file_path):
    with open(file_path, "a+") as f:
        f.write(s + "\n")


def read_query(sql):
    hive_line = '''hive -e "set hive.cli.print.header=true;set mapreduce.job.queuename=hl_report;%s";''' % (sql)
    data_buffer = os.popen(hive_line)
    data = pd.read_table(data_buffer, sep="\t", chunksize=10000)
    return data


def get_from_hive(query, mode, engine_hive):
    #engine_hive = hive.Connection(host="xxxxx", port=10000, username="xxxx")
    if mode == "pyhive":
        data = pd.read_sql(query, engine_hive)
        return data
    elif mode == "raw":
        data = read_query(query)
        return data
    else:
        print("mode: pyhive or raw")
        return None


def gen_date(bdate, days):
    day = datetime.timedelta(days=1)
    for i in range(days):
        s = bdate + day * i
        # print(type(s))
        yield s.strftime("%Y-%m-%d")


def get_date_list(start=None, end=None):
    if (start is None) | (end is None):
        return []
    else:
        data = []
        for d in gen_date(start, (end - start).days):
            data.append(d)
        return data