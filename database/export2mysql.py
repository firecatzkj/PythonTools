# -*- coding:utf-8 -*-
'''
使用pyhive将hive中的表导入到mysql的命令行脚本
'''
import pandas as pd
from pyhive import hive
from sqlalchemy import create_engine
import sys
'''
用法:
    python export2mysql.py [hive_db].[hive_tableName] [mysql_db].["mysql_tableName"]
'''
# 配置hive的连接
engine_hive = hive.Connection(host="x.x.x.x",
                              port=10000,
                              username="xxxx")
# 配置mysql的数据库连接
engine_mysql = create_engine("mysql+pymysql://[数据库用户名]:[数据库密码]@[数据库地址]:[端口]/[数据库名]?charset=utf8",
                             echo=False, encoding="utf8")

# 读取数据转换成dataframe
# 数据库查询语句可以自定义,这个地方是为了把数据库表整个导出写的通用语句
# 在这里加上chunksize=20000是为了导出大量数据
data = pd.read_sql("select * from {}".format(sys.argv[1]),engine_hive,chunksize=20000)
s = 0
for d in data:
    for c in d.columns:
        # 因为select * 会导致所有的column前带有数据库名,所以在这里进行处理
        new_col = c.split(".")[1]
        d[new_col] = d[c]
        del d[c]
    # 存储到数据库的时候,建议先建表,加索引
    d.to_sql(sys.argv[2],engine_mysql,if_exists="append")
    s += 1
    print("done:", s)