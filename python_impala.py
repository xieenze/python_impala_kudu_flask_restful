#coding:utf8
from impala.dbapi import connect
from impala.util import as_pandas
from flask import jsonify
import json
conn = connect(host='10.20.110.2', port=21050, database="db10002", user="admin@evcard", password="admin")
cur = conn.cursor()
cur.execute("select * from province limit 2")

#将结果转换成list
#print cur.fetchall()
#打印表结构
#print cur.description



#将impala查询到的结果转成pandas的dataframe
df = as_pandas(cur)
print df.to_json(orient='index')
print df.to_json(orient='split')
print df.to_json(orient='records')
print df.to_json(orient='columns')
print df.to_json(orient='values')


#更多api详见
#http://blog.cloudera.com/blog/2014/04/a-new-python-client-for-impala/
#http://legacy.python.org/dev/peps/pep-0249/
#https://github.com/cloudera/impyla
#http://www.ibis-project.org/
