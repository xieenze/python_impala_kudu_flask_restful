#coding:utf8

import requests

#res = requests.get("http://10.20.130.23:5000/todo/api/v1.0/tasks")
res = requests.get("http://localhost:5000/todo/api/v1.0/tasks")
print res.text
