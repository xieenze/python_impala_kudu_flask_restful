# -*- coding: utf-8 -*-
from __future__ import print_function
from impala.dbapi import connect
import configparser
import datetime
import numpy as np
import json



def get_now_and_7days_time():
    datetime.timedelta()
    now_time = datetime.datetime.now()
    seven_days_time = now_time + datetime.timedelta(weeks=-1)
    now_time = now_time.strftime('%Y%m%d')
    seven_days_time = seven_days_time.strftime('%Y%m%d')
    return now_time,seven_days_time

def get_yes_time():
    datetime.timedelta()
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(-1)
    yes_time = yes_time.strftime('%Y%m%d')
    return yes_time

def get_conn():
    conn = connect(host='10.20.110.2', port=21050, database="db10002", user="admin@evcard", password="admin")
    cur = conn.cursor()
    return cur


def get_sql(section,key):
    config = configparser.ConfigParser()
    config.read('sql.ini')
    sql = config.get(section, key)
    return sql

def trans_array(df):
    d = np.array(df).transpose().tolist()
    dictt = {"income_date": d[0], "total": d[1], "actual": d[2], "coupon": d[3], "ecurrency": d[4]}
    res = (json.dumps(dictt, ensure_ascii=False))
    return res

def trans_array2(df):
    dict2 = []
    d = np.array(df).tolist()
    for i in range(0, len(d)):
        dict = {"name": d[i][0], "value": [d[i][1], d[i][2], d[i][3]], "shop_seq": d[i][4]}
        dict2.append(dict)
    res = (json.dumps(dict2, ensure_ascii=False))
    return res
