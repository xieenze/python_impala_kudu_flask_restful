# -*- coding: utf-8 -*-
from __future__ import print_function
from impala.dbapi import connect
import configparser


def get_conn():
    conn = connect(host='10.20.110.2', port=21050, database="db10002", user="admin@evcard", password="admin")
    cur = conn.cursor()
    return cur


def get_sql(section,key):
    config = configparser.ConfigParser()
    config.read('sql.ini')
    sql = config.get(section, key)
    return sql