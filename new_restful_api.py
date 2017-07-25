# -*- coding: utf-8 -*-
from __future__ import print_function
from impala.dbapi import connect
from flask import Flask, jsonify
from flask import make_response
from impala.util import as_pandas
import json

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

sql_pro = '''
SELECT DISTINCT
	(p.PROVINCE),
	p.provinceid
FROM
	shop_info s
INNER JOIN area a ON s.AREA_CODE = a.AREAID
AND s.shop_status = 3
INNER JOIN city c ON c.CITYID = a.FATHERID
INNER JOIN province p ON p.PROVINCEID = c.FATHERID
'''

sql_city = '''
SELECT DISTINCT
	(c.city),
	c.cityid
FROM
	shop_info s
INNER JOIN area a ON s.AREA_CODE = a.AREAID
AND s.shop_status = 3
INNER JOIN city c ON c.CITYID = a.FATHERID
INNER JOIN province p ON p.PROVINCEID = c.FATHERID
where p.provinceid=
'''

sql_area = '''
SELECT DISTINCT
	(a.area),a.areaid
FROM
	shop_info s
INNER JOIN area a ON s.AREA_CODE = a.AREAID
AND s.shop_status = 3
INNER JOIN city c ON c.CITYID = a.FATHERID
INNER JOIN province p ON p.PROVINCEID = c.FATHERID
where c.cityid=
'''


def get_conn():
    conn = connect(host='x.x.x.x', port=21050, database="dbname", user="user", password="passwd")
    cur = conn.cursor()
    return cur
@app.route('/api/province', methods=['GET'])
def get_province():
    cur=get_conn()
    cur.execute(sql_pro)
    df = as_pandas(cur)

    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
@app.route('/api/city/<provinceid>', methods=['GET'])
def get_city(provinceid):
    cur=get_conn()
    tmpsql = sql_city+provinceid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

@app.route('/api/area/<cityid>', methods=['GET'])
def get_area(cityid):
    cur=get_conn()
    tmpsql = sql_area+cityid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    app.run(debug=True,port=5000,host='0.0.0.0')
