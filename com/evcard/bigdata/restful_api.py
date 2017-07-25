# -*- coding: utf-8 -*-
from __future__ import print_function
from flask import Flask, jsonify,request
from flask import make_response
from impala.util import as_pandas
import json
from com.evcard.bigdata.utils import get_sql,get_conn

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
cur=get_conn()


@app.route('/api/province', methods=['GET'])
def get_province():
    sql_pro = get_sql("sql", "sql_pro")
    cur.execute(sql_pro)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

@app.route('/api/city/<provinceid>', methods=['GET'])
def get_city(provinceid):
    tmpsql =  get_sql("sql","sql_city")+provinceid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

@app.route('/api/area/<cityid>', methods=['GET'])
def get_area(cityid):
    tmpsql = get_sql("sql","sql_area")+cityid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

@app.route('/api/payrate')
def api_hello():
    if 'province' in request.args and 'city' in request.args and "area" in request.args:
        sql=get_sql("sql","sql_payrate1")\
            .replace("province_",request.args['province'])\
            .replace("city_",request.args['city'])\
            .replace("area_",request.args['area'])
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    if 'province' in request.args and 'city' in request.args:
        sql = get_sql("sql", "sql_payrate2") \
            .replace("province_", request.args['province']) \
            .replace("city_", request.args['city'])
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    if 'province' in request.args :
        sql = get_sql("sql", "sql_payrate3") \
            .replace("province_", request.args['province'])
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    else:
        sql = get_sql("sql", "sql_payrate4")
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    app.run(debug=False,port=5000,host='0.0.0.0')