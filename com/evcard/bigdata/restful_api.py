# -*- coding: utf-8 -*-
from __future__ import print_function
from flask import Flask, jsonify,request
from flask import make_response
from impala.util import as_pandas
import json
from com.evcard.bigdata.Commonutils import get_sql,get_conn,get_now_and_7days_time,get_yes_time,trans_array,trans_array2


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
cur=get_conn()

'''获得省'''
@app.route('/evcard-datav/api/province', methods=['GET'])
def get_province():
    sql_pro = get_sql("sql", "sql_pro")
    cur.execute(sql_pro)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
'''获得市'''
@app.route('/evcard-datav/api/city/<provinceid>', methods=['GET'])
def get_city(provinceid):
    tmpsql =  get_sql("sql","sql_city")+provinceid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
'''获得区'''
@app.route('/evcard-datav/api/area/<cityid>', methods=['GET'])
def get_area(cityid):
    tmpsql = get_sql("sql","sql_area") + cityid
    cur.execute(tmpsql)
    df = as_pandas(cur)
    return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

'''1网点response'''
@app.route('/evcard-datav/api/shop')
def api_shop():
    yes_time = get_yes_time()
    sql = get_sql("sql","sql_shop")\
        .replace("long1",str(float(request.args['longitude1'])*1000000)) \
        .replace("long2", str(float(request.args['longitude2'])*1000000))\
        .replace("lati1",str(float(request.args['latitude1'])*1000000))\
        .replace("lati2",str(float(request.args['latitude2'])*1000000))\
        .replace("yes_time",yes_time)
    print (sql)
    cur.execute(sql)
    df = as_pandas(cur)
    print (len(df))
    res=trans_array2(df)
    return res

'''2支付比例折线图'''
@app.route('/evcard-datav/api/payrate_zhexian')
def api_payrate_zhexian():
    now_time, seven_days_time = get_now_and_7days_time()
    if 'provinceid' in request.args and 'cityid' in request.args and "areaid" in request.args and "shopid" not in request.args:
        sql=get_sql("sql","sql_payrate_zhexian1")\
            .replace("get_provinced_id",request.args['provinceid'])\
            .replace("get_city_id",request.args['cityid'])\
            .replace("get_area_id",request.args['areaid'])\
            .replace("seven_days_time","'"+seven_days_time+"'")\
            .replace("now_time","'"+now_time+"'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        res = trans_array(df)
        return res
    if 'provinceid' in request.args and 'cityid' in request.args and "shopid" not in request.args:
        sql = get_sql("sql", "sql_payrate_zhexian2") \
            .replace("get_provinced_id", request.args['provinceid']) \
            .replace("get_city_id", request.args['cityid']) \
            .replace("seven_days_time", "'"+seven_days_time+"'") \
            .replace("now_time", "'"+now_time+"'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        res = trans_array(df)
        return res
    if 'provinceid' in request.args and "shopid" not in request.args:
        sql = get_sql("sql", "sql_payrate_zhexian3") \
            .replace("get_provinced_id", request.args['provinceid']) \
            .replace("seven_days_time", "'"+seven_days_time+"'") \
            .replace("now_time", "'"+now_time+"'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        res = trans_array(df)
        return res
    if 'shopid' in request.args :
        sql = get_sql("sql", "sql_payrate_zhexian4") \
            .replace("shop__", request.args['shopid']) \
            .replace("seven_days_time", "'" + seven_days_time + "'") \
            .replace("now_time", "'" + now_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        res = trans_array(df)
        return res
    else:
        sql = get_sql("sql", "sql_payrate_zhexian5") \
            .replace("seven_days_time", "'"+seven_days_time+"'") \
            .replace("now_time", "'"+now_time+"'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        res = trans_array(df)
        return res


'''3支付比例饼图'''
@app.route('/evcard-datav/api/payrate_bing')
def api_payrate_bing():
    yes_time = get_yes_time()
    if 'provinceid' in request.args and 'cityid' in request.args and "areaid" in request.args and "shopid" not in request.args:
        sql=get_sql("sql","sql_payrate1")\
            .replace("province_",request.args['provinceid'])\
            .replace("city_",request.args['cityid'])\
            .replace("area_",request.args['areaid']) \
            .replace("yes_time", "'" + yes_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    if 'provinceid' in request.args and 'cityid' in request.args and "shopid" not in request.args:
        sql = get_sql("sql", "sql_payrate2") \
            .replace("province_", request.args['provinceid']) \
            .replace("city_", request.args['cityid']) \
            .replace("yes_time", "'" + yes_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    if 'provinceid' in request.args and "shopid" not in request.args:
        sql = get_sql("sql", "sql_payrate3") \
            .replace("province_", request.args['provinceid']) \
            .replace("yes_time", "'" + yes_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)
    if 'shopid' in request.args :
        sql = get_sql("sql", "sql_payrate4") \
            .replace("shop__", request.args['shopid']) \
            .replace("yes_time", "'" + yes_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)

    else:
        sql = get_sql("sql", "sql_payrate5") \
            .replace("yes_time", "'" + yes_time + "'")
        #print (sql)
        cur.execute(sql)
        df = as_pandas(cur)
        return json.dumps(df.to_dict(orient='records'), ensure_ascii=False)




@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    app.run(debug=False,port=5000,host='0.0.0.0')
