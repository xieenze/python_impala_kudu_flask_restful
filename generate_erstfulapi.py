# -*- coding: utf-8 -*-
from __future__ import print_function
from impala.dbapi import connect
from flask import Flask, jsonify
from flask import make_response
from flask_script import Manager
from impala.util import as_pandas
import json

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
#manager = Manager(app)

@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    conn = connect(host='10.20.110.2', port=21050, database="db10002", user="admin@evcard", password="admin")
    cur = conn.cursor()
    cur.execute("select * from org_info limit 2")
    #a = cur.fetchall()
    df = as_pandas(cur)

    #多种转换json方式
    #return jsonify(a)
    #return json.dumps(a,ensure_ascii=False)
    return df.to_json(orient='records')
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    app.run(debug=True,port=5000,host='0.0.0.0')
