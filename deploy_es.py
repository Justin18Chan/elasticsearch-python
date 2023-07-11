#-*- coding:utf-8 -*-
from fastapi import FastAPI, Request
import uvicorn, json, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import exceptions
import pandas as pd
import numpy as np
import os
import base64


app = FastAPI()

error_code = {
    "200": "正常",
    "300": "未知异常",
    "600": "ES连接失败",
    "400": "document解析失败，或格式错误",
    "500": "interface参数错误",
    "510": "index参数为空",
    "511": "index索引不存在",
    "520": "id参数为空",
    "521": "id已经存在",
    "522": "id不存在"
}



def es_interface(es, **es_kwargs):
    """
    支持接口功能包括：
    1) create:没有id则创建, id存在则不处理;
    2) insert:没有id则创建, id存在则更新;
    2) update:没有id则不处理, 有则更新;
    3) delete:删除一条旧样本;
    """

    code = 300
    msg = ""
    if es_kwargs["interface"] is None or es_kwargs['interface'] not in ['create', 'insert', 'update', 'delete']:
        code = 300
        msg = "interface参数错误!"
        return code, msg
    
    index = es_kwargs['index']
    if index is None:
        code = 510
        msg = "索引index参数为空"
        return code, msg
    
    index_exists = es.indices.exists(index=index)
    if index_exists == False:#索引不存在
        code = 511
        msg = "输入的索引不存在"
        return code, msg
    
    id = es_kwargs['id']
    id_exist = es.exists(index=index, id=id) # 是否存在该id

    if es_kwargs['interface'] == "create":# 创建一个新用户
        if id_exist:
            code = 521
            msg = "索引中已经存在id：{id}, 创建失败！"
            return code, msg
        else:
            document = es_kwargs['document']
            try:
                res = es.index(index=index, id=id, document=document)
                code = 200
                msg = res
                return code, msg
            except exceptions.BadRequestError as e:
                msg = e
                code = 400
                return code, msg
    elif es_kwargs['interface'] == "insert":
        document = es_kwargs['document']
        try:
            res = es.index(index=index, id=id, document=document)
            code = 200
            msg = res
            return code, msg
        except exceptions.BadRequestError as e:
            msg = e
            code = 400
            return code, msg
    elif es_kwargs['interface'] == "update":
        document = es_kwargs['document']
        if not id_exist:
            code = 522
            msg = f"更新样本失败, id: {id}不存在!"
            return code, msg
        try:
            res = es.index(index=index, id=id, document=document)
            code = 200
            msg = res
            return code, msg
        except exceptions.BadRequestError as e:
            msg = e
            code = 400
            return code, msg
    elif es_kwargs['interface'] == "delete":
        if not id_exist:
            code = 522
            msg = f"删除失败, id: {id}不存在!"
            return code, msg
        try:
            res = es.delete(index=index, id=id)
            if res['result'] == 'deleted':
                code = 200
                msg = res
                return code, msg
            else:
                return code, msg
        except exceptions.BadRequestError as e:
            msg = e
            code = 400
            return code, msg
    else:
        code = 300
        msg = "未知的接口"

    return code, msg


@app.post("/ai/es")
async def create_item(request: Request):
    json_post_raw = await request.json()
    json_post = json.dumps(json_post_raw)
    json_post_list = json.loads(json_post)
    # host = json_post_list.get('host')
    # port = json_post_list.get('port')
    user = json_post_list.get('user')
    passwd = json_post_list.get('passwd')
    request_timeout = json_post_list.get('request_timeout')
    index = json_post_list.get('index')
    user_id = json_post_list.get('id')
    document = json_post_list.get('document')
    interface = json_post_list.get('interface')
    es_kwargs = {"interface":interface, "index": index, "id": user_id, "document":document}

    host = "http://192.168.10.50"
    port=9200
    #user = 'elastic'
    #passwd = '3wN0OQlt1aewDl6qUiwh'
    request_timeout = None
    code = 200
    msg = ""
    if not user is None and not passwd is None:
        es = Elasticsearch([str(host)+":"+str(port)], http_auth=(user, passwd), request_timeout=request_timeout)
    else:
        es = Elasticsearch([str(host)+":"+str(port)], request_timeout=request_timeout)
    if es is None:
        msg = '连接失败，请检查ES参数!'
        code = 600
    else:
        if not es.ping():
            msg = "ES集群未启动!请检查ES参数是否正确!"
            code = 600
        else:
            code, msg = es_interface(es, **es_kwargs)


    now = datetime.datetime.now()
    time = now.strftime("%Y-%m-%d %H:%M:%S")
    answer = {
        "status": code,
        "msg":msg, 
        "time": time
    }
    return answer



if __name__ == '__main__':
    uvicorn.run(app, host='192.168.10.50', port=9222, workers=1)



