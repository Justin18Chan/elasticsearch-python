#-*- coding:utf-8 -*-
from ES import ESObj
import utils
import numpy as np
import pandas as pd
import os
import base64
from tqdm import tqdm


log = "log/log.log"


def create_index_by_file(es, index, settings, mappings, data):
    # 
    res = es.indicesExists(index)
    if res == True:
        res = es.deleteIndices(index=index)
        print(res)
        
    res = es.createIndex(index, settings, mappings)
    if res['acknowledged']:
        if isinstance(data, list):
            for item in tqdm(data):
                if "id" in item:
                    id = item.pop("id")
                    body = item.copy()
                    res = es.updateDoc(index, id=id, body=body)
                else:
                    body = item.copy()
                    res = es.insertDoc(index, body=body)
    else:
        pass




if __name__ == '__main__':
    logging = utils.getLogger(log)
    logging.info(__file__)
    es = ESObj(host="http://192.168.10.50", port=9200, http_auth=('elastic', '3wN0OQlt1aewDl6qUiwh'))
    index = "test_index"
    users_info = utils.loadConfig("./mappings/users_info.json")
    df = pd.read_csv("data/user_info.csv", header=0)
    df = df.fillna("")
    data = df.to_dict(orient="records")

    print(data[0])
    body = []
    for per in data:
        result = per.copy()
        
        for x in per:
            if per[x] == "":
                result.pop(x)
        body.append(result)
    create_index_by_file(es, index, settings=users_info['settings'], mappings=users_info['mappings'], data=body)
