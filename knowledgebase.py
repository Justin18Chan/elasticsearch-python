#-*- coding:utf-8 -*-
from ES import ESObj
import utils
import numpy as np
import pandas as pd
import os
import base64


log = "log/log.log"


if __name__ == '__main__':
    logging = utils.getLogger(log)
    logging.info("Start")
    
    es = ESObj(host="http://192.168.10.50", port=9200, http_auth=('elastic', '3wN0OQlt1aewDl6qUiwh'))

    users_info = utils.loadConfig("./mappings/users_info.json")
    indexArray = {"users_info": {"file":"data/users_info.csv", "mappings":users_info}}

    for index in indexArray.keys():
        print("创建索引：{}".format(index))
        es.createIndex(index=index, body=indexArray[index]["mappings"])

    for index in indexArray.keys():
        print(index)
        data = []
        tables = utils.getDataFromXLSX(indexArray[index]["file"])
        for table in tables:
            cols = table.columns
            for i in range(len(table)):
                if i >100:
                    continue
                line = dict()
                for col in cols:
                    if pd.isnull(table.loc[i, col]) or table.loc[i, col] == '' or table.loc[i, col] is None or table.loc[i, col] == 'nan':
                        continue

                    line[col] = str(table.loc[i, col]).strip()
                data.append(line)
        es.jsonDataToES(index=index, jsonData=data)

