#-*- coding:utf-8 -*-
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd


class ESObj(object):
    def __init__(self, host='http://localhost', port=9200, http_auth=None, timeout=3600):
        self.host = host
        self.port = port
        http_port = str(self.host)+":"+str(self.port)
        self.http_auth = http_auth
        self.timeout = timeout
        try:
            if self.http_auth is None:
                self.es = Elasticsearch([http_port], timeout=timeout)
            else:
                self.es = Elasticsearch([http_port], http_auth=self.http_auth,timeout=timeout)
        except Exception as e:
            print(e)
            self.es=None




    def info(self):
        """
        获取当前集群的基本信息。
        """
        res = self.es.info()
        return res
    
    def ping(self):
        """
        判断集群是否已启动
        return: 启动 True; 未启动 False.
        """
        res = self.es.ping()
        return res


    def createIndex(self, index, settings, mappings):
        """
        创建索引
        :param index:
        :param body:
        :return:
        """
        res = self.es.indices.create(index=index, settings=settings, mappings=mappings)
        return res
        
    

    def updateDoc(self, index, id, body):
        """
        更新一个已有id的文档
        :param index:索引
        :param id:文档id
        :param body:更新的文档数据
        :return:
        """
        res = self.es.index(index=index, id=id, body=body)
        return res
    
    def insertDoc(self, index, body):
        """
        插入一条新文档
        :param index:索引
        :param body:更新的文档数据
        :return:
        """
        res = self.es.index(index=index, body=body)
        return res
    
    def getDocFromId(self, index, id, doc_type='doc'):
        """
        查询索引中的指定文档
        :param index:索引
        :param doc_type:文档数据类型
        :param id:文档id
        :return:
        """
        res = self.es.get(index=index, id=id, doc_type=doc_type)
        return res
    
    def getSourceDocFromId(self, index, id, doc_type='doc'):
        """
        通过索引、类型和ID获取文档的来源，直接返回想要的字典。
        :param index:索引
        :param doc_type:文档数据类型
        :param id:文档id
        return: 直接返回结果的字典，example:{'name': '张三', 'age': 19}
        """
        res = self.es.get(index=index, id=id, doc_type=doc_type)
        return res
    
    def searchDoc(self, index, body, doc_type='doc'):
        """
        执行搜索查询并获取与查询匹配的搜索匹配。可以跟复杂的查询条件。
        :param index: 要搜索的以逗号分隔的索引名称列表; 使用_all 或空字符串对所有索引执行操作。
        :param doc_type:要搜索的以逗号分隔的文档类型列表; 留空以对所有类型执行操作。
        :param body:使用Query DSL（QueryDomain Specific Language查询表达式）的搜索定义。
        return:
            _source：返回_source字段的true或false，或返回的字段列表，返回指定字段。
            _source_exclude： 要从返回的_source字段中排除的字段列表，返回的所有字段中，排除哪些字段。
            _source_include从_source：字段中提取和返回的字段列表，跟_source差不多。
        Examples:
            1) 一般查询: body={"query": {"match":{"age": 20}}}；
            2）结果字段过滤：body={"query": {"match":{"age": 19}}},_source=['name', 'age'])；
            3）返回结果排除字段：body={"query": {"match":{"age": 19}}},_source_exclude  =[ 'age'])；
            4）返回结果包含字段：body={"query": {"match":{"age": 19}}},_source_include =[ 'age'])；
        """
        res = self.es.search(index=index, body=body, doc_type=doc_type)
        return res
    
    def countDoc(self, index, doc_type=None, body=None):
        """
        执行查询并获取该查询的匹配数。
        :param index: 要搜索的以逗号分隔的索引名称列表; 使用_all 或空字符串对所有索引执行操作。
        :param doc_type:要搜索的以逗号分隔的文档类型列表; 留空以对所有类型执行操作。
        :param body:使用Query DSL（QueryDomain Specific Language查询表达式）的搜索定义, 留空对所有条件查询。examples:
            body = {
                "query": {
                    "match": {
                        "age": 18
                        }
                    }
                }
        return:
            匹配文档的数量
            Examples:
            1) {'count': 1, '_shards': {'total': 5, 'successful': 5, 'skipped': 0, 'failed': 0}}
            2) 取匹配数量 res['count']
        """
        res = self.es.count(index=index, doc_type=doc_type, body=body)
        return res
    
    def deleteIndices(self, index):
        """
        删除指定的文档。但不能删除索引，如果想要删除索引，还需要es.indices.delete来处理.
        :param index:索引
        :param id:文档id
        :return:
        """
        res = self.es.indices.delete(index=index)
        return res

    def deleteId(self, index, id):
        """
        删除指定的文档。
        :param index:索引
        :param id:文档id
        :return:
        """
        res = self.es.delete(index=index, id=id)
        return res
    
    def delete_by_query(self, index, doc_type, body):
        """
        删除与查询匹配的所有文档。
        :param index:要搜索的以逗号分隔的索引名称列表; 使用_all 或空字符串对所有索引执行操作。
        :param doc_type:要搜索的以逗号分隔的文档类型列表; 留空以对所有类型执行操作。
        :param body:使用Query DSL的搜索定义。
        :return:
        """
        res = self.es.delete_by_query(index=index, doc_type=doc_type, body=body)
        return res
    
    def indicesExists(self, index):
        """
        检查索引是否存在
        """
        res = self.es.indices.exists(index=index)
        return res
    
    def exists(self, index, id, doc_type):
        """
        查询elasticsearch中是否存在指定的文档，返回一个布尔值。
        :param index:要搜索的以逗号分隔的索引名称列表; 使用_all 或空字符串对所有索引执行操作;
        :param id:文档id;
        :param doc_type:要搜索的以逗号分隔的文档类型列表; 留空以对所有类型执行操作;
        :return: 返回一个布尔值
        """
        res = self.es.exists(index=index, id=id, doc_type=doc_type)
        return res
    
    def search(self, index, body):
        

    def jsonDataToES(self, index, jsonData):
        """
        把json格式List数据入库到ES
        :param jsonData: 格式[jsonData, jsonData, ...]
        :return:
        """
        if isinstance(jsonData, list):
            for item in jsonData:
                res = self.es.index(index=index, body=item)
                print(res)

    def jsonDataBulkToES(self, index, jsonData):
        """
        将json格式的列表批量入库到ES。
        :param jsonData:
        :return:
        """
        ACTIONS = []
        if isinstance(jsonData, list):
            for per in jsonData:
                action = {
                    "_index": index,
                    "_source": per
                }
            ACTIONS.append(action)
            #批量处理
        success, _ = bulk(self.es, ACTIONS, index=index, raise_on_error=True)
        print("Performed {} actions".format(success))

    def csvDataToES(self, index, csvFile):
        """
        将csv文件数据直接导入ES。要求csv第一行为字段名。
        :param index:
        :param csvFile:
        :return:
        """
        df = pd.read_csv(csvFile, encoding='utf-8')
        cols = df.columns
        doc = {}
        for i in range(len(df)):
            for col in cols:
                doc[col] = df.loc[i, col]
            res = self.es.index(index=index, body=doc)
            print(res)

    def createPipeline(self, id, body):
        """
        创建管道流
        :param index:
        :return:
        """

        res = self.es.ingest.put_pipeline(id=id, body=body)
        print(res)

    def getPipeline(self, id):
        """
        获取管道
        :param id:
        :return:
        """
        res = self.es.ingest.get_pipeline(id)
        return res
    
