#-*- coding: utf-8 -*-

import pandas as pd
import xlrd
import logging
import json
import os
import base64

def getDataFromXLSX(xlsx):
    """
    从excel读取所有sheet, 并返回df格式的sheet
    :param xlsx:
    :return:
    """
    excel = xlrd.open_workbook(xlsx)
    sheets = excel.sheet_names()
    tables = []
    for sheet in sheets:
        table = pd.read_excel(xlsx, sheet_name=sheet, header=0)
        tables.append(table)
    return tables

def getLogger(logFile):
    if not os.path.isfile(logFile):
        f = open(logFile, 'w+')
        f.close()
    logger = logging.getLogger(logFile)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logFile)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

def loadConfig(configFile):
    """
    Load configuration of the model
    parameters are stored in json format
    """
    with open(configFile, encoding="utf8") as f:
        return json.load(f)

def saveConfig(config, configFile):
    """
    Save configuration of the model
    parameters are stored in json format
    """
    with open(configFile, "w", encoding="utf8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def file2Buff(file):
    """
    以byte格式读取文本文件，并以base64进行编码
    :param file:
    :return:
    """
    f = open(file, 'rb')
    string = f.read()
    buff = base64.b64encode(string)
    f.close()
    return buff

def scanAllFiles(path):
    allFiles = []
    if not os.path.isdir(path):
        print("{} is invalid path.".format(path))
        return allFiles
    for root, dirs, files in os.walk(path):
        for file in files:
            allFiles.append(os.path.join(root, file))
    return  allFiles





# if __name__ == '__main__':
#     pass
    # getDataFromXLSX("data/diseaseBase.xlsx")
    # files = scanAllFiles("data")
    # print(files)