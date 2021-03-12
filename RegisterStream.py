# -*- coding: utf-8 -*-
#!flask/bin/python
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
import re
import copy
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
from bson.objectid import ObjectId
import pprint
from distutils.util import strtobool
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class RegisterStreamFuncion(BaseType):
    def __init__(self,jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.__jsonData = jsonData
    def getData(self):
        try:
            dbName = "IAMP"
            collectionName = "StreamSubscription"
            DATA = {}
            rtCode = {}
            reqParm = {"IDENTITY":self.__jsonData["IDENTITY"]}
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            self.getMongoConnection()
            self.setMongoDb(dbName)
            self.setMongoCollection(collectionName)
            self.writeLog(f'MongoDB:{dbName}，Collection:{collectionName}')
            self.writeLog(f'IDENTITY:{self.__jsonData["IDENTITY"]}，TYPE:{self.__jsonData["TYPE"]}，EVENTTYPE:{self.__jsonData["EVENTTYPE"]}，CHANNEL:{self.__jsonData["CHANNEL"]}')
            iData = self.getMongoFind(reqParm)
            if iData.count() !=0 :
                for r in iData:
                    DATA = r
                if self.__jsonData["TYPE"].upper() == "UNSUBSCRIBE":                   
                    self.deleteToMongo(reqParm)
                    rtCode["Result"] = "OK"
                    rtCode["Reason"] = 'UNSUBSCRIBE OK'
                elif self.__jsonData["TYPE"].upper() == DATA["TYPE"]:
                    rtCode["Result"] = "NG"
                    rtCode["Reason"] = 'SUBSCRIBE Already'
                else:
                    rtCode["Result"] = "NG"
                    rtCode["Reason"] = 'TYPE is not exist'
            else:
                if self.__jsonData["TYPE"].upper() == "SUBSCRIBE":
                    self.inserOneToMongo(self.__jsonData)
                    rtCode["Result"] = "OK"
                    rtCode["Reason"] = 'SUBSCRIBE OK'
                else:
                    rtCode["Result"] = "NG"
                    rtCode["Reason"] = 'TYPE is not exist or Type is incorrect'
            self.writeLog(rtCode)
            self.closeMongoConncetion()
            return  rtCode, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
            return {'Result': 'NG','Reason':f'{funcName} erro'},400 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
            