# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
from datetime import timedelta
class mongoInsertManyFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.getMongoConnection()
            self.setMongoDb(self.jsonData["DATABASE"])
            self.setMongoCollection(self.jsonData["COLLECTION"])  
            self.insertManyToMongo(self.jsonData["DATA"]) 
            self.closeMongoConncetion()
            return {'Result':'OK', 'Reason': ''}, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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
