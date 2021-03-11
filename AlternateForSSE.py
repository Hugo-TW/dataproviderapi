# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
from datetime import timedelta
import requests
class alternateFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        self.baseConfig = {   
            "WebApiUrl":"http://tiamp.cminl.oa/provider/sseForCommon",#
            "headers":{'Content-type':'application/json','Connection':'close'},
        }
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            #self.writeLog(f'Input Json:{self.jsonData}')
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("alternativeMapping")
            req = {
                    f"{self.jsonData['TYPE']}":{
                                                    '$exists': True 
                                                }
                  }
            row = self.getMongoFindOne(req)              
            if row:
                for r in row:      
                    jsonData = {
                                        "EVENTTYPE":"IAMP",
                                        "CHANNEL":f"{self.jsonData['FACTORY_ID']}_ETL_EVENT",
                                        "DATA":row[r]
                               }
                    response = self.callMongoApi(self.baseConfig["WebApiUrl"], jsonData, self.baseConfig["headers"])
                    self.writeLog(f"TYPE:{r}, STATUS:{response.status_code}, RESPONSE:{response.json()}")
                return {'Result':'OK', 'Reason': ''}, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
            else:
                return {'Result':'NG', 'Reason': 'Do not find mapping in Mongo'}, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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
    def callMongoApi( self ,url, data, headers ):
        #self.writeLog('%s %s() ' %
        #(self.__class__.__name__,sys._getframe().f_code.co_name))
        response = requests.post(url, data = json.dumps(data), headers = headers)
        response.close()
        return response
