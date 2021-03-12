# -*- coding: utf-8 -*-
import json
import requests
from BaseType import BaseType
import sys
import traceback
import time
from bson.objectid import ObjectId
WebApiUrl = "http://tiamp.cminl.oa/provider/StreamComponent"
headers = {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}#宣告json的type
class dataForComponentFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.jsonData = jsonData
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            data = self.reorganizeData()
            self.writeLog(f'Data:{data}')
            response = self.callMongoApi(WebApiUrl, data, headers)
            self.writeLog(f"API:{WebApiUrl},STATUS:{response.status_code},RESPONSE:{response.json()}")
            return response.json(), response.status_code, headers
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
    def callMongoApi(self, url, data, headers):
        response = requests.post(url, data = json.dumps(data), headers = headers)
        response.close()
        return response
    def reorganizeData(self):
        return {
                "QTY1" : {
                    "SETTING" : {
                        "IP" : "10.55.8.62",
                        "PORT" : "27017",
                        "DB" : "IAMP",
                        "COLLECTION" : self.jsonData["COLLECTION"],
                        "TYPE" : self.jsonData["TYPE"]
                     },
                    "CRITERIA" : {
                        "CONDITION" : self.jsonData["CONDITION"],
                        "PROJECTION" :self.jsonData["PROJECTION"],
                        "ISDOT" : self.jsonData["ISDOT"],
                        "DATA" : self.jsonData["DATA"]                       
                     } 
                }
              }