# -*- coding: utf-8 -*-
from flask_sse import sse
import json
import sys
import traceback
import requests
import copy
from BaseType import BaseType
WebApiUrl = "http://tiamp.cminl.oa/provider/MongoFunctionDynamic"
headers = {'Content-type':'application/json','Connection':'close'}#宣告json的type
class ssePublicFunc(BaseType):
    def __init__(self,jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.jsonData = jsonData
    def getData(self):
        try:               
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            for key, value in self.jsonData.items():
                value["SETTING"]["TYPE"] = "SELECT"
                KEYNAME = value['DATA']['KEYNAME']
                EVENTTYPE = value['DATA']['EVENTTYPE']
                CHANNEL = f"channel_{KEYNAME}"

                
                CONDITION = {"KEYNAME":KEYNAME}
                
                self.writeLog(f"CONDITION:{CONDITION}")
                mData = self.organizeMognoApiData(key,value["SETTING"],CONDITION)
                self.writeLog(f"SendMongoApi:{mData}")
                response = self.callMongoApi(WebApiUrl,mData, headers)
                self.writeLog(f"KEY:{key},API:{WebApiUrl},STATUS:{response.status_code},RESPONSE:{response.json()}")
                if response.status_code == 200 and response.json().get(key).get('Status') == 'OK':
                    for rk,rv in response.json().items():
                       sse.publish(rv["Data"],type = rv["Data"][0]["EVENTTYPE"],channel = rv["Data"][0]["CHANNEL"])
                else:
                     sse.publish({"MESSAGE":''},type = EVENTTYPE,channel = CHANNEL)
                return response.json(),response.status_code,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-type'}
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
    def organizeMognoApiData(self, key, setting, condition):
        return {
                    f"{key}": {
                        "SETTING": setting,
                        "CRITERIA": {
                            "CONDITION":condition ,
                        "PROJECTION": {
                            "_id": "False"
                        },
                        "ISDOT": "N",
                        "DATA": {}
                        }           
                    }         
            }
    def callMongoApi(self, url, data, headers):
        response = requests.post(url, data = json.dumps(data), headers = headers)
        response.close()
        return response