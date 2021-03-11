# -*- coding: utf-8 -*-
import json
import requests
from BaseType import BaseType
from confluent_kafka import Producer
import sys
import traceback
import time
from bson.objectid import ObjectId
WebApiUrl = "http://tiamp.cminl.oa/provider/MongoFunctionDynamic"
headers = {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}#宣告json的type
class streamComponentFunction(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.jsonData = jsonData
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            rtCode = {}       
            for key, value in self.jsonData.items():
                TYPE = value.get('SETTING').get('TYPE')
                DATA = value.get('CRITERIA').get('DATA')
                CONDITION = value.get('CRITERIA').get('CONDITION',{})
                value['CRITERIA']['DATA']['TIME'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                DATA['CHANNEL'] = f"channel_{CONDITION.get('KEYNAME','')}" if CONDITION.get('KEYNAME','') else ''
                DATA['TYPE'] = TYPE
                DATA.update(CONDITION)
                response = self.callMongoApi(WebApiUrl, {key:value}, headers)
                self.writeLog(f"KEY:{key},API:{WebApiUrl},STATUS:{response.status_code},RESPONSE:{response.json()}") 
                rtCode[key] = response.json().get(key)
                self.sendKafka("TEST", self.reorganizeData(key, value.get('SETTING'), DATA,self.controlFlag(TYPE)), self.controlFlag(TYPE), response.json().get(key).get('Status'))                  
            self.writeLog(rtCode)
            return rtCode,200,headers             
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
    def delivery_report(self,err, msg):
     if err is not None:
        self.writeError('KAFKA delivery failed: {}'.format(err))
     else:
        self.writeLog('KAFKA delivered to {} [{}]'.format(
                 msg.topic(), msg.partition()))
    def sendKafka(self, topic, data, flag, status):
         if flag and status == 'OK':
            p = Producer({'bootstrap.servers': 'idts-kafka1.cminl.oa'})
            p.produce(topic, json.dumps(data).encode('utf-8') , callback = self.delivery_report)
            p.poll(1) 
            p.flush()
    def callMongoApi(self, url, data, headers):
        response = requests.post(url, data = json.dumps(data), headers = headers)
        response.close()
        return response
    def controlFlag(self, type):
        switcher = {
            "SELECT": False,
            "DELETE": True,
            "UPDATE": True,
            "CREATE": True,
        }
        return switcher.get(type,False)
    def reorganizeData(self, key,setting, data, flag):
        DATA = {}
        dictData = {}
        if flag:
            dictData["SETTING"] = setting
            dictData["DATA"] = data
        DATA[key] = dictData
        self.writeLog(f"reorganizeData:{DATA}")
        return DATA