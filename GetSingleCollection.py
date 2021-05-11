# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
import hashlib
from BaseType import BaseType
from datetime import timedelta
class singleCollectionFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Current Collection:{self.jsonData["COLLECTION"]}')
            if "STARTIME" in self.jsonData and "ENDTIME" in self.jsonData:
                # STARTIME to TimeStamp
                st = datetime.datetime.strptime(self.jsonData['STARTIME'],'%Y-%m-%d')
                STARTIME = int(time.mktime(st.timetuple()))
                # ENDTIME to TimeTtamp
                et = datetime.datetime.strptime(self.jsonData['ENDTIME'],'%Y-%m-%d')
                ENDTIME =  int(time.mktime(et.timetuple()))
                for a in self.jsonData["AGGREGATE"]:
                    for k,v in a.items():
                        if "$match" == k:
                            v["UTC"] = {
                                         "$gte" : STARTIME, 
                                         "$lte" : ENDTIME 
                                       }
            FACTORY_ID = "J001"
            for AGGREGATE in  self.jsonData["AGGREGATE"]:
                for k,v in AGGREGATE.items():
                    if k == "$match":
                        if "FACTORY_ID" in v:
                            FACTORY_ID = v["FACTORY_ID"]
                #self.writeLog(f'TimeStamp:{STARTIME} ~ {ENDTIME}')              
            #AGGREATE = []
            #for s in self.jsonData["AGGREGATE"]:
            #    if self.jsonData.get(s,{}):
            #        AGGREATE.append({s:self.jsonData[s]})
            jsonStr = json.dumps(self.jsonData)
            m = hashlib.md5()
            m.update(jsonStr.encode("utf-8"))
            redisKey = m.hexdigest()
            self.getRedisConnection()
            seconds = 600 if self.collectionMapping(self.jsonData["COLLECTION"]) == "" else self.getKeyExpirTime(FACTORY_ID + self.collectionMapping(self.jsonData["COLLECTION"])) 
            #if self.searchRedisKeys(redisKey):
            #    self.writeLog(f"Cache Data From Redis")
            #    return json.loads(self.getRedisData(redisKey)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds =seconds )).timetuple()),"DataSource":"Redis"}
            self.getMongoConnection()
            self.setMongoDb(self.jsonData["DATABASE"])
            self.setMongoCollection(self.jsonData["COLLECTION"])              
            pData = self.aggregate(self.jsonData["AGGREGATE"])
            data = []
            for p in pData:
                odata = {}
                if '_id' in p:
                    if isinstance(p['_id'],(dict,list)):
                        odata = copy.deepcopy(p['_id'])
                    else:
                        p['_id'] = str(p['_id'])
                if 'ACCT_DATE' in odata:
                    try:
                         odata["ACCT_DATE"] = datetime.datetime.strptime(odata["ACCT_DATE"],'%Y%m%d').strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                elif 'ACCT_DATE' in p:
                    try:
                         p["ACCT_DATE"] = datetime.datetime.strptime(p["ACCT_DATE"],'%Y%m%d').strftime('%Y-%m-%d')
                    except ValueError:
                        pass 
                for k,v in p.items():
                    if not isinstance(v,(dict,list)):
                        odata[k] = v
                data.append(odata)
            self.getRedisConnection()
            if self.searchRedisKeys(FACTORY_ID + self.collectionMapping(self.jsonData["COLLECTION"])):
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), self.getKeyExpirTime(FACTORY_ID + self.collectionMapping(self.jsonData["COLLECTION"])))
            else:
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), 600)         
            return data, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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
    def collectionMapping(self, collection):
        switcher = {
            "wipHisAndCurrent":"_WIP",
            "passHisAndCurrent":"_PASS",
            "deftHisAndCurrent":"_DEFT",
        }
        return switcher.get(collection, "")
