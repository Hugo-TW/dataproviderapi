# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
class wipLogFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Input Json:{self.jsonData}')
            # STARTIME to TimeStamp
            st = datetime.datetime.strptime(self.jsonData['STARTIME'],'%Y-%m-%d')
            STARTIME = int(time.mktime(st.timetuple()))
            # ENDTIME to TimeTtamp
            et = datetime.datetime.strptime(self.jsonData['ENDTIME'],'%Y-%m-%d')
            ENDTIME =  int(time.mktime(et.timetuple()))
            self.writeLog(f'TimeStamp:{STARTIME} ~ {ENDTIME}')
            bottomLine = "_"
            redisKey = ""
            tmp = []
            tmp.append(f"{self.__class__.__name__}")
            tmpCodition = copy.deepcopy(self.jsonData["CONDITION"])
            for k,v in tmpCodition.items():
                if len(v) != 0:
                    tmp.append(v)
                else:
                    del self.jsonData["CONDITION"][k]
            tmp.append(str(STARTIME))
            tmp.append(str(ENDTIME))
            redisKey = bottomLine.join(tmp)
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(self.jsonData["CONDITION"]["FACTORY_ID"] + '_WIP'))).timetuple()),"DataSource":"Redis"}
            tmpCONDITION = self.jsonData["CONDITION"]
            tmpCONDITION["UTC"] = {
                                  "$gte" : STARTIME, "$lte" : ENDTIME  
                            }
            CONDITION = [
                            {
                                "$match":tmpCONDITION
                            },
                            {
                                "$sort":{
                                            "FACTORY_ID":1,
                                            "FAC_ID":1,
                                            "UTC":1
                                        } 
                            },
                            {
                                "$project":{
                                                "_id": 0,
                                                "FAC_ID": 0,
                                                "LCM_OWNER": 0,
                                                "STAY_MONTH_GROUP": 0,
                                                "UTC":0
                                            }
                            }
                        ]
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("wipHisAndCurrent")
            data = []
            self.writeLog(f'CONDITION:{CONDITION}')
            m_data = self.aggregate(CONDITION)
            for m in m_data:
                if "_id" in m:
                    m["_id"] = str(m["_id"])
                data.append(m)   
            self.closeMongoConncetion()

            self.getRedisConnection()
            if self.searchRedisKeys(self.jsonData["CONDITION"]["FACTORY_ID"] + '_WIP'):
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), self.getKeyExpirTime(self.jsonData["CONDITION"]["FACTORY_ID"] + '_WIP'))
            else:
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), 60)
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