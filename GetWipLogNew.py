# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
from datetime import timedelta
class wipLogNewFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            #self.writeLog(f'Input Json:{self.jsonData}')
            # STARTIME to TimeStamp
            st = datetime.datetime.strptime(self.jsonData['STARTIME'],'%Y-%m-%d')
            STARTIME = int(time.mktime(st.timetuple()))
            # ENDTIME to TimeTtamp
            et = datetime.datetime.strptime(self.jsonData['ENDTIME'],'%Y-%m-%d')
            ENDTIME =  int(time.mktime(et.timetuple()))
            #self.writeLog(f'TimeStamp:{STARTIME} ~ {ENDTIME}')
            bottomLine = "_"
            redisKey = ""
            tmp = []
            tmp.append(f"{self.__class__.__name__}")
            tmpCodition = copy.deepcopy(self.jsonData["CONDITION"])
            tmpGroupby = copy.deepcopy(self.jsonData["GROUPBY"])
            tmpConceal = copy.deepcopy(self.jsonData["CONCEAL"])
            tmpCONDITION = {}
            for k,v in tmpCodition.items():
                if len(v) != 0:
                    if isinstance(v,(dict,list)):
                        for kk,vv in v.items():
                            tmp.append(f"{kk}-{vv}")
                    else:
                        tmp.append(v)
                else:
                    del self.jsonData["CONDITION"][k]
            for k,v in tmpConceal.items():
                tmp.append(f"{k}-{v}")
            GROUPBY = {}
            for c in tmpGroupby:
               GROUPBY[c] = f"${c}"
            GROUPBY["ACCT_DATE"] = "$UTC"
            tmp.extend(tmpGroupby)
            tmp.append(str(STARTIME))
            tmp.append(str(ENDTIME))
            redisKey = bottomLine.join(tmp)
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(self.jsonData["CONDITION"]["FACTORY_ID"] + '_WIP'))).timetuple()),"DataSource":"Redis"}
            CONCEAL = {}
            for k,v in self.jsonData["CONCEAL"].items():
                CONCEAL[k] = {"$ne":v}
            tmpCONDITION = self.jsonData["CONDITION"]
            tmpCONDITION.update(CONCEAL)
            tmpCONDITION["UTC"] = {
                                  "$gte" : STARTIME, "$lte" : ENDTIME  
                            }
            CONDITION = [
                            {
                                "$project":{
                                                "_id":0,
                                                "FAC_ID":0,
                                                "STAY_MONTH_GROUP":0,
                                                "LCM_OWNER":0
                                           }   
                            },
                            {
                                "$match":tmpCONDITION
                            },
                            {
                                "$group" : {
                                                #"_id" : {
                                                #            "COMPANY":"$COMPANY",
                                                #            "FACTORY":"$FACTORY",
                                                #            "SITE":"$SITE",      
                                                #            "WORK_CTR":"$WORK_CTR",
                                                #            "PROCESS":"$PROCESS",
                                                #            "UTC":"$UTC"
                                                #         },
                                                "_id" : GROUPBY,
                                                "QTY1" : {
                                                            "$sum" : "$QTY1"
                                                         },
                                                "QTY2" : {
                                                            "$sum" : "$QTY2"
                                                         },
                                                "QTY3" : {
                                                            "$sum" : "$QTY3"
                                                         },
                                                "QTY5" : {
                                                            "$sum" : "$QTY5"
                                                         },
                                                "QTY7" : {
                                                            "$sum" : "$QTY7"
                                                         },
                                                "QTY15" : {
                                                            "$sum" : "$QTY15"
                                                          },
                                                "QTY30" : {
                                                            "$sum" : "$QTY30"
                                                          },
                                                "QTY31" : {
                                                            "$sum" : "$QTY31"
                                                          },
                                                "QTY" : {
                                                            "$sum" : "$QTY"
                                                        },
                                                "QTY45" : {
                                                            "$sum" : "$QTY45"
                                                          },
                                        }
                            },
                            {
                                "$sort":{
                                            "_id.ACCT_DATE":1
                                        } 
                            },
                            
                            
                        ]
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("wipHisAndCurrent")
            data = []
           # self.writeLog(f'CONDITION:{CONDITION}')
            m_data = self.aggregate(CONDITION)          
            for m in m_data:
                if "_id" in m:
                    for g in GROUPBY:
                        if g == "ACCT_DATE":
                            m[g] = str(datetime.datetime.utcfromtimestamp(m['_id'][g]) + timedelta(hours=8)).split(' ')[0]
                        else:
                            m[g] = m['_id'][g]
                    del m['_id']
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