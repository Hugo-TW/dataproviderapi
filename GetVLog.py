# -*- coding: utf-8 -*-
import json
import kafka_replayer
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
class vLogFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            #self.writeLog(f'Input Json:{self.jsonData}')
            # STARTIME to TimeStamp
            st = datetime.datetime.strptime(self.jsonData['STARTIME'],'%Y-%m-%d %H:%M:%S')
            STARTIME = int(time.mktime(st.timetuple()))
            # ENDTIME to TimeTtamp
            et = datetime.datetime.strptime(self.jsonData['ENDTIME'],'%Y-%m-%d %H:%M:%S')
            ENDTIME =  int(time.mktime(et.timetuple()))
            #self.writeLog(f'TimeStamp:{STARTIME} ~ {ENDTIME}')
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("VlogWeekCount")
            CONDITION =[
                            {"$match":{
                                         "utc" :{"$gte" : STARTIME, "$lte" : ENDTIME},
                                         "ad":self.jsonData["ad"]
                                      }
                            },
                        ]
            data = {}
            m = self.aggregate(CONDITION)
            if self.jsonData["TYPE"] == "EACH":              
                for d in m:
                    if "_id" in d: 
                        d["_id"] = str(d["_id"])
                        self.writeLog(f"Data:\n{json.dumps(d, indent=2, ensure_ascii = False)}")
                    data[str(datetime.datetime.fromtimestamp(d["utc"])).split(' ')[0]] = d
            elif self.jsonData["TYPE"] == "SUM":
                if not data:
                    data["accessHistPerPage"] = []
                    data["accessHistPerFactory"] = []                           
                for d in m:
                    for accessHistPerPage in d["accessHistPerPage"]:
                        c1 = copy.deepcopy(accessHistPerPage)
                        c1.pop("pageUUID")
                        if not data["accessHistPerPage"]:
                            data["accessHistPerPage"].append(c1)
                        else:
                            accessHistPerPageFlag = True 
                            for s in data["accessHistPerPage"]:
                                if c1["pageId"] == s["pageId"] and c1["pageDisplay"] == s["pageDisplay"] and c1["factoryId"] == s["factoryId"]:
                                    s["accessTime"] += c1["accessTime"]
                                    accessHistPerPageFlag = False
                            if accessHistPerPageFlag:                               
                                data["accessHistPerPage"].append(c1)
                    for accessHistPerFactory in d["accessHistPerFactory"]:
                        c1 = copy.deepcopy(accessHistPerFactory)
                        c1.pop("pageUUID")
                        if not data["accessHistPerFactory"]:
                            data["accessHistPerFactory"].append(c1)
                        else:
                            accessHistPerFactoryFlag = True   
                            for s in data["accessHistPerFactory"]:
                                if c1["pageId"] == s["pageId"] and c1["component"] == s["component"] and c1["pageDisplay"] == s["pageDisplay"] and c1["factoryId"] == s["factoryId"] and c1["apiRoute"] == s["apiRoute"]:
                                    s["accessTime"] += c1["accessTime"]
                                    accessHistPerFactoryFlag = False
                            if accessHistPerFactoryFlag:
                                data["accessHistPerFactory"].append(c1)          
            self.closeMongoConncetion()
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