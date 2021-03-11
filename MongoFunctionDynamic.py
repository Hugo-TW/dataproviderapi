# -*- coding: utf-8 -*-
#!flask/bin/python
import json
import os
import sys
import traceback
import hashlib
import datetime
import re
import time
from BaseType import BaseType
from bson.objectid import ObjectId
from distutils.util import strtobool
from confluent_kafka import Producer
from datetime import timedelta

os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class mongoDbFunctionDynamic ( BaseType ):
    def __init__( self,jsonData ):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.jsonData = jsonData
    def getData( self ):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            res = {}
            kres = []
            #jsonStr = json.dumps(self.jsonData)
            #m = hashlib.md5()
            #m.update(jsonStr.encode("utf-8"))
            #redisKey = m.hexdigest()
            #self.getRedisConnection()
            #if self.searchRedisKeys(redisKey):
            #    self.writeLog(f"Cache Data From Redis")
            #    return json.loads(self.getRedisData(redisKey)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(redisKey))).timetuple()),"DataSource":"Redis"}    
            for keyName, keyValue in self.jsonData.items():
                if "SETTING" not in keyValue or "CRITERIA" not in keyValue:  
                    res[keyName] = {"Status":"NG","Reason":f" {keyName} miss SETTING or CRITERIA parameter","Data":[]}
                    continue
                self.writeLog(f'{keyName}  Start')

                IP = keyValue.get("SETTING").get("IP","10.55.8.62")
                PORT = int(keyValue.get("SETTING").get("PORT",27017))
                self.writeLog(f'IP:{IP},PORT:{PORT}')

                DB = keyValue.get("SETTING").get("DB","")
                if not DB:
                    res[keyName] = {"Status":"NG","Reason":f" {keyName} miss DB parameter or Value is Empty","Data":[]}
                    continue
                self.writeLog(f'Set DB:{DB}')
        
                COLLECTION = keyValue.get("SETTING").get("COLLECTION","")
                if not COLLECTION:
                    res[keyName] = {"Status":"NG","Reason":f" {keyName} miss COLLECTION parameter or Value is Empty","Data":[]}
                    continue
                self.writeLog(f'Set COLLECTION:{COLLECTION}')

                TYPE = keyValue.get("SETTING").get("TYPE","")
                if not TYPE:
                    res[keyName] = {"Status":"NG","Reason":f" {keyName} miss TYPE parameter","Data":[]}
                    continue
                self.writeLog(f'TYPE:{TYPE}')

                CRITERIA = keyValue.get("CRITERIA",{})
                if not CRITERIA:
                   res[keyName] = {"Status":"NG","Reason":f" {keyName} miss CRITERIA parameter","Data":[]}
                   continue 
                
                self.getMongoConnection(ip = IP, port = PORT)
                self.setMongoDb(DB)
                self.setMongoCollection(COLLECTION)
                if TYPE == "CREATE":
                    res[keyName] = self.__create(COLLECTION, CRITERIA)
                elif TYPE == "SELECT":
                    res[keyName] = self.__select(COLLECTION, CRITERIA)
                elif TYPE == "UPDATE":
                    res[keyName] = self.__update(COLLECTION, CRITERIA)
                elif TYPE == "DELETE":
                    res[keyName] = self.__delete(COLLECTION, CRITERIA)
                else:
                    res[keyName] = {"Status":"NG","Reason":f" {keyName}'s TYPE is not correct","Data":[]}
                    continue
                if self.controlCollectionFlag(COLLECTION) and self.controlTypeFlag(TYPE):
                    kres.append(COLLECTION)
                self.writeLog(res)
                self.closeMongoConncetion()
            if len(kres) != 0:
               self.sendKafka("scm-iamp-config-event-v0", kres)
            #self.getRedisConnection()
            #self.setRedisData(redisKey, json.dumps(res, sort_keys=True, indent=2), 600)    
            return res, 200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":600}
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
            return {'Result': 'NG','Reason':f'{funcName} erro'},400 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Data":[]}
    def __delete( self,collectionName, reqParm ):
        try:
            res = {}
            CONDITION = reqParm.get('CONDITION',{})
            if not CONDITION:
                return {"Status":"NG","Reason":f"CONDITION in CRITERIA is Empty","Data":[]}

            self.writeLog(f"CONDITION:{CONDITION}")
            returnCode = self.deleteToMongo(CONDITION)
            if bool(returnCode):
                res["Status"] = 'OK'
                res['Reason'] = ''
                res['Data'] = []
            else:
                res["Status"] = 'NG'
                res['Reason'] = f'Data is not exist in {collectionName} Collection'
                res['Data'] = []
            return res
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return {'Result': 'NG','Reason':f"Erro Line:{lineNum},Type:{error_class},Detail:{detail}","Data":[]}
    def __update( self,collectionName, reqParm ):
        try:
            res = {}          
            DATA = reqParm.get('DATA',{})
            CONDITION = reqParm.get('CONDITION',{})
            ISDOT = reqParm.get('ISDOT',"N")

            if not DATA:
                return {"Status":"NG","Reason":f" Data in CRITERIA is Empty","Data":[]}

            if not CONDITION:
                return {"Status":"NG","Reason":f"CONDITION in CRITERIA is Empty","Data":[]}
        

            #for cKey, cValue in CONDITION.items():
            #    DATA[cKey] = cValue
            
            DATA.update(CONDITION)
        
            if ISDOT == "Y":
                iData = {}
                for k,v in DATA.items():
                    iData[k.replace('.','')] = v
                DATA = iData

            self.writeLog(f"CONDITION:{CONDITION}")
            self.writeLog(f"ISDOT:{ISDOT}")
            self.writeLog(f"DATA:{DATA}")

            count = self.getMongoFind(CONDITION).count()    
            if count != 0:     
                reqParm['ISDOT'] = 'N'
                if '_id' in DATA:
                    DATA.pop('_id')
                iData = self.__select(collectionName,reqParm)['Data'][0]
                #for k,v in DATA.items():
                #    if not v and k in iData:
                #        iData.pop(k)
                #    elif v:
                #        iData.update({k:v})
                uData = self.updateRecord(DATA,iData, self.controlCollectionFlag(collectionName))
                self.writeLog(f"iData:{uData}")
                if self.updateToMongo(CONDITION, uData):
                    res["Status"] = 'OK'
                    res['Reason'] = ''
                    res["Data"] = []
            else:
                self.writeLog("No Data To Update，Insert One Data")   
                returnCode = self.inserOneToMongo(DATA)
                res["Status"] = 'OK'
                res['Reason'] = ''
                res["Data"] = []
            return res
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return {'Result': 'NG','Reason':f"Erro Line:{lineNum},Type:{error_class},Detail:{detail}","Data":[]}
    def __select( self,collectionName, reqParm ):
        try:   
            rtData = []
            rtCode = {}
            CONDITION = reqParm.get('CONDITION',{})
            INNERCONDITION = reqParm.get('INNERCONDITION',{})
            ISDOT = reqParm.get('ISDOT',"N")
            PROJECTION = {}
            tmpPROJECTION = reqParm.get('PROJECTION',{})
            if tmpPROJECTION:
                for k,v in tmpPROJECTION.items():
                    PROJECTION[k] = strtobool(v)
            if "_id" not in PROJECTION:
                PROJECTION["_id"] = False

            self.writeLog(f"CONDITION:{CONDITION}")
            self.writeLog(f"INNERCONDITION:{INNERCONDITION}")
            self.writeLog(f"PROJECTION:{PROJECTION}")
            self.writeLog(f"ISDOT:{ISDOT}")

            res = self.getMongoFind(CONDITION, projectionFields = PROJECTION)
            if res.count() != 0:
                for r in res:
                    if PROJECTION["_id"] and isinstance(r['_id'],ObjectId):
                        r['_id'] = str(r['_id'])
                    if ISDOT == "Y":
                        iData = {}
                        for k,v in r.items():
                            if self.patternAdnFind(k):
                                iData[k] = v
                            else:
                                iData["." + k] = v
                        rtData.append(iData)  
                    else:
                        if INNERCONDITION:
                            innerData = []
                            for rd in r["DATA"]:
                                resultValue = [False for c in list(INNERCONDITION.keys()) if rd.get(c) != INNERCONDITION.get(c)]
                                if resultValue:
                                    continue
                                innerData.append(rd)
                            r["DATA"] = innerData
                        rtData.append(r)
                rtCode = {"Status":"OK","Reason":"","Data":rtData}
            else:
                rtCode = {"Status":"NG","Reason":f"Data is not Exist {collectionName}Collection","Data":rtData}
            return rtCode
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return {'Result': 'NG','Reason':f"Erro Line:{lineNum},Type:{error_class},Detail:{detail}","Data":[]}
    def __create( self,collectionName,reqParm ):
        try:     
            DATA = reqParm.get('DATA',{})
            CONDITION = reqParm.get('CONDITION',{})
            ISDOT = reqParm.get('ISDOT',"N")
            res = {}
            if not DATA:
                return {"Status":"NG","Reason":f" Data in CRITERIA is Empty","Data":[]}

            if CONDITION:               
                #for cKey, cValue in CONDITION.items():
                #    DATA[cKey] = cValue
                DATA.update(CONDITION)       
       
            if ISDOT == "Y":
                iData = {}
                for k,v in DATA.items():
                    iData[k.replace('.','')] = v
                DATA = iData 

            self.writeLog(f"CONDITION:{CONDITION}")
            self.writeLog(f"ISDOT:{ISDOT}")
            self.writeLog(f"DATA:{DATA}")

            mCode = self.getMongoFind(CONDITION).count()  
            #if bool(mCode):
            #    res['Status'] = "NG"
            #    res['Reason'] = f"Data is exist in {collectionName}
            #    Collection"
            #    res['Data'] = []
            #else:
            returnCode = self.inserOneToMongo(DATA)
            if returnCode :
               res["Status"] = 'OK'
               res['Reason'] = ''
               res['Data'] = []
            return res
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return {'Result': 'NG','Reason':f"Erro Line:{lineNum},Type:{error_class},Detail:{detail}","Data":[]}
    def __connection( self ):
        try:
            IP = "10.55.8.62" if len(self.__SETTING.get('IP',"10.55.8.62")) < 1 else self.__SETTING.get('IP',"10.55.8.62")
            PORT = 27017 if len(self.__SETTING.get('PORT','27017')) < 1 else int(self.__SETTING.get('PORT','27017'))
            self.getMongoConnection(ip = IP, port = PORT)
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return {'Result': 'NG','Reason':f"Erro Line:{lineNum},Type:{error_class},Detail:{detail}","Data":[]}
    def sendKafka( self, topic, mgs ):
        p = Producer({'bootstrap.servers': 'idts-kafka1.cminl.oa'})
        p.produce(topic, json.dumps(mgs).encode('utf-8') , callback = self.delivery_report)
        p.poll(1) 
        p.flush()
    def delivery_report( self,err, msg ):
     if err is not None:
        self.writeError('KAFKA delivery failed: {}'.format(err))
     else:
        self.writeLog('KAFKA delivered to {} [{}]'.format(msg.topic(), msg.partition()))      
    def controlCollectionFlag( self, collection ):
        switcher = {
            "pageConfig":True,
            "cssConfig":True,
            "componentManifest":True,
            "sampleData":True,
            "sysAuth":True,
        }
        return switcher.get(collection,False)
    def controlTypeFlag( self,type ):
        switcher = {
            "CREATE":True,
            "UPDATE":True,
            "DELETE":True
        }
        return switcher.get(type,False)
    def updateRecord( self,data, idata, flag ):     
        if flag:
            return data
        else:
            for k,v in data.items():
                if not v and k in idata:
                    idata.pop(k)
                elif v:
                    idata.update({k:v})
            return idata
    def patternAdnFind( self,k ):
        pattern = [":", "#", "*", "_", "@"]
        for p in pattern:
            if k.find(p,  0 , 1) != -1:
                return True
        return False