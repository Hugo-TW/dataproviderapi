# -*- coding: utf-8 -*-
#!flask/bin/python
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
import re
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
from bson.objectid import ObjectId
import pprint
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class mongoDbFunction(BaseType):
     def __init__(self, DBNAME, COLLECTION, KEYNAME, USERID):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.__DBNAME = DBNAME
        self.__COLLECTION = COLLECTION
        self.__KEYNAME = KEYNAME
        self.__USERID = USERID
     def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.getMongoConnection()
            self.setMongoDb(self.__DBNAME)
            #
            rtCode = {}
            for key, value in self.__COLLECTION.items():       
                self.setMongoCollection(key)
                res = []
                r = {}
                DATA = {}
                TYPE = value["TYPE"]
                iDATA = value["DATA"]
                if iDATA:
                   for k,v in iDATA.items():
                        DATA[k.replace('.','')] = v
                ID = self.__KEYNAME
                DATA["_id"] = ID
                DATA["KEYNAME"] = self.__KEYNAME
                DATA["USERID"] = self.__USERID               
                self.writeLog(f'COLLECTION:{key}，TYPE:{TYPE}，KEYNAME:{self.__KEYNAME}，USERID:{self.__USERID}')
                if(TYPE == "CREATE"):             
                    mCode = self.getMongoFind({"_id":ID}).count()
                    if bool(mCode):
                        r['Status'] = "NG"
                        r['Reason'] = f"Key is exist in {key} Collection"
                        res.append(r)
                    else:               
                        returnCode = self.inserOneToMongo(DATA)
                        if returnCode == ID:
                            r["Status"] = 'OK'
                            r['Reason'] = ''
                            res.append(r)
                elif(TYPE == "SELECT"):
                        rarry = []
                        res = []
                        if not self.__KEYNAME and not self.__USERID:   
                            projection = {'_id':True, "KEYNAME":True, "USERID":True}                        
                            rt = self.getMongoFind({},projectionFields = projection)
                            if rt.count() != 0:                          
                                for rw in rt:
                                    if key == "cssConfig":
                                        rq = {}
                                        for k,v in rw.items():
                                            rq["." + k] = v
                                        res.append(rq) 
                                    else:
                                        res.append(rw)
                            else:
                                r['Status'] = "NG"
                                r['Reason'] = f"Data is not exist in {key} Collection"
                                res.append(r)  
                        elif not self.__KEYNAME:
                            projection = {'_id':True, "KEYNAME":True, "USERID":True}
                            rt = self.getMongoFind({"USERID":self.__USERID},projectionFields = projection )
                            if rt.count() != 0:                          
                                for rw in rt:
                                    if key == "cssConfig":
                                        rq = {}
                                        for k,v in rw.items():
                                            rq["." + k] = v
                                        res.append(rq) 
                                    else:
                                        res.append(rw)
                            else:
                                r['Status'] = "NG"
                                r['Reason'] = f"Data is not exist in {key} Collection"
                                res.append(r)  
                        elif not self.__USERID:
                            projection = {'_id':False, "KEYNAME":False, "USERID":False}
                            rt = self.getMongoFind({"KEYNAME":self.__KEYNAME},projectionFields = projection)
                            if rt.count() != 0:                          
                                for rw in rt:
                                    if key == "cssConfig":
                                        rq = {}
                                        for k,v in rw.items():
                                            rq["." + k] = v
                                        res.append(rq) 
                                    else:
                                        res.append(rw)
                            else:
                                r['Status'] = "NG"
                                r['Reason'] = f"Data is not exist in {key} Collection"
                                res.append(r)    
                        else:
                            rt = self.getMongoFind({'_id':ID},projectionFields = {'_id':False, "KEYNAME":False, "USERID":False})
                            if rt.count() != 0:                          
                                for rw in rt:
                                    if key == "cssConfig":
                                        rq = {}
                                        for k,v in rw.items():
                                            rq["." + k] = v
                                        res.append(rq) 
                                    else:
                                        res.append(rw)
                            else:
                                r['Status'] = "NG"
                                r['Reason'] = f"Data is not exist in {key} Collection"
                                res.append(r)  
                elif(TYPE == "UPDATE"):      
                    mCode = self.updateToMongo({'_id':ID}, DATA)
                    if mCode:
                        r["Status"] = 'OK'
                        r['Reason'] = ''
                        res.append(r)
                    else:
                        self.writeLog("No Data To Update，Insert One Data")   
                        returnCode = self.inserOneToMongo(DATA)
                        if returnCode == ID:
                            r["Status"] = 'OK'
                            r['Reason'] = ''
                            res.append(r)
                elif(TYPE == "DELETE"):
                    returnCode = self.deleteToMongo({'_id':ID})
                    if bool(returnCode):
                        r["Status"] = 'OK'
                        r['Reason'] = ''
                        res.append(r)
                    else:
                        r["Status"] = 'NG'
                        r['Reason'] = f'NG: Data is not exist in {key} Collection'
                        res.append(r)
                else:
                    r["Status"] = 'NG'
                    r['Reason'] = f"TYPE: {TYPE} is not exist"
                    res.append(r)
                self.writeLog(res)
                rtCode[key] = res
            self.closeMongoConncetion()
            self.writeLog(rtCode)
            return rtCode ,200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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