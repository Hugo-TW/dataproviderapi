# -*- coding: utf-8 -*-
#!flask/bin/python
from abc import ABCMeta, abstractmethod
from enum import Enum
from Logger import Logger
import os
import ibm_db
import ibm_db_dbi
import sys
from Dao import DaoHelper,ReadConfig
import json
import traceback
from redis.sentinel import Sentinel
import pymongo.errors as mongoerr
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import objectid
import datetime
from confluent_kafka import Producer,Consumer, KafkaError,KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
class BaseType ( metaclass = ABCMeta ):
    def __init__( self ):
        """初始化Logger"""
        self.__log = Logger('./log/' + self.__class__.__name__ + '.log',level='debug')
    @abstractmethod
    def getData( self ):
        """必須實作"""
        pass
    
    def Time_SEQ( self, c ):
        switcher = {
            "H":"hour",
            "D":"day",
            "W":"week",
            "M":"month",
            "Q":"quarter"
        }
        return switcher.get(c,"D")
    def TIME_CD( self, c ):
        switcher = {
            "H":"hour",
            "D":"hour",
            "W":"day",
            "M":"week",
            "Q":"month"
        }
        return switcher.get(c,"D")
    @staticmethod
    def SpaceDim( SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID ):
        if len(EQP_ID.strip()) > 0:
            return "eqp"
        elif len(SUPPLY_CATEGORY.strip()) > 0:
            return "line"
        elif len(FACTORY_ID.strip()) > 0:
            return "factory"
        else:
            return "site"
    @staticmethod
    def TimeDim( DATATYPE ):
        switcher = {
            "HourlyByPeriod":"hour",
            "DailyByPeriod":"day",
            "WeeklyByPeriod":"week",
            "MonthlyByPeriod":"month",
            "QuarterlyByPeriod":"quarter",
            "DailyByCurrent":"D",
            "WeeklyByCurrent":"W",
            "MonthlyByCurrent":"M",
            "QuarterlyByCurrent":"Q",
        }
        return switcher.get(DATATYPE,"HourlyByPeriod")
    @staticmethod
    def TimeDimForNew( DATATYPE ):
        switcher = {
            "HourlyByPeriod":"hour",
            "DailyByPeriod":"day",
            "WeeklyByPeriod":"week",
            "MonthlyByPeriod":"month",
            "QuarterlyByPeriod":"quarter",
            "DailyByCurrent":"hour",
            "WeeklyByCurrent":"day",
            "MonthlyByCurrent":"month",
            "QuarterlyByCurrent":"quarter",
        }
        return switcher.get(DATATYPE,"HourlyByPeriod")
    @staticmethod
    def validateType( data ):
        jsonData = None
        if type(data) is dict:
            jsonData = data
        elif type(data) is list:
            jsonData = data[0]
        return jsonData
    def writeLog( self,text ):
        """Info Log"""
        self.__log.logger.info(text)
    def writeError( self, text ):
        """Error Log"""
        self.__log.logger.error(text)
    def writeWarning( self, text ):
        self.__log.logger.warning(text)
    def writeDebug( self, text ):
        self.__log.logger.debug(text)
    def writeCritical( self, text ):
        self.__log.logger.critical(text)
    """Oracle DB https://www.oracle.com/tw/database/technologies/oracle19c-windows-downloads.html"""
    def getConnection( self, identity ):
        """連接資料庫
        identity : 資料庫名稱
        """
        try:
            self.__getDbAccount(identity)
            self.__daoHelper = DaoHelper(self.__dbAccount, self.__dbPassword,self.__SERVICE_NAME)
            self.__connect = self.__daoHelper.Connect()
            #return self.__dbAccount, self.__dbPassword, self.__SERVICE_NAME
        except Exception as e:
            self.closeConnection()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
        
    def __getDbAccount( self, identity ):
        """ 取得資料庫帳密
            identity : 資料庫名稱
            回傳 : dbAccount、dbPassword、SERVICE_NAME
        """
        try:
            self.__dbAccount, self.__dbPassword, self.__SERVICE_NAME = ReadConfig('config.json',identity).READ()
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))  
    def closeConnection( self ):
        """關閉資料庫連線"""
        self.__daoHelper.Close()

    def SelectAndDescription( self, sql ):
        """ 取的 row data 跟 row column description
            sql : sql語法
            回傳 : row column description、row data
        """
        try:
            self.__description, self.__data = self.__daoHelper.SelectAndDescription(sql)
            return self.__description, self.__data
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def Select( self, sql ):
        """ 取得 row data
            sql : sql語法
            回傳 : row data
        """
        try:
            self.__data = self.__daoHelper.Select(sql)
            return self.__data
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None

    def zipDescriptionAndData( self, description, data ):
        """ 取得 description和data壓縮後資料
            description :row column description 
            data : row data 
            回傳 [{key:value}]
        """
        try:
            col_names = [row[0] for row in description]
            datajson = [dict(zip(col_names,da)) for da in data]
            data = json.dumps(datajson, sort_keys=True, indent=2)
            return data
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None

    def getColumnName( self ):
        """ 取得 row column description 的 column name
            回傳 : [col_names]
        """
        try:
            col_names = [row[0] for row in self.__description]
            return col_names
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None

    """Redis"""
    def getRedisConnection( self, ip = '10.55.8.62', port = 26379, timeout = 0.5, masterName = 'master1' ):
        """ 連接Reids
            ip : 預設 10.55.8.62
            port : 預設 26379
            timeout : 預設 0.5 s
            masterName : 預設 master1
        """
        try:
            self.__sential = Sentinel([ (ip, port) ], socket_timeout = timeout)
            self.__sential.discover_master(masterName)
            self.__master1 = self.__sential.master_for(masterName)
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
           

    def searchRedisKeys( self, key ):
        """ Key是否存在
            key : 鍵值
            回傳 : 存在(True)、不存在(False)
        """
        try:
            keys = self.__master1.keys(key)
            if not keys :
                return False
            else:
                return True
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return False
    
    def getRedisData( self, key, format = 'utf8' ):
        """ 取得Redis相對應key的資料
            key : 鍵值
            format : 預設 utf8
            回傳 : 沒有資料回傳 None
        """
        try:
            data = None
            if self.searchRedisKeys(key):
                data = self.__master1.get(key).decode(format)
            return data
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def setRedisData( self, key, value, time = None ):
        """ 寫入redis資料
            key : 鍵值
            value: 值
            time: 時間(s)，預設None
        """
        try:
            self.__master1.set(key, value, ex = time)
            return True
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return False
    def setExpire( self, key, time ):
        """ 設定資料過期時間
            key:鑑值
            time: 時間(s)
        """
        try:    
            return  self.__master1.expire(key,time)
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return False
    def setExpireat( self, key, time ):
        """ 設定資料過期時間
            key:鑑值
            time: 時間(datetime.datetime(2020,7,31,15,13,10))
        """
        try:  
            return self.__master1.expireat(key,time)
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return False
    def getKeyExpirTime( self, key ):
        """ 設定資料過期時間
            key:鑑值
            time: 時間(datetime.datetime(2020,7,31,15,13,10))
        """
        try:  
            return self.__master1.ttl(key)
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    """MogoDB"""
    def getMongoConnection( self, ip = "10.55.8.62", port = 27017 ):  #"10.55.8.62"
        """ 取得Mogo連線("mongodb://ip:port/")
            ip : 預設 10.55.8.62
            port : 預設 27017
        """
        try:
            self.__client = MongoClient(f'mongodb://{ip}:{port}/', connect =True)        
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    
    def setMongoDb( self,db = "APPCONF" ):
        """ 設定 Mongo中的DB    
            db : 預設 APPCONF
        """
        try:
             self.__db = self.__client[db]
        except mongoerr.ConnectionFailure as  e:
            #self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))

    def getMongoDbName( self ):
        """取得Mongo中所有Db Name
        """
        try:
            return self.__client.list_database_names()
        except mongoerr.ConnectionFailure as  e:
            #self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))

    def setMongoCollection( self, collection = "sysmain" ):
        """ 設定Mongo中Collection
            collection : 預設 sysmain
        """
        try:
            self.__collection = self.__db[collection]
        except mongoerr.ConnectionFailure as  e:
            #self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))

    def getMongoCollectionName( self ):
        """ 取得Mongo中Collection Name
        """
        try:
            return  self.__db.list_collection_names()
        except mongoerr.ConnectionFailure as  e:
            #self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    def getMongoFindOne( self,reqParm, projectionFields = {'_id':False} ):
        """ 取得全部筆數
            reqParm : 搜尋條件，為dict，預設為空
            projectionFields : 屏蔽條件，為dict 預設 {'_id':False}
        """
        try:
            if objectid.ObjectId.is_valid(reqParm.get('_id','')):
               reqParm['_id'] = objectid.ObjectId(reqParm['_id'])
            return self.__collection.find_one(reqParm, projection = projectionFields)
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def getMongoFind( self, reqParm, projectionFields = {'_id':False} ):
        """ 取得全部筆數
            reqParm : 搜尋條件，為dict，預設為空
            projectionFields : 屏蔽條件，為dict 預設 {'_id':False}
        """
        try:
            if objectid.ObjectId.is_valid(reqParm.get('_id','')):
               reqParm['_id'] = objectid.ObjectId(reqParm['_id'])
            return self.__collection.find(reqParm, projection = projectionFields)
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def inserOneToMongo( self,data ):
        """ 寫入一筆data
            data: 資料
        """
        try:
            insertRecord = self.__collection.insert_one(data)
            return insertRecord.inserted_id
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None 
    def insertManyToMongo( self,data ):
        """ 寫入多筆data
            data: 資料 [{},{}]
        """
        try:
            return self.__collection.insert_many(data)
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None 
    def deleteToMongo( self,reqParm ):
        """ 刪除資料
            reqParm: 刪除條件
        """
        try:
            if objectid.ObjectId.is_valid(reqParm.get('_id','')):
               reqParm['_id'] = objectid.ObjectId(reqParm['_id'])
            return self.__collection.delete_one(reqParm)._DeleteResult__raw_result["n"]
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def aggregate(self,operation):
        """ 聚合操作
            operation:  [{
                            project	選擇集合中要的欄位，並可進行修改。
                            match	篩選操作，可以減少不需要的資料。
                            group	可以欄位進行分組。
                            unwind	拆開，可以將陣列欄位拆開成多個document。
                            sort	可針對欄位進行排序 。
                            limit	可針對回傳結果進行數量限制。
                            skip	略過前n筆資料，在開始回傳 。
                        }]
        """
        try:
            #if objectid.ObjectId.is_valid(condition.get('_id','')):
            #   condition['_id'] = objectid.ObjectId(condition['_id'])
            return self.__collection.aggregate(operation)
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    def closeMongoConncetion( self ):
        """ 關閉連線
        """
        self.__client.close()
    def updateToMongo( self,reqParm, data ):
        """
            更新資料
            reqParm:條件
            data:資料
        """
        try:
            if objectid.ObjectId.is_valid(reqParm.get('_id','')):
               reqParm['_id'] = objectid.ObjectId(reqParm['_id'])
            return self.__collection.update(reqParm,data)["updatedExisting"]
        except mongoerr.ConnectionFailure as  e:
            self.__client.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return None
    """KAFKA"""
    def createProducer( self,config = {'bootstrap.servers': 'idts-kafka1.cminl.oa'} ):
        """
            建立Producer 連線
            config: dict，預設 {'bootstrap.servers': 'idts-kafka1.cminl.oa'}
        """
        try:
            self.__producer = Producer(config)
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    def producerSend( self, topic, data, partition = -1, format = "utf-8" ):
        """
            送KAFKA
            topic: 通道
            data: 值
            partition:分區，預設 -1
            format:格式，預設UTF-8
        """
        try:
            self.__producer.poll(0)
            self.__producer.produce(topic, json.dumps(data).encode(format), partition =partition ,callback = self.delivery_report)
            self.__producer.flush()
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    def delivery_report( self,err, msg ):
        if err is not None:
            self.writeLog(f'Message delivery failed: {err}')
        else:
            self.writeLog(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    def createAdmin( self, config = {'bootstrap.servers': "idts-kafka1.cminl.oa"} ):
        """
            建立KafkaAdmin
            config: dict，預設 {'bootstrap.servers': "idts-kafka1.cminl.oa"}
        """
        try:
            self.__adminCline = AdminClient(config)
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
    def createTopicPartitions( self,topic,count ):
        """
            新增TOPIC分區
            topic:KAFKA TOPIC
            count:分區數量
        """
        try:
            new_parts = [ NewPartitions(topic,count) ]
            fs = self.__adminCline.create_partitions(new_parts, validate_only = False)
            for t, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    self.writeLog(f"Additional partitions created for topic {t}")
                except Exception as e:
                    self.writeError(f"Failed to add partitions to topic {t}: {e}")
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
    def getTopicDetail( self,topic ):
        """
            取得TOPIC細部資訊
            topic: KAFAK通道名稱
        """
        try:
            md = a.list_topics(timeout=10)
            self.writeLog(f"Cluster {md.cluster_id} metadata (response from broker {md.orig_broker_name})")
            self.writeLog(f" {len(md.brokers)} brokers:")
            for b in iter(md.brokers.values()):
                if b.id == md.controller_id:
                    self.writeLog(f"  {b}  (controller)")
                else:
                    self.writeLog(f"  {b}")

            self.writeLog(f" {len(md.topics)} topics")
            detail = {}
            for t in iter(md.topics.values()):
                if t.topic == topic:
                    if t.error is not None:
                        errstr = f": {t.error}"
                    else:
                        errstr = ""
                    detail["TopicErro"] = errstr
                    detail["Partition"] = len(t.partitions)         
                    self.writeLog(f"  \"{t}\" with {len(t.partitions)} partition(s){errstr}")

                    for p in iter(t.partitions.values()):
                        if p.error is not None:
                            errstr = f": {p.error}"
                        else:
                            errstr = ""
                        detail[p.id] = {
                                        "Leader":p.leader,
                                        "Replicas":p.replicas,
                                        "Isrs":p.isrs,
                                        "PartitionErro":errstr
                                        }
                        self.writeLog(f"partition {p.id} leader: {p.leader}, replicas: {p.replicas}, isrs: {p.isrs} errstr: {errstr}")
                    return detail
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
    def getAllTopics(self):
        """
            return Topics, Partitions, Err
        """
        try:
            md = self.__adminCline.list_topics(timeout=10)
            topics = []
            partitions = []
            err = []
            for t in iter(md.topics.values()):
                if t.error is not None:
                    errstr = ": {}".format(t.error)
                else:
                    errstr = ""
                topics.append(str(t))
                partitions.append(len(t.partitions))
                err.append(errstr)
                #print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))
            return topics, partitions, err
            #for p in iter(t.partitions.values()):
            #    if p.error is not None:
            #        errstr = ": {}".format(p.error)
            #    else:
            #        errstr = ""

            #    print("partition {} leader: {}, replicas: {},"
            #          " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
            #                                        p.isrs, errstr))     
        except KafkaException as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
    """DB2"""
    def getDb2Connection(self, Database, Hostname, Port, Username, Password):
        """
            Database: 資料庫
            Hostname: Ip
            Port: Port
            Username: 帳號
            Password: 密碼
        """
        try:
            self.db2conn = ibm_db.connect(f"database={Database};hostname={Hostname};port={Port};Protocol=TCPIP;uid={Username};pwd={Password};", Username, Password)
            return self.db2conn
        except Exception as e:
            self.db2conn.close()          
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))           
            return False
    #def db2Select(self, sql):
    #    """
    #        sql: SQL
    #    """
    #    try:
    #        result = ibm_db.exec_immediate(self.db2conn, sql)     
    #        rows = ibm_db.fetch_tuple(result)
    #        row = []
    #        while rows != False:
    #            row.append(rows)
    #            rows = ibm_db.fetch_tuple(result)
    #        return row
    #    except Exception as e:
            
    #        error_class = e.__class__.__name__ #取得錯誤類型
    #        detail = e.args[0] #取得詳細內容
    #        cl, exc, tb = sys.exc_info() #取得Call Stack
    #        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    #        fileName = lastCallStack[0] #取得發生的檔案名稱
    #        lineNum = lastCallStack[1] #取得發生的行號
    #        funcName = lastCallStack[2] #取得發生的函數名稱
    #        self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    #        self.db2conn.close()
    #        return None
    def db2Select(self, sql):
        """
            sql: SQL
        """
        try:
            result = ibm_db.exec_immediate(self.db2conn, sql)     
            rows = ibm_db.fetch_assoc(result)  
            row = []
            while rows != False:
                d = {}
                for k,v in rows.items():
                    if str(k).isdigit():
                        continue
                    if type(v) == datetime.datetime or type(v) == datetime.date:
                        v = str(v)      
                    d[k] = v
                row.append(d)
                rows = ibm_db.fetch_assoc(result)
            return row
        except Exception as e:
            
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            self.db2conn.close()
            return None
    #def db2Insert(self, sql, params):
    #    """
    #        sql:"insert into mytable values(?,?)"
    #        params :((1,'Sanders'),(2,'Pernal'),(3,'OBrien'))    
    #    """
    #    try:
    #        stmt_insert = ibm_db.prepare(self.db2conn, sql)
    #        return ibm_db.execute_many(stmt_insert,params)
    #    except Exception as e:
    #        self.db2conn.close()
    #        error_class = e.__class__.__name__ #取得錯誤類型
    #        detail = e.args[0] #取得詳細內容
    #        cl, exc, tb = sys.exc_info() #取得Call Stack
    #        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    #        fileName = lastCallStack[0] #取得發生的檔案名稱
    #        lineNum = lastCallStack[1] #取得發生的行號
    #        funcName = lastCallStack[2] #取得發生的函數名稱
    #        self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    #        return None
    #def db2Update(self, sql):
    #    """
    #        sql: SQL
    #    """
    #    try:
    #        return self.db2cursor.execute(sql)
    #    except Exception as e:
    #        self.db2conn.close()
    #        error_class = e.__class__.__name__ #取得錯誤類型
    #        detail = e.args[0] #取得詳細內容
    #        cl, exc, tb = sys.exc_info() #取得Call Stack
    #        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    #        fileName = lastCallStack[0] #取得發生的檔案名稱
    #        lineNum = lastCallStack[1] #取得發生的行號
    #        funcName = lastCallStack[2] #取得發生的函數名稱
    #        self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
    #        return None
    def db2CloseConnection(self):   
        return ibm_db.close(self.db2conn)