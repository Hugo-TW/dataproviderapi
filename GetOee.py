from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
import traceback
import datetime
import time
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class Oee(BaseType):
     def __init__(self, COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, indentity):
        super().__init__()     
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__COMPANY_CODE = COMPANY_CODE
        self.__SITE = SITE
        self.__FACTORY_ID = FACTORY_ID
        self.__SUPPLY_CATEGORY = SUPPLY_CATEGORY
        self.__EQP_ID = EQP_ID
        self.__SPACE_DIM = SPACE_DIM
        self.__TIME_DIM = TIME_DIM
        self.__DATA_SEQ = DATA_SEQ
        self.__indentity = indentity
     def getData(self):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql = "WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' line_id,'{4}' eqp_id FROM DUAL) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id AS SUPPLY_CATEGORY,i.eqp_id,hi.{6}_cd AS TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim hi LEFT JOIN DMB_DC_OEE_{5}_{6} s  ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id= NVL (i.factory_id, 'NA') AND s.line_id = NVL (i.line_id, 'NA') AND s.eqp_id = NVL (i.eqp_id, 'NA') AND hi.{6}_cd = s.{6}_cd WHERE hi.{6}_seq <= '{7}' ORDER BY hi.{6}_cd ASC".format(self.__COMPANY_CODE, self.__SITE, self.__FACTORY_ID, self.__SUPPLY_CATEGORY, self.__EQP_ID, self.__SPACE_DIM.upper(), self.__TIME_DIM, self.__DATA_SEQ)
            self.writeLog(f'SQL:\n {sql}')

            key = f"{self.__class__.__name__}_{self.__COMPANY_CODE}_{self.__SITE}_{self.__FACTORY_ID}_{self.__SUPPLY_CATEGORY}_{self.__EQP_ID}_{self.__SPACE_DIM.upper()}_{self.__TIME_DIM}_{self.__DATA_SEQ}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}

            self.getConnection(self.__indentity)
            description,data = self.SelectAndDescription(sql)
            self.closeConnection()
            col_names = self.getColumnName()         
            dictRUN = {}
            dictIDLE = {}
            dictDOWN = {}
            count = 0
            for da in data:
                w = dict(zip(col_names,da))
                if w['STATUS_RUN'] is not None and w['STATUS_IDLE'] is not None and w['STATUS_DOWN'] is not None:
                    count += 1                   
                    dictRUN['STATUS_RUN'] = dictRUN.get('STATUS_RUN', 0) + int(w['STATUS_RUN'])
                    dictIDLE['STATUS_IDLE'] = dictIDLE.get('STATUS_IDLE', 0) + int(w['STATUS_IDLE'])
                    dictDOWN['STATUS_DOWN'] = dictDOWN.get('STATUS_DOWN', 0) + int(w['STATUS_DOWN'])
           
            datajson=[{
                 "COMPANY_CODE":self.__COMPANY_CODE,
                 "SITE":self.__SITE,
                 "FACTORY_ID":self.__FACTORY_ID,
                 "SUPPLY_CATEGORY":self.__SUPPLY_CATEGORY,
                 "EQP_ID":self.__EQP_ID,
                 "STATUS_RUN": round(dictRUN['STATUS_RUN'] / count , 2) if count !=0 else 0,
                 "STATUS_DOWN":round(dictDOWN['STATUS_DOWN'] / count , 2) if count !=0 else 0,
                 "STATUS_IDLE":round(dictIDLE['STATUS_IDLE'] / count , 2) if count !=0 else 0}]
           
            data = json.dumps(datajson, sort_keys=True, indent=2)

            self.getRedisConnection()
            self.setRedisData(key, data, 600)
            self.writeLog(f"Json:\n{data}")
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
           
            return json.loads(data),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(minutes = 10)).timetuple()),"DataSource":"Oracle"} 
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
