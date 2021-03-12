from flask import Flask, jsonify
from flask import request
import threading,logging
import datetime
import time
import multiprocessing
import json
import redis
import os
import sys
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class StockerStageInfo(BaseType):
    def __init__(self, COMPANYCODE, SITE, MAPID, TYPE):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.__COMPANYCODE = COMPANYCODE
        self.__SITE = SITE
        self.__MAPID = MAPID
        self.__TYPE = TYPE
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sential = Sentinel([('10.55.8.62',26379)],socket_timeout=0.5)
            sential.discover_master('master1')
            master1 = sential.master_for('master1')
            keys = master1.keys(self.__COMPANYCODE + '-' + self.__SITE + '-' + self.__MAPID + '-' + self.__TYPE)
            self.writeLog(f'Reids Key: {keys}')
            if not keys:
                self.writeLog(f' {keys} is not exist in redis')
                return {'Result': 'NG','Reason':'Key in redis is not exist'}, 401,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
            data = master1.get(keys[0].decode('utf8')).decode('utf8')
            self.writeLog(f'Json:\n {data}')
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
            return json.loads(data),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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