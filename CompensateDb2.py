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
import configparser
class compensate(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        self.data = []
    def getData( self ):
        try:
            config = configparser.ConfigParser()
            config.read('setting.ini')
            target = self.jsonData["FACTORY_ID"]
            self.baseConfig = {
                "server":{'bootstrap.servers': 'idts-kafka1.cminl.oa'},
                #"topic":"scm-local-operation-raw-wip-v0",
                "target":target,
                "database":config.get('DB', target),
                "ip":config.get('IP', target),
                "port":config.get('PORT', target),
                "account":config.get('ACCOUNT', target),
                "password":config.get('PASSWORD', target),
                "schema":config.get('SCHEMA', target),
                "WebApiUrl":"http://idts-kafka1.cminl.oa:5001/alternative",#
                "headers":{'Content-type':'application/json','Connection':'close'},
            }
            self.baseConfig["topic"] = self.mappingTopic(self.jsonData["TYPE"])
            db = self.getDb2Connection(self.baseConfig["database"],self.baseConfig["ip"], self.baseConfig["port"], self.baseConfig["account"], self.baseConfig["password"])
            self.data = self.returnDb2Funciton(self.jsonData["TYPE"])(self.jsonData["TIME"])
            self.db2CloseConnection()
            self.createProducer()
            self.producerSend(self.baseConfig["topic"], self.data)     
            return {'Result':'OK', 'Reason': ''}, 200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        except Exception as e:
                error_class = e.__class__.__name__ #取得錯誤類型
                detail = e.args[0] #取得詳細內容
                cl, exc, tb = sys.exc_info() #取得Call Stack
                lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
                fileName = lastCallStack[0] #取得發生的檔案名稱
                lineNum = lastCallStack[1] #取得發生的行號
                funcName = lastCallStack[2] #取得發生的函數名稱
                self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
                return {'Result':'NG', 'Reason': 'Exception'}, 200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
    def getWIPSUMHIS(self, cuttime):
        sql = f"select FAC_ID, WORK_CTR, LCM_OWNER, PROD_NBR, ACCT_DATE, QTY1,QTY3, QTY5, QTY7, QTY15, QTY30, QTY31, QTY, QTY45, STAY_MONTH_GROUP, QTY2 from {self.baseConfig['schema']}.V_WIPSUMHIS where ACCT_DATE = '{cuttime}' with ur"
        print(f"sql:{sql}")
        return self.db2Select(sql)
    def getDEFTSUMHIS(self, cuttime):
        sql = f"select FAC_ID, ACCT_DATE, PROD_NBR, MAIN_WC, LCM_OWNER, RW_COUNT, DFCT_CODE, QTY from {self.baseConfig['schema']}.V_DEFTSUMHIS where ACCT_DATE = '{cuttime}' with ur"
        return self.db2Select(sql)
    def getPASSSUMHIS(self, cuttime):
        sql = f"select FAC_ID, ACCT_DATE, PROD_NBR, MAIN_WC, TRANS_TYPE, LCM_OWNER, RW_COUNT, QTY, LCM_GRADE from {self.baseConfig['schema']}.V_PASSSUMHIS where ACCT_DATE = '{cuttime}' with ur"
        return self.db2Select(sql)
    def getTOBESCRAPSUMHIS(self, cuttime):
        sql = f"select CHAR(FAC_ID), ACCT_DATE, PROD_NBR, MAIN_WC, LCM_OWNER, RESP_OPER, RESP_OWNER, SCRAP_CODE, TOBESCRAP_QTY from {self.baseConfig['schema']}.V_TOBESCRAPSUMHIS where ACCT_DATE = '{cuttime}' with ur"
        return self.db2Select(sql)
    def addJobTime(self, data, jobTime):
        for s in data:
            s["JobFinishTime"] = jobTime
    def mappingTopic(self, TYPE):
        switcher = {
            "WIP":"scm-local-operation-raw-wip-v0",
            "PASS":"scm-local-operation-raw-pass-v0",
            "DEFT":"scm-local-operation-raw-deft-v0",
            "SCRP":"scm-local-operation-raw-scrp-v0",
        }
        return switcher.get(TYPE,"scm-local-operation-raw-wip-v0")
    def returnDb2Funciton(self,TYPE):
        switcher = {
            "WIP":self.getWIPSUMHIS,
            "PASS":self.getPASSSUMHIS,
            "DEFT":self.getDEFTSUMHIS,
            "SCRP":self.getTOBESCRAPSUMHIS,
        }
        return switcher.get(TYPE,self.getWIPSUMHIS)