from flask import Flask, jsonify
from flask import request
import threading,logging
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
from datetime import date, datetime
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class iSFPScrapChartInfo(BaseType):
    def __init__(self,indentity, start_time, end_time):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        self.__start_time = start_time
        self.__end_time = end_time
        
    def getData(self):
        try:
            #取需要堆疊的資料
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """select to_char(t.data_date,'mm/dd') as data_date,t.data_type,t.data_value as YVALUE,DENSE_RANK() over (order by t.data_date )-1 as XVALUE
                        from wayne_test_tv t 
                        where t.item_name='SCRAP'
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                        and t.data_type not in ('Total報廢率','目標')""".format(self.__start_time, self.__end_time) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()

            datajson=[]

            if(len(data) != 0):
                for da in data:
                    datadict={
                        "DATA_DATE" : da[0],
                        "DATA_TYPE" : da[1],
                        "YVALUE" : da[2],
                        "XVALUE" : da[3]
                    }
                    datajson.append(datadict)
            else:
                datadict={
                        "DATA_DATE" : "",
                        "DATA_TYPE" :"",
                        "YVALUE" : "",
                        "XVALUE" : ""
                    }
                datajson.append(datadict)

            data_result = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder) 

            #取Line的資料
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """select to_char(t.data_date,'mm/dd') as data_date,t.data_type,t.data_value as YVALUE,DENSE_RANK() over (order by t.data_date )-1 as XVALUE
                        from wayne_test_tv t 
                        where t.item_name='SCRAP'
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                        and t.data_type in ('Total報廢率','目標')""".format(self.__start_time, self.__end_time) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_line = self.Select(sql)
            self.closeConnection()

            dataLine1json=[]
            dataLine2json=[]

            if(len(data) != 0):
                for da in data_line:
                    if(da[1] == 'Total報廢率'):
                        datadict1={
                            "DATA_DATE" : da[0],
                            "DATA_TYPE" : da[1],
                            "YVALUE" : da[2],
                            "XVALUE" : da[3]
                        }
                        dataLine1json.append(datadict1)
                    elif(da[1] == '目標'):   
                        datadict2={
                            "DATA_DATE" : da[0],
                            "DATA_TYPE" : da[1],
                            "YVALUE" : da[2],
                            "XVALUE" : da[3]
                        }
                        dataLine2json.append(datadict2) 
            else:
                datadict={
                        "DATA_DATE" : "",
                        "DATA_TYPE" :"",
                        "YVALUE" : "",
                        "XVALUE" : ""
                    }
                dataLine1json.append(datadict)
                dataLine2json.append(datadict)

            #組元件所需資料格式          
            responseResult = {}
            responseResult = dict(DATASERIES = datajson, LINESERIES1 = dataLine1json, LINESERIES2 = dataLine2json)

            self.writeLog(f"Json:\n {data_result}")
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
            #return json.loads(data_result),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
            return responseResult,200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

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
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)