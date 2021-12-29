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
class iSFPLightInfo(BaseType):
    def __init__(self,indentity, start_time, end_time, line_type, item_name):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        self.__start_time = start_time
        self.__end_time = end_time
        self.__line_type = line_type
        self.__item_name = item_name
        
    def getData(self):
        try:
            #取欄位名稱
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """SELECT distinct t.data_type 
                        FROM isfp_data_upload t 
                        where item_name = '{3}' 
                        and t.line_type = '{2}'
                        and t.data_date between '{0}' and '{1}' 
                        order by 1""".format(self.__start_time, self.__end_time, self.__line_type, self.__item_name) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()
            colnumjson=[]
            colnumjson.append("DATE")
            sColnumName = ""
            iCount = 0
            if(len(data) != 0):
                for da in data:

                    sDataName = da[0]
                    colnumjson.append(sDataName)
                    if(iCount == len(data)-1):
                        sColnumName = sColnumName + sDataName
                    else:   
                        sColnumName = sColnumName + sDataName + "','" 
                        iCount = iCount + 1

            #將欄位名稱 SQL Pivot
            self.__sColnumName = sColnumName
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """select * from 
                        (
                        SELECT to_char(to_date(t.data_date,'yyyy/mm/dd'),'mm/dd') as data_date,t.data_type,t.rgb_type as  data_value
                        FROM isfp_data_upload t 
                        where item_name = '{4}' 
                        and t.line_type = '{3}'
                        and t.data_date between '{0}' and '{1}'
                        and t.date_type = 'D'

                        union
                        
                        select 'MTD' as date_time,t.data_type,t.rgb_type as data_value
                        from isfp_data_upload t
                        where item_name = '{4}' 
                        and t.line_type = '{3}'
                        and t.data_date = 'MTD'
                        and t.date_type = 'MTD'
                        )
                        PIVOT (max (data_value)FOR data_type IN ('{2}')) 
                        order by decode(data_date,'MTD',1,2),data_date""".format(self.__start_time, self.__end_time, self.__sColnumName, self.__line_type, self.__item_name) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_result = self.Select(sql)
            self.closeConnection()

            datajson=[]
            dataResult=[]
            iNum_data = 0
            iNum_data_result = 0
            if(len(data_result) != 0):

                for da in data_result:
                    
                    if(da[1] == "GREEN"):
                        dataResult.append(da[0])
                        dataResult.append('LimeGreen')
                    elif(da[1] == "YELLOW"): 
                        dataResult.append(da[0])
                        dataResult.append('YELLOW')
                    else:
                        dataResult.append(da[0])
                        dataResult.append('RED') 

                    datadict = dict(zip(colnumjson, dataResult))
                    datajson.append(datadict)
                    dataResult=[]          
                           
            else:
                datadict = dict(zip(colnumjson, ""))
                datajson.append(datadict) 

            data_result = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder)
            
            self.writeLog(f"Json:\n {data_result}")
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
            return json.loads(data_result),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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