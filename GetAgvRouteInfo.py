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
class AgvRouteInfo(BaseType):
    def __init__(self,indentity):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql = "SELECT * FROM (SELECT line_id,fg_mtrl,from_identity,to_identity,command_order,agv_position,create_time,trx_sub_job_id,sc,agv_id,status_remark,status_desc,alarm_msg,position,material_info FROM DCS_AGV_MONITOR_RPT ORDER BY TRX_JOB_ID DESC, TRX_SUB_JOB_ID ASC, command_order asc) WHERE ROWNUM <= 48 or create_time > sysdate - 10/(24*60)"
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()
            
            datajson=[]
            if(len(data) != 0):
                for da in data:
                    datadict={
                        "線體" : da[0],
                        "機種" : da[1],
                        "FROM" : da[2],
                        "TO" : da[3],
                        "執行順序" : da[4],
                        "AGV#" : da[5],
                        "CREATE_TIME" : da[6],
                        "DISPATCH ID" : da[7],
                        "SC" : da[8],
                        "AGV ID" : da[9] if (da[9] != None) else "",
                        "搬運狀態" : da[10]if (da[10] != None) else "",
                        "AGVC 狀態": da[11] if (da[11] != None) else "",
                        "故障狀態" : da[12] if (da[12] != None) else "",
                        "POSITION" : da[13] if (da[13] != None) else "",
                        "載具明細" : da[14] if (da[14] != None) else ""
                    }
                    datajson.append(datadict)
            else:
                datadict={
                        "線體" : "",
                        "機種" :"",
                        "FROM" : "",
                        "TO" : "",
                        "執行順序" : "",
                        "AGV#" : "",
                        "CREATE_TIME" : "",
                        "DISPATCH ID" : "",
                        "SC" : "",
                        "AGV ID" : "",
                        "搬運狀態" : "",
                        "AGVC 狀態": "",
                        "故障狀態" : "",
                        "POSITION" : "",
                        "載具明細" : ""
                    }
                datajson.append(datadict)
            data = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder)
            
            self.writeLog(f"Json:\n {data}")
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
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)