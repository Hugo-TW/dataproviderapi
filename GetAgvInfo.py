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
class AgvInfo(BaseType):
    def __init__(self,indentity):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  "with position_mapping as (select t.item5 as Line, t.item6 as MM, t.item7 as SS, t.item8 as S, t.item9 as STK_POSITION,t.item10 as LINE_POSITION from DCSMOD3_DB.DCS_EQ_PORT_LIST t)select t.agv_id ,case when t.create_date + (t1.s/86400) < sysdate then null else t1.Line||' ('||t.localtion_to||')' end as location,case when t.create_date + (t1.s/86400) < sysdate then null else to_date(to_char(t.create_date,'yyyy/mm/dd hh24miss'),'yyyy/mm/dd hh24miss') end as start_time,case when t.create_date + (t1.s/86400) < sysdate then null else t.create_date + (t1.s/86400) end as end_time  from ( select t.event_code as description,t.agv_id,t.localtion_from,t.localtion_to, t.current_position, max(t.create_date) over (partition by t.line_id,t.agv_id) as max_create_date,t.create_date from mcs_dispatch_command_log t where t.event_code = 3 and t.create_date is not null and t.create_user <> 'AGVC' and t.current_position in (select t.STK_POSITION from position_mapping t ) )t, position_mapping t1   where t.max_create_date = t.create_date and t.localtion_to = t1.LINE_POSITION(+) and t.create_date + (t1.s/86400) > sysdate order by 1 asc  "
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()
            
            datajson=[]
            if(len(data) != 0):
                for da in data:
                    datadict={
                        "車號" : da[0],
                        "目的地" : da[1],
                        "出發時間" : da[2],
                        "預估到站時間" : da[3]
                    }
                    datajson.append(datadict)
            else:
                datadict={
                        "車號" : "",
                        "目的地" :"",
                        "出發時間" : "",
                        "預估到站時間" : ""
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