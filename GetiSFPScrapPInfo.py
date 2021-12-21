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
class iSFPScrapPInfo(BaseType):
    def __init__(self,indentity, start_time, end_time):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        self.__start_time = start_time
        self.__end_time = end_time
        
    def getData(self):
        try:
            #取欄位名稱
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """SELECT distinct '1' as A,to_char(t.data_date,'mm/dd') as data_date 
                        FROM WAYNE_TEST_TV t 
                        where item_name = 'SCRAP_P' 
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                        union
                        select '0' as A,'MTD' from dual""".format(self.__start_time, self.__end_time) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()
            colnumjson=[]
            ResultColnumjson=[]
            ResuleSide=[]
            dataList=[]
            dataListArray=[]
            colnumjson.append("DATA_DATE")
            sColnumName = ""
            iCount = 0
            if(len(data) != 0):
                for da in data:

                    sDataName = da[1]
                    colnumjson.append(sDataName)
                    ResultColnumjson.append(sDataName)
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
                        SELECT to_char(t.data_date,'mm/dd') as data_date,t.data_type,to_char(t.data_value) as  data_value
                        FROM WAYNE_TEST_TV t 
                        where item_name = 'SCRAP_P'
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                        
                        union
                        
                        select 'MTD' as date_time,t.data_type,to_char(round(decode(t.data_type,'燈號',round(sum(t.data_value)/count(*),0),sum(t.data_value)/count(*)),1)) as data_value
                        from WAYNE_TEST_TV t
                        where item_name = 'SCRAP_P'
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                        group by t.data_type
                        )
                        PIVOT (SUM (data_value)FOR data_date IN ('{2}')) 
                        order by 1 desc""".format(self.__start_time, self.__end_time, self.__sColnumName) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_result = self.Select(sql)
            self.closeConnection()

            datajson=[]
            dataitem=[0]
            dataTitle=[]
            iNum_data_list = 1

            if(len(data_result) != 0):

                for da in data_result:
                    iNum_data_list = 1
                    datadict = dict(zip(colnumjson, da))
                    datajson.append(datadict)  

                    if(da[0] == 'SCRAP_P'):  
                        dataitem = '破片報廢率(%)'
                    else: 
                        dataitem = ''     
                         
                    dataTitle.append(dataitem)   
                    dataitem = []
                 
                    #組元件所需Value格式
                    for da_D in data:
                        if(da[iNum_data_list] is None):
                            dataColor = ''
                        elif(float(da[iNum_data_list]) > 0.2):
                            dataColor = 'red'
                        elif(float(da[iNum_data_list]) >= 0.13 and float(da[iNum_data_list]) <= 0.2):
                            dataColor = 'yellow' 
                        else:
                            dataColor = 'green'

                        Testdatadict={
                                    "color" : dataColor,
                                    "value" : da[iNum_data_list]
                                }        
                        iNum_data_list = iNum_data_list + 1   
                        dataList.append(Testdatadict) 
                    dataListArray.append(dataList)  
                    dataList = []    
                           
            else:
                datadict = dict(zip(colnumjson, ""))
                datajson.append(datadict) 

            data_result = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder)
            
            #組元件所需資料格式          
            responseResult = {}
            #dataitem[0] = "達產(%)"
            #ResuleSide.append(dataitem)
            ResuleSide.append(dataTitle)
            responseResult = dict(borderType = 2,titleArray = ResultColnumjson,sideArray = ResuleSide,listArray = dataListArray)
            

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