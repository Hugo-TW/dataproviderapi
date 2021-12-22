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
class iSFPWOWIPInfo(BaseType):
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
                        where item_name = 'WO' 
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                        union
                        SELECT distinct '1' as A,to_char(t.data_date,'mm/dd') as data_date 
                        FROM WAYNE_TEST_TV t 
                        where item_name = 'WIP14' 
                        and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                        union
                        SELECT distinct '1' as A,to_char(t.data_date,'mm/dd') as data_date 
                        FROM WAYNE_TEST_TV t 
                        where item_name = 'WIP30' 
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
            ResultColnumjson.append("")
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
                          select t.data_date,t.data_type,
                          case when (t.data_date <> 'MTD' and t.data_value > t1.red_day) then 'red'
                               when (t.data_date <> 'MTD' and t.data_value >= t1.green_day and t.data_value <= t1.red_day) then 'yellow'
                               when (t.data_date <> 'MTD' and t.data_value < t1.green_day) then 'green' 
                               when (t.data_date = 'MTD' and t.data_value > t1.red_mtd) then 'red'
                               when (t.data_date = 'MTD' and t.data_value >= t1.green_mtd and t.data_value <= t1.red_mtd) then 'yellow'
                               when (t.data_date = 'MTD' and t.data_value < t1.green_mtd) then 'green' 
                               end||'/'||t.data_value as data_value
                               from
                          (
                            SELECT to_char(t.data_date,'mm/dd') as data_date,to_char(t.data_date,'mm') as month_date,t.data_type,to_char(t.data_value) as  data_value,t.item_name
                            FROM WAYNE_TEST_TV t 
                            where item_name in ('WO','WIP14','WIP30') 
                            and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                            
                            union
                            
                            select 'MTD' as date_time,substr('{1}',4,2) as month_dat,t.data_type,to_char(round(decode(t.data_type,'燈號',round(sum(t.data_value)/count(*),0),sum(t.data_value)/count(*)),1),'FM990.0') as data_value,t.item_name
                            from WAYNE_TEST_TV t
                            where item_name in ('WO','WIP14','WIP30') 
                            and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                            group by t.data_type,t.item_name
                          )t,
                          (
                            select to_char(t.data_date,'MM') as data_date,t.item_name,t.item_desc,t.red_day,t.green_day,t.red_mtd,t.green_mtd from isfp_target_upload t 
                            where t.data_date between to_date(substr('{0}',0,6),'yyyy/mm') and to_date(substr('{1}',0,6),'yyyy/mm') 
                            and t.item_name in ('WO','WIP14','WIP30')
                          )t1
                          where t.month_date = t1.data_date
                          and t.item_name = t1.item_name
                        )
                        PIVOT (max (data_value)FOR data_date IN ('{2}')) 
                        order by 1 desc""".format(self.__start_time, self.__end_time, self.__sColnumName) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_result = self.Select(sql)
            self.closeConnection()

            datajson=[]
            dataitem=[]
            dataTitle=[]
            iNum_data_list = 1

            if(len(data_result) != 0):

                for da in data_result:
                    iNum_data_list = 1
                    datadict = dict(zip(colnumjson, da))
                    datajson.append(datadict)

                    if(da[0] == 'WO'):  
                        dataitem = '工單結案 N+4結案率%'
                    elif(da[0] == 'WIP30'):
                        dataitem = '>30天的WIP(PCS)'   
                    elif(da[0] == 'WIP14'):
                        dataitem = '>14天的WIP(PCS)' 
                    else: 
                        dataitem = ''     
                         
                    dataTitle.append(dataitem)   
                    dataitem = []

                    #組元件所需Value格式
                    for da_D in data:
                        # if(da[0] == 'WO'):
                        #     if(da[iNum_data_list] is None):
                        #         dataColor = ''
                        #     elif(float(da[iNum_data_list]) < 80):
                        #         dataColor = 'red'
                        #     elif(float(da[iNum_data_list]) >= 80 and float(da[iNum_data_list]) <= 90):
                        #         dataColor = 'yellow' 
                        #     else:
                        #         dataColor = 'green'

                        # elif(da[0] == 'WIP30'):
                        #     if(da[iNum_data_list] is None):
                        #         dataColor = ''
                        #     elif(float(da[iNum_data_list]) > 90):
                        #         dataColor = 'red'
                        #     elif(float(da[iNum_data_list]) >= 20 and float(da[iNum_data_list]) <= 90):
                        #         dataColor = 'yellow' 
                        #     else:
                        #         dataColor = 'green'  

                        # elif(da[0] == 'WIP14'):
                        #     if(da[iNum_data_list] is None):
                        #         dataColor = ''
                        #     elif(float(da[iNum_data_list]) > 450):
                        #         dataColor = 'red'
                        #     elif(float(da[iNum_data_list]) >= 200 and float(da[iNum_data_list]) <= 450):
                        #         dataColor = 'yellow' 
                        #     else:
                        #         dataColor = 'green'                  

                        # Testdatadict={
                        #             "color" : dataColor,
                        #             "value" : da[iNum_data_list]
                        #         }      

                        if(da[iNum_data_list] is None):
                            Testdatadict={
                                    "color" : '',
                                    "value" : ''
                                }   
                        else:    
                            Color_Value = da[iNum_data_list].split("/", 1)
                            Testdatadict={
                                    "color" : Color_Value[0],
                                    "value" : float(Color_Value[1])
                                }   
                                  
                        iNum_data_list = iNum_data_list + 1   
                        dataList.append(Testdatadict) 
                    dataListArray.append(dataList)  
                    dataList = []    
                           
            else:
                datadict = dict(zip(colnumjson, ""))
                datajson.append(datadict) 

            data_result = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder)
            
            #取燈號 Size
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql =  """select t.data_type,t.data_value from wayne_test_size t"""
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_RGB_Size = self.Select(sql)
            self.closeConnection()

            for da in data_RGB_Size:
                if(da[0] == 'circleSize'):
                    ResultCircleSize = da[1]
                elif(da[0] == 'fontSize'):
                    ResultFontSize = da[1]

            #組元件所需資料格式          
            responseResult = {}
            #dataitem[0] = "達產(%)"
            #ResuleSide.append(dataitem)
            ResuleSide.append(dataTitle)
            responseResult = dict(circleSize = ResultCircleSize, fontSize = ResultFontSize, borderType = 2,titleArray = ResultColnumjson,sideArray = ResuleSide,listArray = dataListArray)
            

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