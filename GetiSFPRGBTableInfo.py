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
class iSFPRGBTableInfo(BaseType):
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

            Item_Value = self.__item_name.split("/", 1)
            iItemCount = 0
            sql = ''

            
            for da in Item_Value:
                sql = sql + """ SELECT distinct '1' as A,to_char(t.data_date,'mm/dd') as data_date 
                                FROM WAYNE_TEST_TV t 
                                where item_name = '{2}' 
                                and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss') 
                                and t.line_type = '{3}'
                                union""".format(self.__start_time, self.__end_time, Item_Value[iItemCount], self.__line_type) 
                    
                iItemCount = iItemCount + 1            
                
            sql = sql + """ select '0' as A,'MTD' from dual"""

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

            if(self.__item_name == 'WO/WIP'):
                sDataType = Item_Value[0]
                sItemName = '''WO','WIP14','WIP30'''
            elif(self.__item_name == 'RESIGN/ATTENDANCE'):    
                sDataType = '出勤率'
                sItemName = '''RESIGN','ATTENDANCE'''
            elif(self.__item_name == 'REACH'):    
                sDataType = ''
                sItemName = '''REACH'''
            elif(self.__item_name == 'TD/INPUT'):    
                sDataType = ''
                sItemName = '''TD','INPUT''' 
            elif(self.__item_name == 'OEE'):    
                sDataType = ''
                sItemName = '''OEE''' 
            elif(self.__item_name == 'FPY_TOTAL/SCRAP_P'):    
                sDataType = 'FPY_TOTAL'
                sItemName = '''FPY_TOTAL','SCRAP_P'''  
            elif(self.__item_name == 'HEADLINES/IPQA'):    
                sDataType = ''
                sItemName = '''HEADLINES','IPQA''' 
            elif(self.__item_name == 'SCRAP_P'):    
                sDataType = 'AA'
                sItemName = '''SCRAP_P'''                       
            
            sql = ''

            sql =  """select * from """

            if(self.__item_name == 'HEADLINES/IPQA'):
                sql = sql + """
                            (
                            select t.data_date,t.data_type,
                            case when (t.data_type = 'HEADLINES' and t.data_date <> 'MTD' and t.data_value >= t1.red_day) then 'red'
                                when (t.data_type = 'HEADLINES' and t.data_date <> 'MTD' and t.data_value < t1.red_day) then 'green'
                                when (t.data_type = 'HEADLINES' and t.data_date = 'MTD' and t.data_value >= t1.red_mtd) then 'green' 
                                when (t.data_type = 'HEADLINES' and t.data_date = 'MTD' and t.data_value < t1.red_mtd) then 'green' 
                                
                                when (t.data_type = 'IPQA' and t.data_date <> 'MTD' and t.data_value > t1.red_day) then 'red'
                                when (t.data_type = 'IPQA' and t.data_date <> 'MTD' and t.data_value >= t1.green_day and t.data_value <= t1.red_day) then 'yellow'
                                when (t.data_type = 'IPQA' and t.data_date <> 'MTD' and t.data_value < t1.green_day) then 'green' 
                                when (t.data_type = 'IPQA' and t.data_date = 'MTD' and t.data_value > t1.red_mtd) then 'red'
                                when (t.data_type = 'IPQA' and t.data_date = 'MTD' and t.data_value >= t1.green_mtd and t.data_value <= t1.red_mtd) then 'yellow'
                                when (t.data_type = 'IPQA' and t.data_date = 'MTD' and t.data_value < t1.green_mtd) then 'green'
                                end||'/'||t.data_value as data_value
                                from """  

            elif(self.__item_name == 'TD/INPUT' or self.__item_name == 'REACH' or self.__item_name == 'OEE'):
                sql = sql + """
                            (
                            select t.data_date,t.data_type,
                            case when (t.data_date <> 'MTD' and t.data_value < t1.red_day) then 'red'
                                when (t.data_date <> 'MTD' and t.data_value >= t1.red_day and t.data_value <= t1.green_day) then 'yellow'
                                when (t.data_date <> 'MTD' and t.data_value > t1.green_day) then 'green' 
                                when (t.data_date = 'MTD' and t.data_value < t1.red_mtd) then 'red'
                                when (t.data_date = 'MTD' and t.data_value >= t1.red_mtd and t.data_value <= t1.green_mtd) then 'yellow'
                                when (t.data_date = 'MTD' and t.data_value > t1.green_mtd) then 'green' 
                                end||'/'||t.data_value as data_value
                                from """ 

            else:
                sql = sql + """
                            (
                            select t.data_date,t.data_type,
                            case when (t.data_type = '{3}' and t.data_date <> 'MTD' and t.data_value < t1.red_day) then 'red'
                                when (t.data_type = '{3}' and t.data_date <> 'MTD' and t.data_value >= t1.red_day and t.data_value <= t1.green_day) then 'yellow'
                                when (t.data_type = '{3}' and t.data_date <> 'MTD' and t.data_value > t1.green_day) then 'green' 
                                when (t.data_type = '{3}' and t.data_date = 'MTD' and t.data_value < t1.red_mtd) then 'red'
                                when (t.data_type = '{3}' and t.data_date = 'MTD' and t.data_value >= t1.red_mtd and t.data_value <= t1.green_mtd) then 'yellow'
                                when (t.data_type = '{3}' and t.data_date = 'MTD' and t.data_value > t1.green_mtd) then 'green' 
                                
                                when (t.data_type <> '{3}' and t.data_date <> 'MTD' and t.data_value > t1.red_day) then 'red'
                                when (t.data_type <> '{3}' and t.data_date <> 'MTD' and t.data_value >= t1.green_day and t.data_value <= t1.red_day) then 'yellow'
                                when (t.data_type <> '{3}' and t.data_date <> 'MTD' and t.data_value < t1.green_day) then 'green' 
                                when (t.data_type <> '{3}' and t.data_date = 'MTD' and t.data_value > t1.red_mtd) then 'red'
                                when (t.data_type <> '{3}' and t.data_date = 'MTD' and t.data_value >= t1.green_mtd and t.data_value <= t1.red_mtd) then 'yellow'
                                when (t.data_type <> '{3}' and t.data_date = 'MTD' and t.data_value < t1.green_mtd) then 'green'
                                end||'/'||t.data_value as data_value
                                from """
            sql = sql + """                   
                          (
                            SELECT to_char(t.data_date,'mm/dd') as data_date,to_char(t.data_date,'mm') as month_date,t.data_type,to_char(t.data_value) as  data_value,t.item_name
                            FROM WAYNE_TEST_TV t 
                            where item_name in ('{4}') 
                            and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                            and t.line_type = '{5}'

                            union """

            if(self.__item_name == 'HEADLINES/IPQA'):
                sql = sql + """                
                            select 'MTD' as date_time,substr('{0}',4,2) as month_dat,t.data_type,
                            decode(t.data_type,'HEADLINES',to_char(sum(t.data_value)),to_char(round(decode(t.data_type,'COLOR',round(sum(t.data_value)/count(*),0),sum(t.data_value)/count(*)),1),'FM990.0')) as data_value,t.item_name
                            from WAYNE_TEST_TV t
                            where item_name in ('{4}') 
                            and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                            and t.line_type = '{5}'
                            group by t.data_type,t.item_name """

            else:     
                sql = sql + """                
                            select 'MTD' as date_time,substr('{1}',4,2) as month_dat,t.data_type,to_char(round(decode(t.data_type,'COLOR',round(sum(t.data_value)/count(*),0),sum(t.data_value)/count(*)),1),'FM990.0') as data_value,t.item_name
                            from WAYNE_TEST_TV t
                            where item_name in ('{4}') 
                            and t.data_date between to_date('{0}','yyyy/mm/dd hh24miss') and to_date('{1}','yyyy/mm/dd hh24miss')
                            and t.line_type = '{5}'
                            group by t.data_type,t.item_name """           

            sql = sql + """                 
                          )t,
                          (
                            select to_char(t.data_date,'MM') as data_date,t.item_name,t.item_desc,t.red_day,t.green_day,t.red_mtd,t.green_mtd from isfp_target_upload t 
                            where t.data_date between to_date(substr('{0}',0,6),'yyyy/mm') and to_date(substr('{1}',0,6),'yyyy/mm') 
                            and t.item_name in ('{4}')
                          )t1
                          where t.month_date = t1.data_date
                          and t.item_name = t1.item_name
                        )
                        PIVOT (max (data_value)FOR data_date IN ('{2}')) """

            if(self.__item_name == 'REACH' or self.__item_name == 'OEE' or self.__item_name == 'FPY_TOTAL/SCRAP_P' or self.__item_name == 'HEADLINES/IPQA' or self.__item_name == 'SCRAP_P'):
                sql = sql + """  order by 1 """
            else:  
                sql = sql + """  order by 1 desc """

            sql = sql.format(self.__start_time, self.__end_time, self.__sColnumName, sDataType, sItemName, self.__line_type) 
            
            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data_result = self.Select(sql)
            self.closeConnection()

            datajson=[]
            dataitem=[]
            dataItemTitle= [0]
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
                    elif(da[0] == 'SCRAP_P'):  
                        dataitem = '破片報廢率(%)'
                    elif(da[0] == 'FPY_TOTAL'):
                        dataitem = 'FPYTotal(%)'
                    elif(da[0] == 'HEADLINES'):  
                        dataitem = '今日頭條%'
                    elif(da[0] == 'IPQA'):
                        dataitem = 'IPQA(%)'           
                    else: 
                        dataitem = da[0]     
                         
                    dataTitle.append(dataitem)   
                    dataitem = []

                    #組元件所需Value格式
                    for da_D in data:
                        
                        if(da[iNum_data_list] is None):
                            Testdatadict={
                                    "color" : '',
                                    "value" : ''
                                }   
                        else:    
                            Color_Value = da[iNum_data_list].split("/", 1)
                            if(self.__item_name == 'HEADLINES/IPQA'):
                                Testdatadict={
                                        "color" : Color_Value[0],
                                        "value" : Color_Value[1]
                                    }   
                            else:  
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

            if(self.__item_name == 'REACH'):
                dataItemTitle[0] = "達產(%)"
                ResuleSide.append(dataItemTitle)
                ResuleSide.append(dataTitle)
                iborderType = 1
            elif(self.__item_name == 'OEE'):
                dataItemTitle[0] = "設備OEE%(%)"
                ResuleSide.append(dataItemTitle)
                ResuleSide.append(dataTitle)
                iborderType = 1    
            else:    
                ResuleSide.append(dataTitle)
                iborderType = 2

            responseResult = dict(circleSize = ResultCircleSize, fontSize = ResultFontSize, borderType = iborderType,titleArray = ResultColnumjson,sideArray = ResuleSide,listArray = dataListArray)
            

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