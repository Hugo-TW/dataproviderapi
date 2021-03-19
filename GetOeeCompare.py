from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import datetime
import time
import os
import sys
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class OeeCompare(BaseType):
    def __init__(self, COMPANY_CODE, SITE,SUPPLY_CATEGORY,FACTORY_ID,TIME_DIM, DATA_SEQ, indentity):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__COMPANY_CODE = COMPANY_CODE
        self.__SITE = SITE
        self.__FACTORY_ID = FACTORY_ID
        self.__SUPPLY_CATEGORY = SUPPLY_CATEGORY       
        self.__TIME_DIM = TIME_DIM
        self.__DATA_SEQ = DATA_SEQ
        self.__indentity = indentity
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql = ""
            if self.__COMPANY_CODE and self.__SITE and self.__FACTORY_ID and self.__SUPPLY_CATEGORY:
               sql = f"""
                        WITH eqp_info 
                        AS (SELECT DISTINCT 
                                company_code, 
                                site, 
                                factory_id, 
                                supply_line as line_id, 
                                entity_id as eqp_id 
                        FROM   dmb_aline_all_eqp_v t 
                        WHERE  company_code = '{self.__COMPANY_CODE}' 
                        AND site = '{self.__SITE}' 
                        AND factory_id = '{self.__FACTORY_ID}' 
                        AND supply_line = '{self.__SUPPLY_CATEGORY}'
                        AND type = 'AGV') 
                        SELECT DISTINCT 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id AS SUPPLY_CATEGORY, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd AS TIME, 
                            nvl(Avg (s.status_run),0)  AS STATUS_RUN, 
                            nvl(Avg (s.status_down),0) AS STATUS_DOWN, 
                            nvl(Avg (s.status_idle),0) AS STATUS_IDLE 
                        FROM  eqp_info i 
                            cross join dmb_time_dim hi 
                                left join dmb_dc_oee_eqp_{self.__TIME_DIM} s 
                                    ON s.company_code = i.company_code 
                                        AND s.site = i.site 
                                        AND s.factory_id = Nvl (i.factory_id, 'NA') 
                                        AND s.line_id = Nvl (i.line_id, 'NA') 
                                        AND s.eqp_id = Nvl (i.eqp_id, 'NA') 
                                        AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd 
                            WHERE  hi.{self.__TIME_DIM}_seq <= '{self.__DATA_SEQ}' 
                        GROUP  BY 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd 
                        ORDER  BY Decode (factory_id, 
                                'M011', 1, 
                                'J001', 2, 
                                'J003', 3, 
                                'J004', 4, 
                                5), 
                        eqp_id ASC, 
                        TIME ASC
                      """
            elif self.__COMPANY_CODE and self.__SITE and self.__FACTORY_ID:
                sql = f"""
                        WITH eqp_info 
                        AS (SELECT DISTINCT 
                            company_code, 
                            site, 
                            factory_id, 
                            supply_line as line_id, 
                            'NA' as eqp_id 
                        FROM  dmb_aline_all_eqp_v t 
                        WHERE company_code = '{self.__COMPANY_CODE}'
                        AND site = '{self.__SITE}' 
                        AND factory_id = '{self.__FACTORY_ID}'
                        AND type = 'AGV') 
                        SELECT DISTINCT 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id AS SUPPLY_CATEGORY, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd AS TIME, 
                            nvl(Avg (s.status_run),0) AS STATUS_RUN, 
                            nvl(Avg (s.status_down),0) AS STATUS_DOWN, 
                            nvl(Avg (s.status_idle),0) AS STATUS_IDLE 
                        FROM  eqp_info i 
                            cross join dmb_time_dim hi 
                                left join dmb_dc_oee_line_{self.__TIME_DIM} s 
                                    ON s.company_code = i.company_code 
                                    AND s.site = i.site 
                                    AND s.factory_id = Nvl (i.factory_id, 'NA') 
                                    AND s.line_id = Nvl (i.line_id, 'NA') 
                                    AND s.eqp_id = Nvl (i.eqp_id, 'NA') 
                                    AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd 
                        WHERE  hi.{self.__TIME_DIM}_seq <= '{self.__DATA_SEQ}'
                        GROUP  BY 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd 
                        ORDER  BY Decode (factory_id, 
                              'M011', 1, 
                              'J001', 2, 
                              'J003', 3, 
                              'J004', 4, 
                              5), 
                        supply_category ASC, 
                        TIME ASC 
                       """
            elif self.__COMPANY_CODE and self.__SITE:
                sql = f"""
                        WITH eqp_info 
                        AS (SELECT DISTINCT 
                            company_code, 
                            site, 
                            factory_id, 
                            'NA' as line_id, 
                            'NA' as eqp_id 
                        FROM   dmb_aline_all_eqp_v t 
                        WHERE  company_code = '{self.__COMPANY_CODE}'
                        AND site = '{self.__SITE}' 
                        AND type = 'AGV') 
                        SELECT DISTINCT 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id  AS SUPPLY_CATEGORY, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd  AS TIME, 
                            nvl(Avg (s.status_run),0)  AS STATUS_RUN, 
                            nvl(Avg (s.status_down),0) AS STATUS_DOWN, 
                            nvl(Avg (s.status_idle),0) AS STATUS_IDLE 
                        FROM  eqp_info i 
                            cross join dmb_time_dim hi 
                                left join dmb_dc_oee_factory_{self.__TIME_DIM} s 
                                    ON s.company_code = i.company_code 
                                    AND s.site = i.site 
                                    AND s.factory_id = Nvl (i.factory_id, 'NA') 
                                    AND s.line_id = Nvl (i.line_id, 'NA') 
                                    AND s.eqp_id = Nvl (i.eqp_id, 'NA') 
                                    AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd 
                        WHERE  hi.{self.__TIME_DIM}_seq <= '{self.__DATA_SEQ}'
                        GROUP  BY 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd 
                        ORDER  BY Decode (factory_id, 
                              'M011', 1, 
                              'J001', 2, 
                              'J003', 3, 
                              'J004', 4, 
                              5), 
                        TIME ASC 
                       """
            else:
                sql = f"""
                        WITH eqp_info 
                        AS (SELECT DISTINCT 
                            company_code, 
                            site, 
                            'NA' factory_id, 
                            'NA' as line_id, 
                            'NA' as eqp_id 
                        FROM   dmb_aline_all_eqp_v t 
                        WHERE  company_code = '{self.__COMPANY_CODE}'
                        AND type = 'AGV') 
                        SELECT DISTINCT 
                            i.company_code, 
                            i.site, 
                            i.factory_id, 
                            i.line_id AS SUPPLY_CATEGORY, 
                            i.eqp_id, 
                            hi.{self.__TIME_DIM}_cd AS TIME, 
                nvl(Avg (s.status_run),0)  AS STATUS_RUN, 
                nvl(Avg (s.status_down),0) AS STATUS_DOWN, 
                nvl(Avg (s.status_idle),0) AS STATUS_IDLE 
                FROM  eqp_info i 
                    cross join dmb_time_dim hi 
                        left join dmb_dc_oee_site_{self.__TIME_DIM} s 
                            ON s.company_code = i.company_code 
                            AND s.site = i.site 
                            AND s.factory_id = Nvl (i.factory_id, 'NA') 
                            AND s.line_id = Nvl (i.line_id, 'NA') 
                            AND s.eqp_id = Nvl (i.eqp_id, 'NA') 
                            AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd 
                WHERE  hi.{self.__TIME_DIM}_seq <= '{self.__DATA_SEQ}' 
                GROUP  BY 
                    i.company_code, 
                    i.site, 
                    i.factory_id, 
                    i.line_id, 
                    i.eqp_id, 
                    hi.{self.__TIME_DIM}_cd 
                ORDER  BY Decode (factory_id, 
                              'M011', 1, 
                              'J001', 2, 
                              'J003', 3, 
                              'J004', 4, 
                              5), 
                site ASC, 
                TIME ASC 
                      """
            self.writeLog(f'SQL:{sql}')

            key = f"{self.__class__.__name__}_{self.__COMPANY_CODE}_{self.__SITE}_{self.__FACTORY_ID}_{self.__SUPPLY_CATEGORY}_{self.__TIME_DIM}_{self.__DATA_SEQ}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}
            self.getConnection(self.__indentity)
            description, data = self.SelectAndDescription(sql)
            self.closeConnection()   
            data = self.zipDescriptionAndData(description, data)
            jsonData = json.loads(data)
            jsonDataAr = []
            data = []
            for i in range(1,len(jsonData) + 1):
                jsonDataAr.append(jsonData[i-1])
                if i % int(self.__DATA_SEQ) == 0:
                    #data.append(jsonDataAr)
                    data.extend(jsonDataAr)
                    jsonDataAr = []             
            self.writeLog(f"Json:\n {json.dumps(data,sort_keys=True, indent=2)}")

            self.getRedisConnection()
            self.setRedisData(key, json.dumps(data,sort_keys=True, indent=2), 600)

            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')     
            return data ,200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(minutes = 10)).timetuple()),"DataSource":"Oracle"} 
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