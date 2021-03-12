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
class OfrCompare(BaseType):
    def __init__(self, COMPANY_CODE, SITE, TIME_DIM, DATA_SEQ, indentity):
        super().__init__()     
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__COMPANY_CODE = COMPANY_CODE
        self.__SITE = SITE
        self.__TIME_DIM = TIME_DIM
        self.__DATA_SEQ = DATA_SEQ
        self.__indentity = indentity
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            Dsql = None
            Msql = None
            i = 0
            if  len(self.__SITE.strip()) == 0:
                Dsql = "WITH eqp_info AS (select company_code, site, 'NA' factory_id, 'NA' line_id, 'NA' eqp_id from dmb_aline_monitor_map t where company_code = '{1}'  group by company_code, site) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id  AS SUPPLY_CATEGORY,i.eqp_id,hi.{0}_cd AS TIME,Avg(s.assignment)   AS Assignment,Avg(s.fulfillment)  AS fulfillment,Avg(s.finish_ratio) AS finish_ratio FROM   eqp_info i cross join dmb_time_dim hi left join dmb_dc_ofr_site_{0} s ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id = Nvl (i.factory_id, 'NA') AND s.line_id = Nvl (i.line_id, 'NA') AND s.eqp_id = Nvl (i.eqp_id, 'NA') AND hi.{0}_cd = s.{0}_cd WHERE  hi.{0}_seq <= '{2}' GROUP  BY i.company_code, i.site,i.factory_id,i.line_id,i.eqp_id,hi.{0}_cd ORDER  BY Decode(site, 'TN', 1,'JN', 2,'FS', 3,'NGB', 4,'NJ', 5),Decode(factory_id, 'M011', 1,'J001', 2,'J003', 3,'J004', 4,5),TIME ASC, TIME ASC ".format(self.__TIME_DIM, self.__COMPANY_CODE, self.__DATA_SEQ)
                Msql = "select distinct site from dmb_aline_monitor_map WHERE  company_code = '{0}' ORDER BY Decode(site, 'TN', 1, 'JN', 2, 'FS', 3, 'NGB', 4, 'NJ', 5)".format(self.__COMPANY_CODE)
                i = 1#site 
            else:
                Dsql = "WITH eqp_info AS (select company_code, site, factory_id, 'NA' line_id, 'NA' eqp_id from dmb_aline_monitor_map t where company_code = '{1}' and site = '{2}' group by company_code, site, factory_id) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id AS SUPPLY_CATEGORY,i.eqp_id,hi.{0}_cd  AS TIME,Avg(s.assignment)   AS Assignment,Avg(s.fulfillment)  AS fulfillment,Avg(s.finish_ratio) AS finish_ratio FROM  eqp_info i cross join dmb_time_dim hi left join dmb_dc_ofr_factory_{0} s ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id = Nvl (i.factory_id, 'NA') AND s.line_id = Nvl (i.line_id, 'NA') AND s.eqp_id = Nvl (i.eqp_id, 'NA')  AND hi.{0}_cd = s.{0}_cd WHERE  hi.{0}_seq <= '{3}' GROUP  BY i.company_code,i.site,i.factory_id,i.line_id,i.eqp_id, hi.{0}_cd order by decode(factory_id, 'M011', 1, 'J001', 2, 'J003', 3, 'J004', 4, 5), time asc".format(self.__TIME_DIM, self.__COMPANY_CODE, self.__SITE, self.__DATA_SEQ)
                Msql = "select distinct factory_id from dmb_aline_monitor_map WHERE  company_code = '{0}' AND site = '{1}'  ORDER BY Decode(factory_id, 'M011', 1, 'J001', 2, 'J003', 3, 'J004', 4, 5)".format(self.__COMPANY_CODE,self.__SITE)
                i = 2#fab_id
            self.writeLog(f'SQL:\n {Dsql}')

            key = f"{self.__class__.__name__}_{self.__COMPANY_CODE}_{self.__SITE}_{self.__TIME_DIM}_{self.__DATA_SEQ}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}

            self.getConnection(self.__indentity)
            description , data = self.SelectAndDescription(Dsql)
            fabname = self.Select(Msql)
            self.closeConnection()
            col_names = self.getColumnName()
            datajson = []
            for fa in fabname :
                tmp = [dict(zip(col_names, da)) for da in data if da[i] == fa[0]]
                if len(tmp) != 0:
                    datajson.append(tmp)           
            data = json.dumps(datajson, sort_keys=True, indent=2)
            self.writeLog(f"Json:\n {data}")

            self.getRedisConnection()
            self.setRedisData(key, data, 600)

            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
            return json.loads(data),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(minutes = 10)).timetuple()),"DataSource":"Oracle"} 
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


