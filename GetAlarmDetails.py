from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
import datetime
import time
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class AlarmDetails(BaseType):
    def __init__(self, COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, TOP,indentity):
        super().__init__()     
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__COMPANY_CODE = COMPANY_CODE
        self.__SITE = SITE
        self.__FACTORY_ID = FACTORY_ID
        self.__SUPPLY_CATEGORY = SUPPLY_CATEGORY
        self.__EQP_ID = EQP_ID
        self.__SPACE_DIM = SPACE_DIM
        self.__TIME_DIM = TIME_DIM
        self.__DATA_SEQ = DATA_SEQ
        self.__TOP = TOP
        self.__indentity = indentity        
    def getData(self):#nvl(s.alarm_desc,'Not Define') and s.alarm_id is not null
       try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            sql = f"WITH eqp_info AS (SELECT '{self.__COMPANY_CODE}' company_code,'{self.__SITE}' site,'{self.__FACTORY_ID}' factory_id,'{self.__SUPPLY_CATEGORY}' line_id,'{self.__EQP_ID}' eqp_id FROM DUAL) SELECT alm.company_code,alm.site,alm.factory_id,alm.supply_category,alm.eqp_id,alm.TIME,alm.alarm_id,alm.alarm_desc,SUM(alm.alarm_interval) AS alarm_interval,SUM(alm.alarm_times) AS alarm_times FROM   (SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id AS supply_category,i.eqp_id,hi.{self.__TIME_DIM}_cd AS TIME,ai.alarm_id,NVL(ai.alarm_desc, CONCAT(ai.alarm_id, 'Undefined')) alarm_desc,NVL(s.alarm_interval,0) alarm_interval,NVL(s.alarm_times,0) alarm_times FROM eqp_info i CROSS JOIN dmb_time_dim hi CROSS JOIN (SELECT alarm_id, alarm_desc FROM ( SELECT s.alarm_id,alarm_desc,RANK () OVER (PARTITION BY i.company_code,i.site,i.factory_id,i.line_id,i.eqp_id ORDER BY SUM (alarm_times) DESC, SUM (alarm_interval) DESC) rank_seq  FROM eqp_info i CROSS JOIN dmb_time_dim hi LEFT JOIN dmb_dc_alm_{self.__SPACE_DIM.upper()}_{self.__TIME_DIM} s  ON  s.company_code = i.company_code AND s.site = i.site AND s.factory_id = NVL (i.factory_id, 'NA') AND s.line_id = NVL (i.line_id, 'NA') AND s.eqp_id = NVL (i.eqp_id, 'NA') AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd WHERE hi.{self.__TIME_DIM}_seq <= {self.__DATA_SEQ} AND s.alarm_id IS NOT NULL GROUP BY i.company_code,i.site,i.factory_id,i.line_id,i.eqp_id,s.alarm_id,s.alarm_desc) a WHERE rank_seq <= {self.__TOP}) ai LEFT JOIN dmb_dc_alm_{self.__SPACE_DIM.upper()}_{self.__TIME_DIM} s ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id = NVL (i.factory_id, 'NA') AND s.line_id = NVL (i.line_id, 'NA') AND s.eqp_id = NVL (i.eqp_id, 'NA') AND hi.{self.__TIME_DIM}_cd = s.{self.__TIME_DIM}_cd AND ai.alarm_id = s.alarm_id WHERE  hi.{self.__TIME_DIM}_seq <= {self.__DATA_SEQ})alm GROUP  BY company_code,site,factory_id,supply_category,eqp_id,TIME,alarm_id,alarm_desc ORDER  BY alarm_id,TIME ASC"
            #if len(self.__TIME_DIM) < 2:
            #    sql = "WITH eqp_info AS ( SELECT '{0}' company_code, '{1}' site, '{2}' factory_id,'{3}' line_id,'{4}' eqp_id FROM  dual) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id AS supply_category,i.eqp_id,hi.{6}_cd AS TIME,ai.alarm_id,nvl(ai.alarm_desc,CONCAT(ai.alarm_id,'Undefined'))  alarm_desc,s.alarm_interval,s.alarm_times FROM  eqp_info i cross join  dmb_time_dim hi cross join  DMB_ALARM_ID_DIM ai left join     dmb_dc_alm_{5}_{6} s ON  s.company_code = i.company_code AND  s.site = i.site AND   s.factory_id= nvl (i.factory_id, 'NA') AND  s.line_id = nvl (i.line_id, 'NA') AND  s.eqp_id = nvl (i.eqp_id, 'NA') AND  hi.{6}_cd = s.{6}_cd AND  ai.alarm_id = s.alarm_id and hi.{8}_cd=s.{8}_cd WHERE  hi.{8}_seq <= '{7}' ORDER BY   ai.alarm_id, hi.{6}_cd ASC".format(self.__COMPANY_CODE, self.__SITE, self.__FACTORY_ID, self.__SUPPLY_CATEGORY, self.__EQP_ID, self.__SPACE_DIM.upper(), self.TIME_CD(self.__TIME_DIM), "1" , self.Time_SEQ(self.__TIME_DIM))
                
            #else:
            #    sql = "WITH eqp_info AS (SELECT '{0}'company_code, '{1}' site, '{2}' factory_id,'{3}' line_id,'{4}' eqp_id  FROM   dual) select alm.company_code, alm.site, alm.factory_id,alm.supply_category,alm.eqp_id,alm.TIME,alm.alarm_id,alm.alarm_desc,sum(alm.alarm_interval) as  alarm_interval,sum(alm.alarm_times)as alarm_times FROM (SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id  AS supply_category,i.eqp_id,hi.{6}_cd AS TIME,ai.alarm_id,Nvl(ai.alarm_desc, Concat(ai.alarm_id, 'Undefined')) alarm_desc,s.alarm_interval ,s.alarm_times FROM  eqp_info i cross join dmb_time_dim hi cross join dmb_alarm_id_dim ai left join dmb_dc_alm_{5}_{6} s ON s.company_code = i.company_code  AND s.site = i.site AND s.factory_id = Nvl (i.factory_id, 'NA') AND s.line_id = Nvl (i.line_id, 'NA') AND s.eqp_id = Nvl (i.eqp_id, 'NA') AND hi.{6}_cd = s.{6}_cd  AND ai.alarm_id = s.alarm_id WHERE  hi.{6}_seq <= {7} )alm group by company_code,site,factory_id,supply_category,eqp_id,TIME,alarm_id,alarm_desc ORDER  BY alarm_id,time ASC".format(self.__COMPANY_CODE, self.__SITE, self.__FACTORY_ID, self.__SUPPLY_CATEGORY, self.__EQP_ID, self.__SPACE_DIM.upper(), self.__TIME_DIM, self.__DATA_SEQ)          
            self.writeLog(f'SQL:\n {sql}')
            
            key = f"{self.__class__.__name__}_{self.__COMPANY_CODE}_{self.__SITE}_{self.__FACTORY_ID}_{self.__SUPPLY_CATEGORY}_{self.__EQP_ID}_{self.__SPACE_DIM.upper()}_{self.__TIME_DIM}_{self.__DATA_SEQ}_{self.__TOP}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}
   
            self.getConnection(self.__indentity)
            description, data = self.SelectAndDescription(sql)
            self.closeConnection()
            data = self.zipDescriptionAndData(description, data)
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

#"WITH eqp_info AS ( SELECT '{0}' company_code, '{1}' site, '{2}' factory_id,'{3}' line_id,'{4}' eqp_id FROM  dual) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id AS supply_category,i.eqp_id,hi.{6}_cd AS TIME,ai.alarm_id,nvl(ai.alarm_desc,CONCAT(ai.alarm_id,'Undefined'))  alarm_desc,sum(s.alarm_interval) as alarm_interval,sum(s.alarm_times) as  alarm_times FROM  eqp_info i cross join  dmb_time_dim hi cross join  DMB_ALARM_ID_DIM ai left join       dmb_dc_alm_{5}_{6} s ON  s.company_code = i.company_code AND  s.site = i.site AND   s.factory_id= nvl (i.factory_id, 'NA') AND  s.line_id = nvl (i.line_id, 'NA') AND  s.eqp_id = nvl (i.eqp_id, 'NA') AND  hi.{6}_cd = s.{6}_cd AND  ai.alarm_id = s.alarm_id WHERE  hi.{6}_seq <= '{7}' group by i.company_code,i.site,i.factory_id, i.line_id ,i.eqp_id, hi.{6}_cd ,ai.alarm_id,ai.alarm_desc ORDER BY   ai.alarm_id, hi.{6}_cd ASC"


#"WITH eqp_info AS (SELECT '{0}'company_code, '{1}' site, '{2}' factory_id,'{3}' line_id,'{4}' eqp_id  FROM   dual) select alm.company_code, alm.site, alm.factory_id,alm.supply_category,alm.eqp_id,alm.TIME,alm.alarm_id,alm.alarm_desc,sum(alm.alarm_interval) as  alarm_interval,sum(alm.alarm_times)as alarm_times FROM (SELECT DISTINCT i.company_code,i.site,i.factory_id,i.line_id  AS supply_category,i.eqp_id,hi.{6}_cd AS TIME,ai.alarm_id,Nvl(ai.alarm_desc, Concat(ai.alarm_id, 'Undefined')) alarm_desc,s.alarm_interval ,s.alarm_times FROM  eqp_info i cross join dmb_time_dim hi cross join dmb_alarm_id_dim ai left join dmb_dc_alm_{5}_{6} s ON s.company_code = i.company_code  AND s.site = i.site AND s.factory_id = Nvl (i.factory_id, 'NA') AND s.line_id = Nvl (i.line_id, 'NA') AND s.eqp_id = Nvl (i.eqp_id, 'NA') AND hi.{6}_cd = s.{6}_cd  AND ai.alarm_id = s.alarm_id WHERE  hi.{6}_seq <= {7} )alm group by company_code,site,factory_id,supply_category,eqp_id,TIME,alarm_id,alarm_desc ORDER  BY alarm_id,time ASC"