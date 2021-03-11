# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
from Logger import Logger
log = Logger('ALL.log',level='debug')
def FactorySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM dual) SELECT company_code,site,factory_id,hour_cd as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.hour_cd,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.hour_cd = s.period WHERE  hi.hour_seq <= '{3}') GROUP  BY company_code,site,factory_id,hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM dual) SELECT company_code,site,factory_id,DAY_CD as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.DAY_CD,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.DAY_CD = s.DAY_CD WHERE  hi.day_seq <= '{3}') GROUP  BY company_code,site,factory_id,DAY_CD ORDER BY DAY_CD ASC".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def SiteSQL(DATATYPE,COMPANY_CODE,SITE,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM dual) SELECT company_code,site,hour_cd as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.hour_cd,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.hour_cd = s.period WHERE  hi.hour_seq <= '{2}') GROUP  BY company_code,site,hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,SITE,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM dual) SELECT company_code,site,DAY_CD as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.DAY_CD,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.DAY_CD = s.DAY_CD WHERE  hi.day_seq <= '{2}') GROUP  BY company_code,site,DAY_CD ORDER BY DAY_CD ASC".format(COMPANY_CODE,SITE,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def CompanySQL(DATATYPE,COMPANY_CODE,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code FROM dual) SELECT company_code,hour_cd as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,hi.hour_cd,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND hi.hour_cd = s.period WHERE  hi.hour_seq <= '{1}') GROUP  BY company_code,hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code FROM dual) SELECT company_code,DAY_CD as time,SUM (assignment) assignment,nvl(Round (SUM (finish_count) / NULLIF (SUM (assignment), 0) * 100, 2), 0) finish_ratio, SUM (finish_count) finish_count FROM (SELECT i.company_code,hi.DAY_CD,Nvl (s.assignment, 0) AS assignment,Nvl (s.finish_ratio, 0) AS finish_ratio,Round (Nvl (s.assignment, 0) * Nvl (s.finish_ratio, 0) / 100, 2) AS finish_count FROM eqp_info i cross join dmb_time_dim_v hi left join dmb_dc_agv_pass_log_v s ON s.company_code = i.company_code AND hi.DAY_CD = s.DAY_CD WHERE  hi.day_seq <= '{1}') GROUP  BY company_code,DAY_CD ORDER BY DAY_CD ASC".format(COMPANY_CODE,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
class OFRHrsDetail:
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,dbschema):
        log.logger.info('OFRHrsDetail __init__')
        self.COMPANY_CODE=COMPANY_CODE
        self.SITE=SITE
        self.FACTORY_ID=FACTORY_ID
        self.DATATYPE=DATATYPE
        self.DATA_SEQ=DATA_SEQ
        self.dbschema=dbschema
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.dbschema).READ()
    def GetOFRHrsDetailData(self):
        try:
            log.logger.info('OFRHrsDetail GetOFRHrsDetailData Start')
            sql=None
            if len(self.FACTORY_ID.strip())>0:
                sql=FactorySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            elif len(self.SITE.strip())>0:
                sql=SiteSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.DATA_SEQ)
            else:
                sql=CompanySQL(self.DATATYPE,self.COMPANY_CODE,self.DATA_SEQ)
            log.logger.info('SQL:\n'+sql)
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            daoHelper.Connect()
            description,data=daoHelper.SelectAndDescription(sql)
            daoHelper.Close()
            col_names = [row[0] for row in description]
            datajson=[dict(zip(col_names,da)) for da in data]
            dataT=json.dumps(datajson,sort_keys=True, indent=2)
            log.logger.info('Json:\n'+str(dataT))
            log.logger.info('OFRHrsDetail GetOFRHrsDetailData DONE')
            log.logger.info('Route /GetFactoryOFRHrsDetail DONE')
            return dataT,200,{"Content-Type": "application/json",'Connection':'close'}
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return jsonify({'Result': 'NG','Reason':'{0} erro'.format(funcName)}),400 ,{"Content-Type": "application/json",'Connection':'close'}
