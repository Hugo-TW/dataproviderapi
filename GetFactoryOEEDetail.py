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

def SwitchFactorySQL(datatype,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"  SELECT company_code,site,factory_id,hour_cd as TIME,ROUND (SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_RUN, ROUND (SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_DOWN, ROUND (SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site ,'{2}' factory_id FROM DUAL) SELECT i.company_code,i.site,i.factory_id,hi.hour_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.hour_cd = s.period WHERE hi.hour_seq <= {3} GROUP BY i.company_code,i.site, i.factory_id, hi.hour_cd) GROUP BY company_code,site, factory_id, hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,SITE,FACTORY_ID,int(DATA_SEQ)),
        "DailyByPeriod":"SELECT company_code,site,factory_id,day_cd as TIME,ROUND(SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_RUN,ROUND(SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME)*100,2)AS STATUS_DOWN,ROUND(SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site, '{2}' factory_id FROM DUAL) SELECT DISTINCT i.company_code,i.site,i.factory_id,hi.day_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {3} GROUP BY i.company_code,i.site, i.factory_id, hi.day_cd) GROUP BY company_code, site,factory_id, day_cd ORDER BY day_cd ASC".format(COMPANY_CODE,SITE,FACTORY_ID,int(DATA_SEQ)),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.week_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON     s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ)),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.month_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON     s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.month_cd = s.month_cd WHERE hi.month_seq <={2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ))
    }
    return switcher.get(datatype,None)
def SwitchSiteSQL(datatype,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"  SELECT company_code,site,hour_cd as TIME,ROUND (SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_RUN, ROUND (SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_DOWN, ROUND (SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code, '{1}' site FROM DUAL) SELECT i.company_code,i.site,hi.hour_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON s.company_code = i.company_code AND s.site = i.site AND hi.hour_cd = s.period WHERE hi.hour_seq <= {2} GROUP BY i.company_code, i.site, hi.hour_cd) GROUP BY company_code,site, hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,SITE,int(DATA_SEQ)),
        "DailyByPeriod":"SELECT company_code,site,day_cd as TIME,ROUND(SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_RUN,ROUND(SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_DOWN,ROUND(SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT DISTINCT i.company_code,i.site,hi.day_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {2} GROUP BY i.company_code,i.site, hi.day_cd) GROUP BY company_code,site, day_cd ORDER BY day_cd ASC".format(COMPANY_CODE,SITE,int(DATA_SEQ)),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.week_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON     s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ)),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.month_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON     s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.month_cd = s.month_cd WHERE hi.month_seq <={2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ))
    }
    return switcher.get(datatype,None)
def SwitchCompanySQL(datatype,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":" SELECT company_code,hour_cd as TIME,ROUND (SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_RUN, ROUND (SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_DOWN, ROUND (SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME) * 100, 2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code FROM DUAL) SELECT i.company_code,hi.hour_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON s.company_code = i.company_code AND hi.hour_cd = s.period WHERE hi.hour_seq <= {1} GROUP BY i.company_code, hi.hour_cd) GROUP BY company_code, hour_cd ORDER BY hour_cd ASC".format(COMPANY_CODE,int(DATA_SEQ)),
        "DailyByPeriod":"SELECT company_code,day_cd as TIME,ROUND(SUM (STATUS_RUN) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_RUN,ROUND(SUM (STATUS_DOWN) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_DOWN,ROUND(SUM (STATUS_IDLE) / SUM (STATUS_TOTAL_TIME)*100,2) AS STATUS_IDLE FROM (WITH eqp_info AS (SELECT '{0}' company_code FROM DUAL) SELECT DISTINCT i.company_code,hi.day_cd,SUM (s.STATUS_RUN) STATUS_RUN,SUM (s.STATUS_DOWN) STATUS_DOWN,SUM (s.STATUS_IDLE) STATUS_IDLE,(SUM (s.STATUS_RUN)+ SUM (s.STATUS_DOWN)+ SUM (s.STATUS_IDLE)) STATUS_TOTAL_TIME FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {1} GROUP BY i.company_code,hi.day_cd) GROUP BY company_code, day_cd ORDER BY day_cd ASC".format(COMPANY_CODE,int(DATA_SEQ)),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.week_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON     s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ)),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code, '{1}' factory_id FROM DUAL) SELECT i.company_code,i.factory_id,hi.month_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON   s.company_code = i.company_code AND s.factory_id = i.factory_id AND hi.month_cd = s.month_cd WHERE hi.month_seq <={2}".format(COMPANY_CODE,FACTORY_ID,int(DATA_SEQ))
    }
    return switcher.get(datatype,None)
#class Pack:
#    def __init__(self,COMPANY_CODE,FACTORY_ID,TIME,STATUS_RUN,STATUS_DOWN,STATUS_IDLE):
#        self.COMPANY_CODE=COMPANY_CODE
#        self.FACTORY_ID=FACTORY_ID
#        self.TIME=TIME
#        self.STATUS_RUN=STATUS_RUN
#        self.STATUS_DOWN=STATUS_DOWN
#        self.STATUS_IDLE=STATUS_IDLE
#    def __repr__(self):
#        return repr(self.COMPANY_CODE,self.FACTORY_ID,self.TIME,self.STATUS_RUN,self.STATUS_DOWN,self.STATUS_IDLE)
class SiteOEEDetail:
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity):
        log.logger.info('SiteOEEDetail __init__')
        self.COMPANY_CODE=COMPANY_CODE
        self.SITE=SITE
        self.FACTORY_ID=FACTORY_ID
        self.DATATYPE=DATATYPE
        self.indentity=indentity
        self.DATA_SEQ=DATA_SEQ
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.indentity).READ()
    def GetSiteOEEDetailData(self):
        try:
            log.logger.info('SiteOEEDetail GetSiteOEEDetailData Start')
            sql=None
            if len(self.FACTORY_ID.strip())>0:
                sql=SwitchFactorySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            elif len(self.SITE.strip())>0:
                sql=SwitchSiteSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            else:
                sql=SwitchCompanySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            log.logger.info('SQL:\n'+sql)
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            daoHelper.Connect()
            description,data=daoHelper.SelectAndDescription(sql)
            daoHelper.Close()
            col_names = [row[0] for row in description]
            datajson=[dict(zip(col_names,da)) for da in data]
            #datajson=[]
            #for da in data:
            #    datajson.append(Pack(da[0],da[1],da[2],da[3],da[4],da[5]))
            data=json.dumps(datajson,sort_keys=True, indent=2,ensure_ascii=False)
            log.logger.info('Json:\n'+str(data))
            log.logger.info('SiteOEEDetail GetSiteOEEDetailData DONE')
            log.logger.info('Route /GetFactoryOEEDetails DONE')
            return data,200,{"Content-Type": "application/json",'Connection':'close'}
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
