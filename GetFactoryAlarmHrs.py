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

#class Pack:
#    def __init__(self,COMPANY_CODE,FACTORY_ID,ALARM_ID,ALARM_DESC,ALARM_TIMES,ALARM_INTERVAL):
#        self.COMPANY_CODE=COMPANY_CODE
#        self.FACTORY_ID=FACTORY_ID
#        self.ALARM_ID=ALARM_ID
#        self.ALARM_DESC=ALARM_DESC
#        self.ALARM_TIMES=ALARM_TIMES
#        self.ALARM_INTERVAL=ALARM_INTERVAL
#    def __repr__(self):
#        return repr(self.COMPANY_CODE,self.FACTORY_ID,self.ALARM_ID,self.ALARM_DESC,self.ALARM_TIMES,self.ALARM_INTERVAL)
def FactorySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switch={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM   dual) SELECT company_code, site, factory_id, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,i.site,i.factory_id,hi.hour_cd,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0)    alarm_interval  FROM   eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id = i.factory_id AND hi.hour_cd = s.hour_cd  WHERE  hi.hour_seq <= '{3}') GROUP  BY company_code, site, factory_id, alarm_id,alarm_desc ORDER  BY alarm_id ASC".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM   dual) SELECT company_code, site, factory_id, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,i.site,i.factory_id,hi.DAY_CD,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0) alarm_interval  FROM   eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site = i.site AND s.factory_id = i.factory_id AND hi.DAY_CD = s.DAY_CD  WHERE  hi.day_seq <= '{3}') GROUP  BY company_code, site, factory_id, alarm_id,alarm_desc ORDER  BY alarm_id ASC".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
    }
    return switch.get(DATATYPE,None)
def SiteSQL(DATATYPE,COMPANY_CODE,SITE,DATA_SEQ):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM   dual) SELECT company_code, site, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,i.site,hi.hour_cd,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0)  alarm_interval FROM  eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site = i.site  AND hi.hour_cd = s.hour_cd  WHERE  hi.hour_seq <= '{2}') GROUP  BY company_code, site, alarm_id,alarm_desc ORDER BY alarm_id ASC".format(COMPANY_CODE,SITE,DATA_SEQ),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM   dual) SELECT company_code, site, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,i.site,hi.DAY_CD,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0)  alarm_interval FROM  eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site = i.site AND hi.DAY_CD = s.DAY_CD  WHERE  hi.day_seq <= '{2}') GROUP  BY company_code, site, alarm_id,alarm_desc ORDER BY alarm_id ASC".format(COMPANY_CODE,SITE,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def CompanySQL(DATATYPE,COMPANY_CODE,DATA_SEQ):
    swichter={
        'HourlyByPeriod': "WITH eqp_info AS (SELECT '{0}' company_code FROM  dual) SELECT company_code, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,hi.hour_cd,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0)  alarm_interval FROM  eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code  AND hi.hour_cd = s.hour_cd  WHERE  hi.hour_seq <= '{1}') GROUP  BY company_code, alarm_id,alarm_desc ORDER BY alarm_id ASC".format(COMPANY_CODE,DATA_SEQ),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code FROM  dual) SELECT company_code, alarm_id, alarm_desc, Sum (alarm_times) alarm_times,Sum (alarm_interval) alarm_interval FROM (SELECT DISTINCT i.company_code,hi.DAY_CD,s.alarm_id,s.alarm_desc,Nvl (s.alarm_times_total, 0) alarm_times,Nvl (s.alarm_interval, 0)  alarm_interval FROM  eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code  AND hi.DAY_CD = s.DAY_CD  WHERE  hi.day_seq <= '{1}') GROUP  BY company_code, alarm_id,alarm_desc ORDER BY alarm_id ASC".format(COMPANY_CODE,DATA_SEQ),
    }
    return swichter.get(DATATYPE,None)
class SiteAlarmHrs:
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity):
        log.logger.info('SiteAlarmHrs __init__')
        self.COMPANY_CODE=COMPANY_CODE
        self.SITE=SITE
        self.FACTORY_ID=FACTORY_ID
        self.DATATYPE=DATATYPE
        self.DATA_SEQ=DATA_SEQ
        self.indentity=indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.indentity).READ()
    def GetSiteAlarmHrsData(self):
        try:
            log.logger.info('SiteAlarmHrs GetSiteAlarmHrsData Start')
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
            #datajson=[]
            #for da in data:
            #    datajson.append(Pack(da[0],da[1],da[2],da[3],da[4],da[5]))
            data=json.dumps(datajson,sort_keys=True, indent=2,ensure_ascii=False)
            log.logger.info('Json:\n'+str(data))
            log.logger.info('SiteAlarmHrs GetSiteAlarmHrsData DONE')
            log.logger.info('Route /GetFactoryAlarmHrs DONE')
            return data,200,{"Content-Type": "application/json",'Connection':'close'}
        except Exception as e:
            error_class = e.__class__.__name__ #??????????????????
            detail = e.args[0] #??????????????????
            cl, exc, tb = sys.exc_info() #??????Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #??????Call Stack?????????????????????
            fileName = lastCallStack[0] #???????????????????????????
            lineNum = lastCallStack[1] #?????????????????????
            funcName = lastCallStack[2] #???????????????????????????
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return jsonify({'Result': 'NG','Reason':'{0} erro'.format(funcName)}),400 ,{"Content-Type": "application/json",'Connection':'close'}

