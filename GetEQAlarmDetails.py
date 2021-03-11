# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
import os
import sys 
import traceback
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
from Logger import Logger
log = Logger('ALL.log',level='debug')

#class Pack:
#    def __init__(self,COMPANY_CODE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,TIME,ALARM_ID,ALARM_DESC,ALARM_TIMES,ALARM_INTERVAL):
#        self.COMPANY_CODE=COMPANY_CODE
#        self.FACTORY_ID=FACTORY_ID
#        self.SUPPLY_CATEGORY=SUPPLY_CATEGORY
#        self.EQP_ID=EQP_ID
#        self.TIME=TIME
#        self.ALARM_ID=ALARM_ID
#        self.ALARM_DESC=ALARM_DESC
#        self.ALARM_TIMES=ALARM_TIMES
#        self.ALARM_INTERVAL=ALARM_INTERVAL
#    def __repr__(self):
#        return repr(self.COMPANY_CODE,self.FACTORY_ID,self.SUPPLY_CATEGORY,self.EQP_ID,self.TIME,self.ALARM_ID,self.ALARM_DESC,self.ALARM_TIMES,self.ALARM_INTERVAL)
def EqpSQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ):
    switcher={
            "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM   dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,i.eqp_id,hi.hour_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi  LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND i.eqp_id = s.entity_id AND hi.hour_cd = s.hour_cd WHERE  hi.hour_seq <= '{5}' ORDER  BY alarm_id,hi.hour_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
            "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM   dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,i.eqp_id,hi.day_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi  LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND i.eqp_id = s.entity_id AND hi.day_cd = s.day_cd  WHERE  hi.day_seq <= '{5}' ORDER BY alarm_id,hi.day_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
            "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM   dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,i.eqp_id,hi.WEEK_CD AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi  LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND i.eqp_id = s.entity_id AND hi.WEEK_CD = s.WEEK_CD WHERE  hi.week_seq <= '{5}' ORDER BY alarm_id,hi.WEEK_CD ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
            "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,i.eqp_id,hi.MONTH_CD AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi  LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND i.eqp_id = s.entity_id AND hi.MONTH_CD = s.MONTH_CD  WHERE  hi.week_seq <= '{5}' ORDER BY alarm_id,hi.MONTH_CD ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def SupplySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM   dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,hi.hour_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND hi.hour_cd = s.hour_cd WHERE  hi.hour_seq <= '{4}' ORDER  BY alarm_id,hi.hour_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,hi.day_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND hi.day_cd = s.day_cd  WHERE  hi.day_seq <= '{4}' ORDER  BY alarm_id,hi.day_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,hi.week_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND hi.week_cd = s.week_cd WHERE  hi.week_seq <= '{4}' ORDER  BY alarm_id,hi.week_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,i.supply_category,hi.MONTH_CD AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.supply_line = i.supply_category AND hi.MONTH_CD = s.MONTH_CD  WHERE  hi.month_seq <= '{4}' ORDER  BY alarm_id,hi.MONTH_CD ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def FactorySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,hi.hour_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.hour_cd = s.hour_cd WHERE  hi.hour_seq <= '{3}' ORDER  BY alarm_id,hi.hour_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,hi.day_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.day_cd = s.day_cd  WHERE  hi.day_seq <= '{3}' ORDER  BY alarm_id,hi.day_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,hi.week_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.week_cd = s.week_cd  WHERE  hi.week_seq <= '{3}' ORDER  BY alarm_id,hi.week_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM  dual) SELECT DISTINCT i.company_code,i.site, i.factory_id,hi.month_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.month_cd = s.month_cd  WHERE  hi.month_seq <= '{3}' ORDER  BY alarm_id,hi.month_cd ASC ".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def SiteSQL(DATATYPE,COMPANY_CODE,SITE,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM  dual) SELECT DISTINCT i.company_code,i.site,hi.hour_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.hour_cd = s.hour_cd  WHERE  hi.hour_seq <= '{2}' ORDER  BY alarm_id,hi.hour_cd ASC ".format(COMPANY_CODE,SITE,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM  dual) SELECT DISTINCT i.company_code,i.site,hi.day_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.day_cd = s.day_cd WHERE  hi.day_seq <= '{2}' ORDER  BY alarm_id,hi.day_cd ASC ".format(COMPANY_CODE,SITE,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM  dual) SELECT DISTINCT i.company_code,i.site,hi.week_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.week_cd = s.week_cd  WHERE  hi.week_seq <= '{2}' ORDER  BY alarm_id,hi.week_cd ASC ".format(COMPANY_CODE,SITE,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM  dual) SELECT DISTINCT i.company_code,i.site,hi.month_cd AS TIME,s.alarm_id,s.alarm_desc, Nvl (s.alarm_times_total, 0) alarm_times, Nvl (s.alarm_interval, 0) alarm_interval FROM eqp_info i CROSS JOIN dmb_time_dim_v hi  LEFT JOIN dmb_dc_agv_alm_hour_items_v s ON s.company_code = i.company_code AND s.site=i.site  AND hi.month_cd = s.month_cd  WHERE  hi.month_seq <= '{2}' ORDER  BY alarm_id,hi.month_cd ASC ".format(COMPANY_CODE,SITE,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
class AlarmDetail():
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity):
        log.logger.info('AlarmDetail __init__')
        self.COMPANY_CODE=COMPANY_CODE
        self.SITE=SITE
        self.FACTORY_ID=FACTORY_ID
        self.SUPPLY_CATEGORY=SUPPLY_CATEGORY
        self.EQP_ID=EQP_ID
        self.DATATYPE=DATATYPE
        self.DATA_SEQ=DATA_SEQ
        self.indentity=indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.indentity).READ()
    def GetAlarmDetailsData(self):
        try:
            log.logger.info('AlarmDetail GetAlarmDetailsData Start')
            sql=None
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            if len(self.EQP_ID.strip())>0:
                sql=EqpSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.SUPPLY_CATEGORY,self.EQP_ID,self.DATA_SEQ)
            elif len(self.SUPPLY_CATEGORY.strip())>0:
                sql= SupplySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.SUPPLY_CATEGORY,self.DATA_SEQ)
            elif len(self.FACTORY_ID.strip())>0:
                sql=FactorySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            else:
                sql=SiteSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.DATA_SEQ)
            log.logger.info('SQL:\n'+sql)
            daoHelper.Connect()     
            description,data=daoHelper.SelectAndDescription(sql)
            daoHelper.Close()
            col_names = [row[0] for row in description]
            datajson=[dict(zip(col_names,da)) for da in data]
            #for da in data:
            ##print(da[6])
            #    datajson.append(Pack(da[0],da[1],da[2],da[3],da[4],da[5],da[6],da[7],da[8]))
            data=json.dumps(datajson, sort_keys=True, indent=2,ensure_ascii=False)
            log.logger.info('Json:\n'+str(data))
            log.logger.info('AlarmDetail GetAlarmDetailsData DONE')
            log.logger.info('Route /GetEQAlarmDetails DONE')
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

