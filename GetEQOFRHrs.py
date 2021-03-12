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

def EqpSQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.hour_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.hour_cd = s.period WHERE hi.hour_seq <= {5}) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.day_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {5}) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.week_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {5}) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.month_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.month_cd = s.month_cd WHERE hi.week_seq <= {5}) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def SupplySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,hi.hour_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  AND hi.hour_cd = s.period WHERE hi.hour_seq <= {4}) GROUP BY company_code,site,factory_id,supply_category".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,hi.day_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {4}) GROUP BY company_code,site,factory_id,supply_category".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,hi.week_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {4}) GROUP BY company_code,site,factory_id,supply_category".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,hi.month_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND hi.month_cd = s.month_cd WHERE hi.week_seq <= {4}) GROUP BY company_code,site,factory_id,supply_category".format(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def FactorySQL(DATATYPE,COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.hour_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.hour_cd = s.period WHERE hi.hour_seq <= {3}) GROUP BY company_code,site,factory_id".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.day_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {3}) GROUP BY company_code,site,factory_id".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.week_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {3}) GROUP BY company_code,site,factory_id".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,i.factory_id,hi.month_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.month_cd = s.month_cd WHERE hi.week_seq <= {3}) GROUP BY company_code,site,factory_id".format(COMPANY_CODE,SITE,FACTORY_ID,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
def SiteSQL(DATATYPE,COMPANY_CODE,SITE,DATA_SEQ):
    switcher={
        "HourlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.hour_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site AND hi.hour_cd = s.period WHERE hi.hour_seq <= {2}) GROUP BY company_code,site".format(COMPANY_CODE,SITE,DATA_SEQ),
        "DailyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.day_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site  AND hi.day_cd = s.day_cd WHERE hi.day_seq <= {2}) GROUP BY company_code,site".format(COMPANY_CODE,SITE,DATA_SEQ),
        "WeeklyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.week_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON  s.company_code = i.company_code AND s.site=i.site  AND hi.week_cd = s.week_cd WHERE hi.week_seq <= {2}) GROUP BY company_code,site".format(COMPANY_CODE,SITE,DATA_SEQ),
        "MonthlyByPeriod":"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,SUM (assignment) assignment,case when SUM (assignment) = 0 then 0 else ROUND (SUM (finish_count) / SUM (assignment) * 100, 2) end as finish_ratio,SUM (finish_count) finish_count FROM (SELECT i.company_code,i.site,hi.month_cd,NVL (s.assignment, 0) AS assignment,NVL (s.finish_ratio, 0) AS finish_ratio,ROUND (NVL (s.assignment, 0) * NVL (s.finish_ratio, 0) / 100,2) AS finish_count FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_AGV_PASS_LOG_V s ON s.company_code = i.company_code AND s.site=i.site AND hi.month_cd = s.month_cd WHERE hi.week_seq <= {2}) GROUP BY company_code,site".format(COMPANY_CODE,SITE,DATA_SEQ),
    }
    return switcher.get(DATATYPE,None)
class EQOFRHrs:
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity):
        log.logger.info('EQOFRHrs __init__')
        self.COMPANY_CODE=COMPANY_CODE
        self.SITE=SITE
        self.FACTORY_ID=FACTORY_ID
        self.SUPPLY_CATEGORY=SUPPLY_CATEGORY
        self.EQP_ID=EQP_ID
        self.DATATYPE=DATATYPE
        self.DATA_SEQ=DATA_SEQ
        self.indentity=indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.indentity).READ()
    def GetEQOFRHrsData(self):
        try:
            log.logger.info('EQOFRHrs GetEQOFRHrsData Start')
            sql=None
            if len(self.EQP_ID.strip())>0:
                sql=EqpSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.SUPPLY_CATEGORY,self.EQP_ID,self.DATA_SEQ)
            elif len(self.SUPPLY_CATEGORY.strip())>0:
                 sql=SupplySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.SUPPLY_CATEGORY,self.DATA_SEQ)
            elif len(self.FACTORY_ID.strip())>0:
                 sql=FactorySQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.DATA_SEQ)
            else:
                sql=SiteSQL(self.DATATYPE,self.COMPANY_CODE,self.SITE,self.DATA_SEQ)

            log.logger.info('SQL:\n'+sql)
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            daoHelper.Connect()  
            description,data=daoHelper.SelectAndDescription(sql)
            daoHelper.Close()

            col_names = [row[0] for row in description]
            datajson=[dict(zip(col_names,da)) for da in data]
            data=json.dumps(datajson,sort_keys=True, indent=2,ensure_ascii=False)
            log.logger.info('Json:\n'+data)
            log.logger.info('EQOFRHrs GetEQOFRHrsData DONE')
            log.logger.info('Route /GetEQOFRHrs DONE')
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