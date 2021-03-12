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
from Logger import Logger
log = Logger('ALL.log',level='debug')
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
def EqpOeeSQL(datatype,companycode,site,factoryid,supply_category,eqp_id,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.hour_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.hour_cd = s.period WHERE hi.hour_seq <= '{5}' order by hi.hour_cd asc".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.day_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.day_cd = s.day_cd WHERE hi.day_seq <= '{5}' order by hi.day_cd asc".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.week_cd as time, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.week_cd = s.week_cd WHERE hi.week_seq <= '{5}' order by hi.week_cd asc".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.month_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.month_cd = s.month_cd WHERE hi.month_seq <= {5} order by hi.month_cd asc".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
    }
    return switcher.get(datatype,None)
def SupplyOeeSQL(datatype,companycode,site,factoryid,supply_category,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT i.company_code,i.site,i.factory_id,i.supply_category, hi.hour_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  and hi.hour_cd = s.period WHERE hi.hour_seq <= '{4}' order by hi.hour_cd asc".format(companycode,site,factoryid,supply_category,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT DISTINCT i.company_code,i.site,i.factory_id,i.supply_category,hi.day_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  AND hi.day_cd = s.day_cd WHERE hi.day_seq <= '{4}' order by hi.day_cd asc".format(companycode,site,factoryid,supply_category,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category, hi.week_cd as time, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category and hi.week_cd = s.week_cd WHERE hi.week_seq <= '{4}' order by hi.week_cd asc".format(companycode,site,factoryid,supply_category,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category, hi.month_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category and hi.month_cd = s.month_cd WHERE hi.month_seq <= {4} order by hi.month_cd asc".format(companycode,site,factoryid,supply_category,int(data_seq)),
    }
    return switcher.get(datatype,None)
def FactoryOeeSQL(datatype,companycode,site,factoryid,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT i.company_code,i.site,i.factory_id, hi.hour_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id=i.factory_id  and hi.hour_cd = s.period WHERE hi.hour_seq <= '{3}' order by hi.hour_cd asc".format(companycode,site,factoryid,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT DISTINCT i.company_code,i.site,hi.day_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND hi.day_cd = s.day_cd WHERE hi.day_seq <= '{3}' order by hi.day_cd asc".format(companycode,site,factoryid,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id, hi.week_cd as time, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id  and hi.week_cd = s.week_cd WHERE hi.week_seq <= '{3}' order by hi.week_cd asc".format(companycode,site,factoryid,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT distinct i.company_code,i.site,i.factory_id, hi.month_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id  and hi.month_cd = s.month_cd WHERE hi.month_seq <= {3} order by hi.month_cd asc".format(companycode,site,factoryid,int(data_seq)),
    }
    return switcher.get(datatype,None)
def SiteOeeSQL(datatype,companycode,site,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT i.company_code,i.site, hi.hour_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site  and hi.hour_cd = s.period WHERE hi.hour_seq <= '{2}' order by hi.hour_cd asc".format(companycode,site,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT DISTINCT i.company_code,i.site,hi.day_cd as TIME,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND hi.day_cd = s.day_cd WHERE hi.day_seq <= '{2}' order by hi.day_cd asc".format(companycode,site,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT distinct i.company_code,i.site, hi.week_cd as time, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site and hi.week_cd = s.week_cd WHERE hi.week_seq <= '{2}' order by hi.week_cd asc".format(companycode,site,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT distinct i.company_code,i.site, hi.month_cd as TIME, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site and hi.month_cd = s.month_cd WHERE hi.month_seq <= {2} order by hi.month_cd asc".format(companycode,site,int(data_seq)),
    }
    return switcher.get(datatype,None)


def EqpAvgSQL(datatype,companycode,site,factoryid,supply_category,eqp_id,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id,hi.hour_cd,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id AND hi.hour_cd = s.period WHERE hi.hour_seq <= {5}) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.day_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.day_cd = s.day_cd WHERE hi.day_seq <= {5} ) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.week_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.week_cd = s.week_cd WHERE hi.week_seq <= {5} ) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category,'{4}' eqp_id FROM DUAL) SELECT company_code,site,factory_id,supply_category,eqp_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category,i.eqp_id, hi.month_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category AND i.eqp_id = s.entity_id and hi.month_cd = s.month_cd WHERE hi.month_seq <= {5}  ) GROUP BY company_code,site,factory_id,supply_category,eqp_id".format(companycode,site,factoryid,supply_category,eqp_id,int(data_seq)),
    }
    return switcher.get(datatype,None)
def SupplyAvgSQL(datatype,companycode,site,factoryid,supply_category,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT i.company_code,i.site,i.factory_id,i.supply_category,hi.hour_cd,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  AND hi.hour_cd = s.period WHERE hi.hour_seq <= {4}) GROUP BY company_code,site,factory_id,supply_category".format(companycode,site,factoryid,supply_category,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category, hi.day_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category and hi.day_cd = s.day_cd WHERE hi.day_seq <= {4} ) GROUP BY company_code,site,factory_id,supply_category".format(companycode,site,factoryid,supply_category,int(data_seq)),#
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category, hi.week_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category  and hi.week_cd = s.week_cd WHERE hi.week_seq <= {4} ) GROUP BY company_code,site,factory_id,supply_category".format(companycode,site,factoryid,supply_category,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id,'{3}' supply_category FROM DUAL) SELECT company_code,site,factory_id,supply_category,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id,i.supply_category, hi.month_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id AND s.SUPPLY_LINE = i.supply_category and hi.month_cd = s.month_cd WHERE hi.month_seq <= {4}  ) GROUP BY company_code,site,factory_id,supply_category".format(companycode,site,factoryid,supply_category,int(data_seq)),
    }
    return switcher.get(datatype,None)
def FactoryAvgSQL(datatype,companycode,site,factoryid,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT i.company_code,i.site,i.factory_id,hi.hour_cd,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id  AND hi.hour_cd = s.period WHERE hi.hour_seq <= {3}) GROUP BY company_code,site,factory_id".format(companycode,site,factoryid,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id, hi.day_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id and hi.day_cd = s.day_cd WHERE hi.day_seq <= {3} ) GROUP BY company_code,site,factory_id".format(companycode,site,factoryid,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id, hi.week_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id and hi.week_cd = s.week_cd WHERE hi.week_seq <= {3} ) GROUP BY company_code,site,factory_id".format(companycode,site,factoryid,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site,'{2}' factory_id FROM DUAL) SELECT company_code,site,factory_id,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site,i.factory_id, hi.month_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site AND s.factory_id = i.factory_id  and hi.month_cd = s.month_cd WHERE hi.month_seq <= {3}  ) GROUP BY company_code,site,factory_id".format(companycode,site,factoryid,int(data_seq)),
    }
    return switcher.get(datatype,None)
def SiteAvgSQL(datatype,companycode,site,data_seq):
    switcher={
        'HourlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT i.company_code,i.site,hi.hour_cd,s.STATUS_RUN,s.STATUS_DOWN,s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE s ON  s.company_code = i.company_code AND s.site=i.site AND hi.hour_cd = s.period WHERE hi.hour_seq <= {2}) GROUP BY company_code,site".format(companycode,site,int(data_seq)),
        'DailyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site, hi.day_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_day s ON s.company_code = i.company_code AND s.site=i.site and hi.day_cd = s.day_cd WHERE hi.day_seq <= {2} ) GROUP BY company_code,site".format(companycode,site,int(data_seq)),
        'WeeklyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site, hi.week_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_week s ON s.company_code = i.company_code AND s.site=i.site and hi.week_cd = s.week_cd WHERE hi.week_seq <= {2} ) GROUP BY company_code,site".format(companycode,site,int(data_seq)),
        'MonthlyByPeriod':"WITH eqp_info AS (SELECT '{0}' company_code,'{1}' site FROM DUAL) SELECT company_code,site,'AVG' TIME,ROUND (AVG (STATUS_RUN), 2) STATUS_RUN,ROUND (AVG (STATUS_DOWN), 2) STATUS_DOWN,ROUND (AVG (STATUS_IDLE), 2) STATUS_IDLE FROM (SELECT distinct i.company_code,i.site, hi.month_cd, s.STATUS_RUN, s.STATUS_DOWN, s.STATUS_IDLE FROM eqp_info i CROSS JOIN dmb_time_dim_v hi LEFT JOIN DMB_DC_ENTITY_STATE_month s ON  s.company_code = i.company_code AND s.site=i.site and hi.month_cd = s.month_cd WHERE hi.month_seq <= {2}  ) GROUP BY company_code,site".format(companycode,site,int(data_seq)),
    }
    return switcher.get(datatype,None)
class OEE:
    def __init__(self,companycode,site,factoryid,supply_category,eqp_id,data_seq,datatype,indentity):
        log.logger.info('OEE __init__')
        self.companycode=companycode
        self.site=site
        self.factoryid=factoryid
        self.supply_category=supply_category
        self.eqp_id=eqp_id
        self.data_seq=data_seq
        self.datatype=datatype
        self.indentity=indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig('config.json',self.indentity).READ()
    def GetOEEData(self):
        try:
            log.logger.info('OEE GetOEEData Start')
            OeeSQL=None
            AgvSQL=None  
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
           
            if len(self.eqp_id.strip())>0:
                OeeSQL=EqpOeeSQL(self.datatype,self.companycode,self.site,self.factoryid,self.supply_category,self.eqp_id,self.data_seq)
                AgvSQL=EqpAvgSQL(self.datatype,self.companycode,self.site,self.factoryid,self.supply_category,self.eqp_id,self.data_seq)
            elif len(self.supply_category.strip())>0:
                OeeSQL=SupplyOeeSQL(self.datatype,self.companycode,self.site,self.factoryid,self.supply_category,self.data_seq)
                AgvSQL=SupplyAvgSQL(self.datatype,self.companycode,self.site,self.factoryid,self.supply_category,self.data_seq)
            elif len(self.factoryid.strip())>0:
                OeeSQL=FactoryOeeSQL(self.datatype,self.companycode,self.site,self.factoryid,self.data_seq)
                AgvSQL=FactoryAvgSQL(self.datatype,self.companycode,self.site,self.factoryid,self.data_seq)
            else:
                OeeSQL=SiteOeeSQL(self.datatype,self.companycode,self.site,self.data_seq)
                AgvSQL=SiteAvgSQL(self.datatype,self.companycode,self.site,self.data_seq)
            log.logger.info('EQ OEE SQL:\n'+OeeSQL)
            log.logger.info('AGV SQL:\n'+AgvSQL)
            daoHelper.Connect()
            EqDes,EqData=daoHelper.SelectAndDescription(OeeSQL)
            AgvDes,AgvData=daoHelper.SelectAndDescription(AgvSQL)
            daoHelper.Close()
            #datajson
            Eq_names = [row[0] for row in EqDes]
            Eqjson=[dict(zip(Eq_names,da)) for da in EqData]

            Agv_names = [row[0] for row in AgvDes]
            Agvjson=[dict(zip(Agv_names,da)) for da in AgvData]
            datajson=Eqjson+Agvjson

            data=json.dumps(datajson,sort_keys=True, indent=2)
            #datajson=[]
            #for da in data:
            #    datajson.append(SwitchType(self.datatype,da))
            #for da1 in avg:
            #    datajson.append(SwitchAVGType(self.datatype,da1))
            # data=json.dumps(datajson, default=lambda o: o.__dict__, sort_keys=True, indent=2)
            log.logger.info("Json \n"+str(data))
            log.logger.info('OEE GetOEEData DONE')
            log.logger.info('Route /GetEQOEE DONE')
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