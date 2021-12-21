# -*- coding: utf-8 -*-
#!flask/bin/python
from flask import Flask, jsonify,Blueprint
from flask import request
from flask_restplus import Api, Resource, fields,reqparse, inputs
from flask_sse import sse
import threading,logging,time
import multiprocessing
import json
import redis
import os
import sys
from pymongo import MongoClient
import requests
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from KpiProvider import Kpi
from KpiSiteOEE import KpiSite
from AuthListByAccountApi import AuthListByAccount
#------------舊版---------------------------
from GetEQOEE import OEE
from Getmenu import Menu
from GetEQAlarmHrs import AlarmHrs
from GetEQAlarmDetails import AlarmDetail
from GetFactoryOEE import SiteOEE
from GetFactoryOEEDetail import SiteOEEDetail
from GetFactoryAlarmHrs import SiteAlarmHrs
from GetFactoryAlarmHrsDetail import SiteAlarmHrsDetail
from GetFactoryOFRHrs import FactoryOFRHrs
from GetEQOFRHrs import EQOFRHrs
from GetEQOFRHrsDetails import EQOFRHrsDetails
from GetFactoryOFRHrsDetail import OFRHrsDetail
#---------------------------------------------
from intSDETL import INTSDETL
from GetINTRelation import INTRelation
from GetINTTALK import INTTALK
from GetINTLV3 import INTLV3
from GetINTLV2 import INTLV2
from GetINTKPI import INTKPI
from GetOeeDetails import OeeDetials
from GetOee import Oee
from GetAlarmDetails import AlarmDetails
from GetAlarm import Alarm
from GetOfrDetails import OfrDetails
from GetOfr import Ofr
from GetOeeCompare import OeeCompare
from GetOfrCompare import OfrCompare
from GetRealTimeEvent import RealTimeEvent
from GetStockerStageInfo import StockerStageInfo
from GetStockerInfoSha import StockerInfoSha
from BaseType import BaseType
from GetAppConfSysMain import AppConfSysMain
from GetAgvRouteInfo import AgvRouteInfo
from GetAgvInfo import AgvInfo
from MongoFunction import mongoDbFunction
from MongoFunctionDynamic import mongoDbFunctionDynamic
from RegisterStream import RegisterStreamFuncion
from StreamComponentFunc import streamComponentFunction
from SsePublish import ssePublicFunc
from DataForComponent import dataForComponentFunc
from kafkaReplay import kafkaReplayFunc
from GetVLog import vLogFunc
from GetWipLog import wipLogFunc
from GetWipLogNp import wipLogFuncNp
from GetWipLogNew import wipLogNewFunc
from AlternateForSSE import alternateFunc
from GetFpyLog import fpyLogFunc
from GetFpyLogApplication import fpyLogApplicationFunc
from GetDpsPlain import dpsPlainFunc
from GetSingleCollection import singleCollectionFunc
from SetMongoInserMany import mongoInsertManyFunc
from CompensateDb2 import compensate
from GetWayneTestInfo import WayneTestInfo
from GetiSFPUpphInfo import iSFPUpphInfo
from GetiSFPUpphLightInfo import iSFPUpphLightInfo
from GetiSFPResignInfo import iSFPResignInfo
from GetiSFPAttendanceInfo import iSFPAttendanceInfo
from GetiSFPReAtInfo import iSFPReAtInfo
from GetiSFPScrapLightInfo import iSFPScrapLightInfo
from GetiSFPScrapInfo import iSFPScrapInfo
from GetiSFPWOInfo import iSFPWOInfo
from GetiSFPWIP30Info import iSFPWIP30Info
from GetiSFPWIP14Info import iSFPWIP14Info
from GetiSFPWOWIPInfo import iSFPWOWIPInfo
from GetiSFPOutLightInfo import iSFPOutLightInfo
from GetiSFPOutputInfo import iSFPOutputInfo
from GetiSFPReachInfo import iSFPReachInfo
from GetiSFPTDInputInfo import iSFPTDInputInfo
from GetiSFPOEEInfo import iSFPOEEInfo
from GetiSFPFPYYLightInfo import iSFPFPYYLightInfo
from GetiSFPFPYYInfo import iSFPFPYYInfo
from GetiSFPFPYScrapInfo import iSFPFPYScrapInfo
from GetiSFPOQCLightInfo import iSFPOQCLightInfo
from GetiSFPOQCInfo import iSFPOQCInfo
from GetiSFPHotIPQAInfo import iSFPHotIPQAInfo
from GetiSFPHoldLightInfo import iSFPHoldLightInfo
from GetiSFPHoldInfo import iSFPHoldInfo
from GetiSFPScrapPInfo import iSFPScrapPInfo
from GetiSFPFPYNLightInfo import iSFPFPYNLightInfo
from GetiSFPFPYNInfo import iSFPFPYNInfo

os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
from Logger import Logger
log = Logger('./log/Main.log',level='debug')
app = Flask(__name__ )

_dbAccount, _dbPassword, _SERVICE_NAME = ReadConfig('config.json',"INT_ORACLEDB_TEST").READ()
_daoHelper = DaoHelper(_dbAccount, _dbPassword, _SERVICE_NAME)
_db_pool= _daoHelper.poolCreate()

# redis路径
app.config["REDIS_URL"] = "redis://idts-kafka1.cminl.oa"
# app注册sse的蓝图,并且访问路由是/stream1
app.register_blueprint(sse, url_prefix='/stream1')
api = Api(app, version='1.0', title='DataProvider API',
    description='KPI資料提供'
)
#app.register_blueprint(blueprint)

CORS(app, supports_credentials=True,cors_allowed_origins='*')
#@app.route('/mb/api/DataProviderApi', methods=['POST'])
#def create_task():
#    if not request:
#        abort(400)
#    condition=""
#    rcondition=""
#    jsonData =request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "MAPID" not in jsonData  or 'FACTORY_ID' not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /mb/api/DataProviderApi Start')
#    log.logger.info('Request Json: '+str(jsonData))
#    companycode=jsonData['COMPANY_CODE']
#    site=jsonData['SITE']
#    mapid=jsonData['MAPID']
#    lineid=jsonData['LINEID']
#    factoryid=jsonData['FACTORY_ID']
#    ##抓取config.json資料庫帳密
#    identity = 'INX-TN-J001'
#    #Oracle and Redis 條件式
#    if factoryid is not "":
#        condition=" and factory_id='{0}'".format(factoryid)
#        rcondition="-"+factoryid
#    if lineid is not "":
#        condition+=" and lineid='{0}'".format(lineid)
#        rcondition+="-"+lineid
#    kpi=Kpi(identity,companycode,site,mapid,lineid,factoryid,condition,rcondition)
#    return kpi.GetJsonData()
##-------------------舊版------------------------------------------------------
#@app.route('/KpiSiteOEE', methods=['POST'])
#def KpiSietOee():
#    if not request:
#        abort(400)
#    log.logger.info('Route /KpiSiteOEE Start')
#    identity = 'INX-TN-J001'
#    kpisite=KpiSite(identity)
#    return kpisite.GetJsonData()
#@app.route('/GetAuthListByAccount', methods=['POST'])
#def GetAuthListByAccount():
#    if not request:
#        abort(400)
#    jsonData =request.json
#    if "ACCOUNT" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetAuthListByAccount Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    identity = 'IAMP'
#    account=jsonData["ACCOUNT"]
#    test=AuthListByAccount(account,identity)
#    return test.GetDataJson()
#@app.route('/GetEQOEE', methods=['POST'])
#def getEQOEE():
#    if not request:
#        abort(400)
#    jsonData =request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetEQOEE Start')
#    log.logger.info('Request Json: '+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    SUPPLY_CATEGORY=jsonData["SUPPLY_CATEGORY"]
#    EQP_ID=jsonData["EQP_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    oee=OEE(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATA_SEQ,DATATYPE,indentity)
#    return oee.GetOEEData()
#MenuNS = api.namespace('GetMenu', description = '選單')
#MenuML = api.model('GetMenu', {
#    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
#    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
#    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
#    'SUPPLY_LINE': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
#    'TYPE': fields.String( required = True, description = 'TYPE', default = 'AGV', example = 'AGV'),
#})
#@MenuNS.route('', methods = ['POST'])
#@MenuNS.response(200, 'Sucess')
#@MenuNS.response(201, 'Created Sucess')
#@MenuNS.response(204, 'No Content')
#@MenuNS.response(400, 'Bad Request')
#@MenuNS.response(401, 'Unauthorized')
#@MenuNS.response(403, 'Forbidden')
#@MenuNS.response(404, 'Not Found')
#@MenuNS.response(405, 'Method Not Allowed')
#@MenuNS.response(409, 'Conflict')
#@MenuNS.response(500, 'Internal Server Error')
#class getMenu(Resource):
#    @MenuNS.doc('Get Menu')
#    @MenuNS.expect(MenuML)
#    def post(self):
#        if not request:
#            abort(400)
#        elif not request.json:
#           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
#        jsonData = request.json
#        if "COMPANY_CODE" not in jsonData or "FACTORY_ID" not in jsonData or "SITE" not in jsonData or "SUPPLY_LINE" not in jsonData or "TYPE" not in jsonData:
#            {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
#        COMPANY_CODE = jsonData["COMPANY_CODE"]
#        FACTORY_ID = jsonData["FACTORY_ID"]
#        SITE = jsonData["SITE"]
#        SUPPLY_LINE = jsonData["SUPPLY_LINE"]
#        TYPE = jsonData["TYPE"]
#        indentity="IAMP"
#        menu = Menu(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_LINE,TYPE,indentity)
#        return menu.getData()
#@app.route('/GetEQAlarmHrs', methods=['POST'])
#def getEQAlarmHrs():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetEQAlarmHrs Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    SUPPLY_CATEGORY=jsonData["SUPPLY_CATEGORY"]
#    EQP_ID=jsonData["EQP_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    alarm=AlarmHrs(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity)
#    return alarm.GetAlarmHrsData()
#@app.route('/GetEQAlarmDetails', methods=['POST'])
#def getEQAlarmDetails():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetEQAlarmDetails Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    SUPPLY_CATEGORY=jsonData["SUPPLY_CATEGORY"]
#    EQP_ID=jsonData["EQP_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    alarmdatail=AlarmDetail(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity)
#    return alarmdatail.GetAlarmDetailsData()

#@app.route('/GetFactoryOEE', methods=['POST'])
#def getFactoryOEE():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATATYPE" not in jsonData or"DATA_SEQ" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryOEE Start')
#    log.logger.info('Request Json: '+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    siteOEE=SiteOEE(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity)
#    return siteOEE.GetSiteOEEData()
#@app.route('/GetFactoryOEEDetails', methods=['POST'])
#def getFactoryOEEDetails():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATA_SEQ" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryOEEDetails Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    siteOEEDetail=SiteOEEDetail(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity)
#    return siteOEEDetail.GetSiteOEEDetailData()

#@app.route('/GetFactoryAlarmHrs', methods=['POST'])
#def getFactoryAlarmHrs():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or"DATA_SEQ" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryAlarmHrs Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    siteAlarmHrs=SiteAlarmHrs(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity)
#    return siteAlarmHrs.GetSiteAlarmHrsData()
#@app.route('/GetFactoryAlarmHrsDetail', methods=['POST'])
#def getSiteAlarmHrsDetail():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATA_SEQ" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryAlarmHrsDetail Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    siteAlarmHrsDetail=SiteAlarmHrsDetail(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity)
#    return siteAlarmHrsDetail.GetSiteAlarmHrsDetailData()
#@app.route('/GetFactoryOFRHrs', methods=['POST'])
#def getFactoryOFRHrs():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATA_SEQ" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryOFRHrs Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    factoryOFR=FactoryOFRHrs(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,indentity)
#    return factoryOFR.FactoryOFRHrsData()
#@app.route('/GetEQOFRHrs', methods=['POST'])
#def getEQOFRHrs():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATA_SEQ" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetEQOFRHrs Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    SUPPLY_CATEGORY=jsonData["SUPPLY_CATEGORY"]
#    EQP_ID=jsonData["EQP_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    eqHrs=EQOFRHrs(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity)
#    return eqHrs.GetEQOFRHrsData()
#@app.route('/GetEQOFRHrsDetails', methods=['POST'])
#def getEQOFRHrsDetails():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATA_SEQ" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetEQOFRHrsDetails Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    SUPPLY_CATEGORY=jsonData["SUPPLY_CATEGORY"]
#    EQP_ID=jsonData["EQP_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    indentity="IAMP"
#    eqHrsDetail=EQOFRHrsDetails(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID,DATATYPE,DATA_SEQ,indentity)
#    return eqHrsDetail.GetEQOFRHrsData()
#@app.route('/GetFactoryOFRHrsDetail', methods=['POST'])
#def getFactoryOFRHrsDetail():
#    if not request:
#        abort(400)
#    jsonData=request.json
#    if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
#        return jsonify({'Result': 'NG','Reason':''}), 401
#    log.logger.info('Route /GetFactoryOFRHrsDetail Start')
#    log.logger.info('Request Json:'+str(jsonData))
#    COMPANY_CODE=jsonData["COMPANY_CODE"]
#    SITE=jsonData["SITE"]
#    FACTORY_ID=jsonData["FACTORY_ID"]
#    DATATYPE=jsonData["DATATYPE"]
#    DATA_SEQ=jsonData["DATA_SEQ"]
#    ofr=OFRHrsDetail(COMPANY_CODE,SITE,FACTORY_ID,DATATYPE,DATA_SEQ,"IAMP")
#    return ofr.GetOFRHrsDetailData()
#-------------------------------------------------------------
#-------------------------新版--------------------------------
OeeDetailsNS = api.namespace('GetOeeDetails', description = '稼動率細項資料')
OeeDetailsML = api.model('GetOeeDetails', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),
})
parser = reqparse.RequestParser()
parser.add_argument('COMPANY_CODE', type = str,
                    default = "INX", required = True)
parser.add_argument('SITE', type = str,
                    default = "TN", required = True)
parser.add_argument('FACTORY_ID', type = str,
                    default = "J001", required = True)
parser.add_argument('SUPPLY_CATEGORY', type = str,
                    default = "CELL", required = True)
parser.add_argument('EQP_ID', type = str,
                    default = "LB-3001", required = True)
parser.add_argument('DATATYPE', type = str,
                    default = "HourlyByPeriod", required = True)
parser.add_argument('DATA_SEQ', type = str,
                    default = "3", required = True)
@OeeDetailsNS.route('', methods = ['POST','GET'])
@OeeDetailsNS.response(200, 'Sucess')
@OeeDetailsNS.response(201, 'Created Sucess')
@OeeDetailsNS.response(204, 'No Content')
@OeeDetailsNS.response(400, 'Bad Request')
@OeeDetailsNS.response(401, 'Unauthorized')
@OeeDetailsNS.response(403, 'Forbidden')
@OeeDetailsNS.response(404, 'Not Found')
@OeeDetailsNS.response(405, 'Method Not Allowed')
@OeeDetailsNS.response(409, 'Conflict')
@OeeDetailsNS.response(500, 'Internal Server Error')
class getOeeDetails(Resource):
    @OeeDetailsNS.doc('Provide OeeDetails[POST]')
    @OeeDetailsNS.expect(OeeDetailsML)  
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)

        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
 
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')

        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        oeeDetails = OeeDetials(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, identity)
        return oeeDetails.getData()
    @OeeDetailsNS.doc('Provide OeeDetails[GET]')
    @OeeDetailsNS.expect(parser) 
    def get(self):
        if not request:
            abort(400)
        if "COMPANY_CODE" not in request.args or "SITE" not in request.args or "FACTORY_ID" not in request.args or "SUPPLY_CATEGORY" not in request.args or "EQP_ID" not in request.args or "DATATYPE" not in request.args or "DATA_SEQ" not in request.args:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        COMPANY_CODE = request.args["COMPANY_CODE"]
        SITE = request.args["SITE"]
        FACTORY_ID = request.args["FACTORY_ID"]
        SUPPLY_CATEGORY = request.args["SUPPLY_CATEGORY"]
        EQP_ID = request.args["EQP_ID"]
        DATATYPE = request.args["DATATYPE"]
        DATA_SEQ = request.args["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        oeeDetails = OeeDetials(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, identity)
        return oeeDetails.getData()
OeeNs = api.namespace('GetOee', description = '稼動率')
OeeML = api.model('GetOee', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@OeeNs.route('', methods = ['POST'])
@OeeNs.response(200, 'Sucess')
@OeeNs.response(201, 'Created Sucess')
@OeeNs.response(204, 'No Content')
@OeeNs.response(400, 'Bad Request')
@OeeNs.response(401, 'Unauthorized')
@OeeNs.response(403, 'Forbidden')
@OeeNs.response(404, 'Not Found')
@OeeNs.response(405, 'Method Not Allowed')
@OeeNs.response(409, 'Conflict')
@OeeNs.response(500, 'Internal Server Error')
class getOee(Resource):
    @OeeNs.doc('Provide Oee')
    @OeeNs.expect(OeeML) 
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')

        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        oee = Oee(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, identity)
        return oee.getData()

OeeCompareNs = api.namespace('GetOeeCompare', description = '稼動率比較')
OeeCompareML = api.model('GetOeeCompare', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID':fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY':fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@OeeCompareNs.route('', methods = ['POST'])
@OeeCompareNs.response(200, 'Sucess')
@OeeCompareNs.response(201, 'Created Sucess')
@OeeCompareNs.response(204, 'No Content')
@OeeCompareNs.response(400, 'Bad Request')
@OeeCompareNs.response(401, 'Unauthorized')
@OeeCompareNs.response(403, 'Forbidden')
@OeeCompareNs.response(404, 'Not Found')
@OeeCompareNs.response(405, 'Method Not Allowed')
@OeeCompareNs.response(409, 'Conflict')
@OeeCompareNs.response(500, 'Internal Server Error')
class getOeeCompare(Resource):
    @OeeCompareNs.doc('Provide OeeCompare')
    @OeeCompareNs.expect(OeeCompareML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in  jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        FACTORY_ID = jsonData["FACTORY_ID"]
    
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        oee = OeeCompare(COMPANY_CODE,SITE,SUPPLY_CATEGORY,FACTORY_ID,TIME_DIM,DATA_SEQ,identity)
        return oee.getData()

AlarmDetailsNs = api.namespace('GetAlarmDetails', description = '異常率細項資料')
AlarmDetailsML = api.model('GetAlarmDetails', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@AlarmDetailsNs.route('', methods = ['POST'])
@AlarmDetailsNs.response(200, 'Sucess')
@AlarmDetailsNs.response(201, 'Created Sucess')
@AlarmDetailsNs.response(204, 'No Content')
@AlarmDetailsNs.response(400, 'Bad Request')
@AlarmDetailsNs.response(401, 'Unauthorized')
@AlarmDetailsNs.response(403, 'Forbidden')
@AlarmDetailsNs.response(404, 'Not Found')
@AlarmDetailsNs.response(405, 'Method Not Allowed')
@AlarmDetailsNs.response(409, 'Conflict')
@AlarmDetailsNs.response(500, 'Internal Server Error')
class getAlarmDetails(Resource):
    @AlarmDetailsNs.doc('Provide AlarmDetails')
    @AlarmDetailsNs.expect(AlarmDetailsML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE,FACTORY_ID, SUPPLY_CATEGORY, EQP_ID)
        TIME_DIM = BaseType.TimeDimForNew(DATATYPE)
        TOP = jsonData.get("TOP",10)
        identity = "IAMP"
        alarmDetails = AlarmDetails(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, TOP ,identity)
        return alarmDetails.getData()

AlarmNs = api.namespace('GetAlarm', description = '異常率')
AlarmML = api.model('GetAlarm', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@AlarmNs.route('', methods = ['POST'])
@AlarmNs.response(200, 'Sucess')
@AlarmNs.response(201, 'Created Sucess')
@AlarmNs.response(204, 'No Content')
@AlarmNs.response(400, 'Bad Request')
@AlarmNs.response(401, 'Unauthorized')
@AlarmNs.response(403, 'Forbidden')
@AlarmNs.response(404, 'Not Found')
@AlarmNs.response(405, 'Method Not Allowed')
@AlarmNs.response(409, 'Conflict')
@AlarmNs.response(500, 'Internal Server Error')
class getAlarm(Resource):
    @AlarmNs.doc('Provide Alarm')
    @AlarmNs.expect(AlarmML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')

        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        TOP = jsonData.get("TOP",10)
        identity = "IAMP"
        alarm = Alarm(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, TOP,identity)
        return alarm.getData()

OfrDetailsNs = api.namespace('GetOfrDetails', description = '達交率細項資料')
OfrDetailsML = api.model('GetOfrDetails', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@OfrDetailsNs.route('', methods = ['POST'])
@OfrDetailsNs.response(200, 'Sucess')
@OfrDetailsNs.response(201, 'Created Sucess')
@OfrDetailsNs.response(204, 'No Content')
@OfrDetailsNs.response(400, 'Bad Request')
@OfrDetailsNs.response(401, 'Unauthorized')
@OfrDetailsNs.response(403, 'Forbidden')
@OfrDetailsNs.response(404, 'Not Found')
@OfrDetailsNs.response(405, 'Method Not Allowed')
@OfrDetailsNs.response(409, 'Conflict')
@OfrDetailsNs.response(500, 'Internal Server Error')
class getOfrDetails(Resource):
    @OfrDetailsNs.doc('Provide OfrDetails')
    @OfrDetailsNs.expect(OfrDetailsML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
       
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE,FACTORY_ID,SUPPLY_CATEGORY,EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        ofrDetails = OfrDetails(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, identity)
        return ofrDetails.getData()

OfrCompareNs = api.namespace('GetOfrCompare', description = '達交率比較')
OfrCompareML = api.model('GetOfrCompare', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@OfrCompareNs.route('', methods = ['POST'])
@OfrCompareNs.response(200, 'Sucess')
@OfrCompareNs.response(201, 'Created Sucess')
@OfrCompareNs.response(204, 'No Content')
@OfrCompareNs.response(400, 'Bad Request')
@OfrCompareNs.response(401, 'Unauthorized')
@OfrCompareNs.response(403, 'Forbidden')
@OfrCompareNs.response(404, 'Not Found')
@OfrCompareNs.response(405, 'Method Not Allowed')
@OfrCompareNs.response(409, 'Conflict')
@OfrCompareNs.response(500, 'Internal Server Error')
class getOfrCompare(Resource):
    @OfrCompareNs.doc('Provide OfrCompare')
    @OfrCompareNs.expect(OfrCompareML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
       
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData  or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        oee = OfrCompare(COMPANY_CODE, SITE, TIME_DIM, DATA_SEQ, identity)
        return oee.getData()

OfrNs = api.namespace('GetOfr', description='達交率')
OfrML = api.model('GetOfr', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_CATEGORY': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'EQP_ID': fields.String( required = True, description = 'EQP_ID', default = 'LB-3001', example = 'LB-3001'),
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'HourlyByPeriod', example = 'HourlyByPeriod'),
    'DATA_SEQ': fields.String( required = True, description = 'DATA_SEQ', default = '3', example = '3'),

})
@OfrNs.route('', methods=['POST'])
@OfrNs.response(200, 'Sucess')
@OfrNs.response(201, 'Created Sucess')
@OfrNs.response(204, 'No Content')
@OfrNs.response(400, 'Bad Request')
@OfrNs.response(401, 'Unauthorized')
@OfrNs.response(403, 'Forbidden')
@OfrNs.response(404, 'Not Found')
@OfrNs.response(405, 'Method Not Allowed')
@OfrNs.response(409, 'Conflict')
@OfrNs.response(500, 'Internal Server Error')
class getOfr(Resource):
    @OfrNs.doc('Provide Ofr')
    @OfrNs.expect(OfrML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "SUPPLY_CATEGORY" not in jsonData or "EQP_ID" not in jsonData or "DATATYPE" not in jsonData or "DATA_SEQ" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        SITE = jsonData["SITE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SUPPLY_CATEGORY = jsonData["SUPPLY_CATEGORY"]
        EQP_ID = jsonData["EQP_ID"]
        DATATYPE = jsonData["DATATYPE"]
        DATA_SEQ = jsonData["DATA_SEQ"]
        SPACE_DIM = BaseType.SpaceDim(SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID)
        TIME_DIM = BaseType.TimeDim(DATATYPE)
        identity = "IAMP"
        ofr = Ofr(COMPANY_CODE, SITE, FACTORY_ID, SUPPLY_CATEGORY, EQP_ID, SPACE_DIM, TIME_DIM, DATA_SEQ, identity)
        return ofr.getData()

StockerEventNs = api.namespace('StockerEvent', description = 'StockerEvent')
StockerEventML = api.model('StockerEvent', {
    'COMPANYCODE': fields.String( required = True, description = 'COMPANYCODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'MAPID': fields.String( required = True, description = 'MAPID', default = 'J001-ALL', example = 'J001-ALL'),
    'TYPE': fields.String( required = True, description = 'TYPE', default = 'StockerShelfInfo', example = 'StockerShelfInfo'),
})
@StockerEventNs.route('', methods = ['POST'])
@StockerEventNs.response(200, 'Sucess')
@StockerEventNs.response(201, 'Created Sucess')
@StockerEventNs.response(204, 'No Content')
@StockerEventNs.response(400, 'Bad Request')
@StockerEventNs.response(401, 'Unauthorized')
@StockerEventNs.response(403, 'Forbidden')
@StockerEventNs.response(404, 'Not Found')
@StockerEventNs.response(405, 'Method Not Allowed')
@StockerEventNs.response(409, 'Conflict')
@StockerEventNs.response(500, 'Internal Server Error')
class stockerStageInfo(Resource):
    @StockerEventNs.doc('Provide StockerEvent')
    @StockerEventNs.expect(StockerEventML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANYCODE" not in jsonData or "SITE" not in jsonData or "MAPID" not in jsonData or "TYPE" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANYCODE = jsonData["COMPANYCODE"]
        SITE = jsonData["SITE"]
        MAPID = jsonData["MAPID"]
        TYPE = jsonData["TYPE"]
        stageInfo = StockerStageInfo(COMPANYCODE, SITE, MAPID, TYPE)
        return stageInfo.getData()
StockerEventShaNs = api.namespace('StockerEventSha', description='StockerEventSha')
StockerEventShaML = api.model('StockerEventSha', {
    'COMPANYCODE': fields.String( required = True, description = 'COMPANYCODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'MAPID': fields.String( required = True, description = 'MAPID', default = 'J001-ALL', example = 'J001-ALL'),
    'TYPE': fields.String( required = True, description = 'TYPE', default = 'StockerShelfInfo', example = 'StockerShelfInfo'),
})
@StockerEventShaNs.route('', methods = ['POST'])
@StockerEventShaNs.response(200, 'Sucess')
@StockerEventShaNs.response(201, 'Created Sucess')
@StockerEventShaNs.response(204, 'No Content')
@StockerEventShaNs.response(400, 'Bad Request')
@StockerEventShaNs.response(401, 'Unauthorized')
@StockerEventShaNs.response(403, 'Forbidden')
@StockerEventShaNs.response(404, 'Not Found')
@StockerEventShaNs.response(405, 'Method Not Allowed')
@StockerEventShaNs.response(409, 'Conflict')
@StockerEventShaNs.response(500, 'Internal Server Error')
class stockerInfoSha(Resource):
    @StockerEventShaNs.doc('Provide StockerEventSha')
    @StockerEventShaNs.expect(StockerEventShaML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
      
        if "COMPANYCODE" not in jsonData or "SITE" not in jsonData or "MAPID" not in jsonData or "TYPE" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        log.logger.info(f'Input Json:\n {jsonData}')
        COMPANYCODE = jsonData["COMPANYCODE"]
        SITE = jsonData["SITE"]
        MAPID = jsonData["MAPID"]
        TYPE = jsonData["TYPE"]
        InfoSha = StockerInfoSha(COMPANYCODE, SITE, MAPID, TYPE)
        return InfoSha.getData()
RealTimeEventNs = api.namespace('RealTimeEvent', description='RealTimeEvent')
RealTimeEventML = api.model('RealTimeEvent', {
    'COMPANYCODE': fields.String( required = True, description = 'COMPANYCODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'MAPID': fields.String( required = True, description = 'MAPID', default = 'J001-ALL', example = 'J001-ALL'),
    'MESSAGETYPE': fields.String( required = True, description = 'MESSAGETYPE', default = 'EntityStateSlim', example = 'EntityStateSlim'),
})
@RealTimeEventNs.route('', methods = ['POST'])
@RealTimeEventNs.response(200, 'Sucess')
@RealTimeEventNs.response(201, 'Created Sucess')
@RealTimeEventNs.response(204, 'No Content')
@RealTimeEventNs.response(400, 'Bad Request')
@RealTimeEventNs.response(401, 'Unauthorized')
@RealTimeEventNs.response(403, 'Forbidden')
@RealTimeEventNs.response(404, 'Not Found')
@RealTimeEventNs.response(405, 'Method Not Allowed')
@RealTimeEventNs.response(409, 'Conflict')
@RealTimeEventNs.response(500, 'Internal Server Error')
class realTimeEvent(Resource):
    @RealTimeEventNs.doc('Provide RealTimeEvent')
    @RealTimeEventNs.expect(RealTimeEventML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
       
        jsonData = BaseType.validateType(request.json)
       
        if "COMPANYCODE" not in jsonData or "SITE" not in jsonData or "MAPID" not in jsonData or "MESSAGETYPE" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        #log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        #log.logger.info(f'Input Json:\n {jsonData}')
        COMPANYCODE = jsonData["COMPANYCODE"]
        SITE = jsonData["SITE"]
        MAPID = jsonData["MAPID"]
        MESSAGETYPE = jsonData["MESSAGETYPE"]
        timeEvent = RealTimeEvent(COMPANYCODE, SITE, MAPID, MESSAGETYPE)
        return timeEvent.getData()
@app.route('/TestSamGdpApiConsumeTime', methods=['POST'])
def TestSamGdpApiConsumeTime():
    if not request:
        abort(400)
    if not request.json:
        return jsonify({'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}), 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
    tStart = time.time()#計時開始     

    jsonData=None
    if type(request.json) is  dict:
       jsonData = request.json
    elif type(request.json) is  list:
       jsonData = request.json[0]
    #else:
    #    return jsonify({'Result': 'NG','Reason':"Data type Error is '{0}' ".format(type(request.json).__name__)}), 400,{"Content-Type": "application/json",'Connection':'close'}
    if "Token" not in jsonData :
        return jsonify({'Result': 'NG', 'Reason': 'Miss Parameter'}), 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
    datajson=json.dumps({"SysID":"IAMP","Token":jsonData["Token"],"PropertyIDs": ["org", "func"]})
    headers = {'Content-type': 'application/json','Connection':'close'}

    response = requests.post("http://tsamv4atho.cminl.oa/api/Main/GetDataPropertyArrayValues", data=datajson, headers=headers)
    tEnd = time.time()#計時結束
    return jsonify({'SamConsumeTime':round((tEnd - tStart)*1000)}), 200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

appConfSysMainNS = api.namespace('getAppConfSysMain', description = 'AppConfSysMain')
appConfSysMainML = api.model('getAppConfSysMain', {})
@appConfSysMainNS.route('', methods = ['POST'])
@appConfSysMainNS.response(200, 'Sucess')
@appConfSysMainNS.response(201, 'Created Sucess')
@appConfSysMainNS.response(204, 'No Content')
@appConfSysMainNS.response(400, 'Bad Request')
@appConfSysMainNS.response(401, 'Unauthorized')
@appConfSysMainNS.response(403, 'Forbidden')
@appConfSysMainNS.response(404, 'Not Found')
@appConfSysMainNS.response(405, 'Method Not Allowed')
@appConfSysMainNS.response(409, 'Conflict')
@appConfSysMainNS.response(500, 'Internal Server Error')
class getAppConfSysMain(Resource):
    @appConfSysMainNS.doc('AppConfSysMain')
    @appConfSysMainNS.expect(appConfSysMainML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        conf = AppConfSysMain(request.json)
        return conf.getData()

agvRouteInfoNS = api.namespace('GetAgvRouteInfo', description = 'AgvRouteInfo')
agvRouteInfoML = api.model('GetAgvRouteInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
})
@agvRouteInfoNS.route('', methods = ['POST'])
@agvRouteInfoNS.response(200, 'Sucess')
@agvRouteInfoNS.response(201, 'Created Sucess')
@agvRouteInfoNS.response(204, 'No Content')
@agvRouteInfoNS.response(400, 'Bad Request')
@agvRouteInfoNS.response(401, 'Unauthorized')
@agvRouteInfoNS.response(403, 'Forbidden')
@agvRouteInfoNS.response(404, 'Not Found')
@agvRouteInfoNS.response(405, 'Method Not Allowed')
@agvRouteInfoNS.response(409, 'Conflict')
@agvRouteInfoNS.response(500, 'Internal Server Error')
class getAgvRouteInfo(Resource):
    @agvRouteInfoNS.doc('AppConfSysMain')
    @agvRouteInfoNS.expect(agvRouteInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        agvRouteInfo = AgvRouteInfo(identity)
        return agvRouteInfo.getData()

agvInfoNS = api.namespace('GetAgvInfo', description = 'AgvInfo')
agvInfoML = api.model('GetAgvInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
})
@agvInfoNS.route('', methods = ['POST'])
@agvInfoNS.response(200, 'Sucess')
@agvInfoNS.response(201, 'Created Sucess')
@agvInfoNS.response(204, 'No Content')
@agvInfoNS.response(400, 'Bad Request')
@agvInfoNS.response(401, 'Unauthorized')
@agvInfoNS.response(403, 'Forbidden')
@agvInfoNS.response(404, 'Not Found')
@agvInfoNS.response(405, 'Method Not Allowed')
@agvInfoNS.response(409, 'Conflict')
@agvInfoNS.response(500, 'Internal Server Error')
class getAgvInfo(Resource):
    @agvInfoNS.doc('AppConfSysMain')
    @agvInfoNS.expect(agvInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        agvInfo = AgvInfo(identity)
        return agvInfo.getData()
                
detailsData = api.model('mongoDataDetailsData',{
    "TEST":fields.String( required = True, description = 'TEST', default = '123', example = '123'),
})
mongoDataDetails = api.model('mongoDataDetails',{
    "TYPE":fields.String( required = True, description = 'TYPE', default = 'SELECT', example = 'SELECT'),
    "DATA":fields.Nested(detailsData),
})
mongoData = api.model('mongoData', {  
      "cssConfig":fields.Nested(mongoDataDetails),
      "pageConfig":fields.Nested(mongoDataDetails),        
})
mongoNS = api.namespace('MongoFunction', description = 'MongoFunction')
mongoML = api.model('MongoFunction', {
    "DBNAME":fields.String( required = True, description = 'DBNAME', default = 'DCS', example = 'DCS'),
    "KEYNAME":fields.String( required = True, description = 'KEYNAME', default = 'AGV_REPORT', example = 'AGV'),
    "USERID":fields.String( required = True, description = 'USERID', default = 'USERID', example = 'JACK'),
    "COLLECTION":fields.Nested(mongoData),
    
})
@mongoNS.route('', methods = ['POST'])
@mongoNS.response(200, 'Sucess')
@mongoNS.response(201, 'Created Sucess')
@mongoNS.response(204, 'No Content')
@mongoNS.response(400, 'Bad Request')
@mongoNS.response(401, 'Unauthorized')
@mongoNS.response(403, 'Forbidden')
@mongoNS.response(404, 'Not Found')
@mongoNS.response(405, 'Method Not Allowed')
@mongoNS.response(409, 'Conflict')
@mongoNS.response(500, 'Internal Server Error')
class mongoFunction(Resource):
    @mongoNS.doc('MongoFunction')
    @mongoNS.expect(mongoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "DBNAME" not in jsonData or "COLLECTION" not in jsonData or "KEYNAME" not in jsonData or "USERID" not in jsonData : 
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        
        DBNAME = jsonData["DBNAME"]
        COLLECTION = jsonData["COLLECTION"]
        KEYNAME = jsonData["KEYNAME"]
        USERID = jsonData["USERID"]
        mongo = mongoDbFunction(DBNAME, COLLECTION, KEYNAME, USERID)
        return mongo.getData()

mongoDynamicParmCriteriaCondition= api.model('mongoDynamicParmCriteriaCondition', {
    "_id":fields.String(required = True, description = 'KEYNAME@USERID', default = 'AGV@JACK', example = 'AGV@JACK'),
    "USERID":fields.String(required = True, description = 'USERID', default = 'JACK', example = 'JACK'),
    "KEYNAME":fields.String(required = True, description = 'KEYNAME', default = 'AGV', example = 'AGV'),
})
mongoDynamicParmCriteriaInnerCondition= api.model('mongoDynamicParmCriteriaInnerCondition', {
    
})
mongoDynamicParmCriteriaProjection= api.model('mongoDynamicParmCriteriaProjection', {
    "_id":fields.Boolean(required = True, description = '_id', default = 'False', example = 'False')
})
mongoDynamicParmCriteriaData= api.model('mongoDynamicParmCriteriaData', {
})
mongoDynamicParmCriteria= api.model('mongoDynamicParmCriteria', {
    "CONDITION":fields.Nested(mongoDynamicParmCriteriaCondition),
    "INNERCONDITION":fields.Nested(mongoDynamicParmCriteriaInnerCondition),
    "PROJECTION":fields.Nested(mongoDynamicParmCriteriaProjection),
    "ISDOT":  fields.String(required = True, description = 'ISDOT', default = 'N', example = 'N'),  
    "DATA":fields.Nested(mongoDynamicParmCriteriaData),
})
mongoDynamicParmSetting = api.model('mongoDynamicParmSetting', {
    "IP":fields.String(required = True, description = 'IP', default = '10.55.8.62', example = '10.55.8.62'),
    "PORT":fields.String(required = True, description = 'PORT', default = '27017', example = '27017'),
    "DB":fields.String(required = True, description = 'DB', default = 'DCS', example = 'DCS'),
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'pageConfig', example = 'pageConfig'),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'SELECT', example = 'SELECT'),
})
mongoDynamicParm = api.model('mongoDynamicParm', {
    "SETTING":fields.Nested(mongoDynamicParmSetting),
    "CRITERIA":fields.Nested(mongoDynamicParmCriteria),
})
mongoDynamicNS = api.namespace('MongoFunctionDynamic', description = 'MongoFunctionDynamic')
mongoDynamicML = api.model('MongoFunctionDynamic', {
   "QTY1":fields.Nested(mongoDynamicParm),
})
@mongoDynamicNS.route('', methods = ['POST'])
@mongoDynamicNS.response(200, 'Sucess')
@mongoDynamicNS.response(201, 'Created Sucess')
@mongoDynamicNS.response(204, 'No Content')
@mongoDynamicNS.response(400, 'Bad Request')
@mongoDynamicNS.response(401, 'Unauthorized')
@mongoDynamicNS.response(403, 'Forbidden')
@mongoDynamicNS.response(404, 'Not Found')
@mongoDynamicNS.response(405, 'Method Not Allowed')
@mongoDynamicNS.response(409, 'Conflict')
@mongoDynamicNS.response(500, 'Internal Server Error')
class mongoFunctionDynamic(Resource):
    @mongoDynamicNS.doc('MongoFunctionDynamic')
    @mongoDynamicNS.expect(mongoDynamicML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        mongo = mongoDbFunctionDynamic(jsonData)        
        return mongo.getData()

sseDataParmM = api.model('sseDataParmM', {  
    "MESSAGE":fields.String(required = True, description = 'MESSAGE', default = '123', example = '123'),
})
sseSetting = api.model('sseSetting', {  
    "IP":fields.String(required = True, description = 'IP', default = '10.55.8.62', example = '10.55.8.62'),
    "PORT":fields.String(required = True, description = 'PORT', default = '27017', example = '27017'),
    "DB":fields.String(required = True, description = 'DB', default = 'IAMP', example = 'IAMP'),
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'streamComponent', example = 'streamComponent'),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'SELECT', example = 'SELECT'),
})
sseData = api.model('sseData', {  
    "EVENTTYPE":fields.String(required = True, description = 'EVENTTYPE', default = 'social', example = 'social'),
    "KEYNAME":fields.String(required = True, description = 'KEYNAME', default = 'Gallery', example = 'Gallery'),
    "MESSAGE":fields.String(required = True, description = 'MESSAGE', default = '123', example = '123'),
})
sseDataParm = api.model('sseDataParm', {
    "SETTING":fields.Nested(sseSetting),
    "DATA":fields.Nested(sseData),
})
sseNS = api.namespace('SSE', description = 'SSE')
sseML = api.model('SSE', {
    "QTY1":fields.Nested(sseDataParm),
})
@sseNS.route('', methods = ['POST'])
@sseNS.response(200, 'Sucess')
@sseNS.response(201, 'Created Sucess')
@sseNS.response(204, 'No Content')
@sseNS.response(400, 'Bad Request')
@sseNS.response(401, 'Unauthorized')
@sseNS.response(403, 'Forbidden')
@sseNS.response(404, 'Not Found')
@sseNS.response(405, 'Method Not Allowed')
@sseNS.response(409, 'Conflict')
@sseNS.response(500, 'Internal Server Error')
class ssePublish(Resource):
    @sseNS.doc('ssePublish')
    @sseNS.expect(sseML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-type'}
        jsonData = BaseType.validateType(request.json)
        s = ssePublicFunc(jsonData)
        # client url "http://10.55.21.116:5001/stream1?channel=channel_bob"
        return s.getData()

sseEqStatusNS = api.namespace('sseForCommon', description = 'sseEqStatus')
sseEqStatusML = api.model('sseForCommon', {
})
@sseEqStatusNS.route('', methods = ['POST'])
@sseEqStatusNS.response(200, 'Sucess')
@sseEqStatusNS.response(201, 'Created Sucess')
@sseEqStatusNS.response(204, 'No Content')
@sseEqStatusNS.response(400, 'Bad Request')
@sseEqStatusNS.response(401, 'Unauthorized')
@sseEqStatusNS.response(403, 'Forbidden')
@sseEqStatusNS.response(404, 'Not Found')
@sseEqStatusNS.response(405, 'Method Not Allowed')
@sseEqStatusNS.response(409, 'Conflict')
@sseEqStatusNS.response(500, 'Internal Server Error')
class sseForCommon(Resource):
    @sseEqStatusNS.doc('ssePublish')
    @sseEqStatusNS.expect(sseEqStatusML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "EVENTTYPE" not in jsonData or "CHANNEL" not in jsonData or "DATA" not in jsonData:
            return {"Result":"NG","Reason":"Miss Parameter"}, 400, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        if "-IOT" in jsonData["CHANNEL"]:
            sse.publish(jsonData["DATA"],type = jsonData["EVENTTYPE"],channel = jsonData["CHANNEL"])
        else:
            sse.publish([jsonData["DATA"]],type = jsonData["EVENTTYPE"],channel = jsonData["CHANNEL"])
        # client url "http://10.55.21.116:5001/stream1?channel=channel_bob"
        return {"Result":"OK","Reason":""}, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
registerStreamNS = api.namespace('RegisterStream', description = 'RegisterStream')
registerStreamML = api.model('RegisterStream', {
        "IDENTITY":fields.String( required = True, description = 'IDENTITY', default = 'INX_TN_J001_CELL_BOND300', example = 'INX_TN_J001_CELL_BOND300'),
        "TYPE":fields.String( required = True, description = 'TYPE', default = 'SUBSCRIBE', example = 'SUBSCRIBE'),
        "EVENTTYPE":fields.String( required = True, description = 'EVENTTYPE', default = 'TEST', example = 'TEST'),
        "CHANNEL":fields.String( required = True, description = 'CHANNEL', default = 'channel_bob', example = 'channel_bob'),    
})
@registerStreamNS.route('', methods = ['POST'])
@registerStreamNS.response(200, 'Sucess')
@registerStreamNS.response(201, 'Created Sucess')
@registerStreamNS.response(204, 'No Content')
@registerStreamNS.response(400, 'Bad Request')
@registerStreamNS.response(401, 'Unauthorized')
@registerStreamNS.response(403, 'Forbidden')
@registerStreamNS.response(404, 'Not Found')
@registerStreamNS.response(405, 'Method Not Allowed')
@registerStreamNS.response(409, 'Conflict')
@registerStreamNS.response(500, 'Internal Server Error')
class registerStream(Resource):
    @registerStreamNS.doc('registerStream')
    @registerStreamNS.expect(registerStreamML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "IDENTITY" not in jsonData or "TYPE" not in jsonData or "EVENTTYPE" not in jsonData or "CHANNEL" not in jsonData : 
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-type'}
        register = RegisterStreamFuncion(jsonData)      
        return  register.getData()
streamCompenentSseData = api.model('streamCompenentSseData', {
    "DATA":fields.Nested(sseDataParmM),
})
streamCompenentParmCriteriaCondition= api.model('streamCompenentParmCriteriaCondition', {
    "USERID":fields.String(required = True, description = 'USERID', default = 'JACK', example = 'JACK'),
    "EVENTTYPE":fields.String(required = True, description = 'EVENTTYPE', default = 'social', example = 'social'),
    "KEYNAME":fields.String(required = True, description = 'KEYNAME', default = 'Gallery', example = 'Gallery'),
})
streamCompenentParmCriteriaProjection= api.model('streamCompenentParmCriteriaProjection', {
    "_id":fields.Boolean(required = True, description = '_id', default = 'False', example = 'False')
})
streamCompenentParmCriteriaDataParm = api.model('streamCompenentParmCriteriaDataParm', {
    "_id":fields.Boolean(required = True, description = '_id', default = 'False', example = 'False')
})
streamCompenentParmCriteriaData= api.model('streamCompenentParmCriteriaData', {
      "MESSAGE":fields.String(required = True, description = 'MESSAGE', default = '123', example = '123'),
})
streamCompenentParmCriteria= api.model('streamCompenentParmCriteria', {
    "CONDITION":fields.Nested(streamCompenentParmCriteriaCondition),    
    "PROJECTION":fields.Nested(streamCompenentParmCriteriaProjection),
    "ISDOT":  fields.String(required = True, description = 'ISDOT', default = 'N', example = 'N'),  
    "DATA":fields.Nested(streamCompenentParmCriteriaData),
})
streamCompenentParmSetting = api.model('streamCompenentParmSetting', {
    "IP":fields.String(required = True, description = 'IP', default = '10.55.8.62', example = '10.55.8.62'),
    "PORT":fields.String(required = True, description = 'PORT', default = '27017', example = '27017'),
    "DB":fields.String(required = True, description = 'DB', default = 'IAMP', example = 'IAMP'),
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'streamComponent', example = 'streamComponent'),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'CREATE', example = 'CREATE'),
})
streamCompenentParm = api.model('streamCompenentParm', {
    "SETTING":fields.Nested(streamCompenentParmSetting),
    "CRITERIA":fields.Nested(streamCompenentParmCriteria),
})
streamCompenentNS = api.namespace('StreamComponent', description = 'StreamComponent')
streamCompenentML = api.model('StreamComponent', {
   "QTY1":fields.Nested(streamCompenentParm),
})
@streamCompenentNS.route('', methods = ['POST'])
@streamCompenentNS.response(200, 'Sucess')
@streamCompenentNS.response(201, 'Created Sucess')
@streamCompenentNS.response(204, 'No Content')
@streamCompenentNS.response(400, 'Bad Request')
@streamCompenentNS.response(401, 'Unauthorized')
@streamCompenentNS.response(403, 'Forbidden')
@streamCompenentNS.response(404, 'Not Found')
@streamCompenentNS.response(405, 'Method Not Allowed')
@streamCompenentNS.response(409, 'Conflict')
@streamCompenentNS.response(500, 'Internal Server Error')
class streamCompenent(Resource):
    @streamCompenentNS.doc('streamCompenent')
    @streamCompenentNS.expect(streamCompenentML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        s = streamComponentFunction(jsonData)        
        return s.getData()

dataForComponentCondition = api.model('dataForComponentCondition', {
})
dataForComponentProjection = api.model('dataForComponentProjection', {
})
dataForComponentData = api.model('dataForComponentData',{}
)
dataForComponentNS = api.namespace('dataForComponent', description = 'dataForComponent')
dataForComponentML = api.model('dataForComponent', {
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'streamComponent', example = 'streamComponent'),
    "CONDITION":fields.Nested(dataForComponentCondition),
    "PROJECTION":fields.Nested(dataForComponentProjection),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'SELECT', example = 'SELECT'),
    "ISDOT":fields.String(required = False, description = 'ISDOT', default = 'N', example = 'N'),
    "DATA":fields.Nested(dataForComponentData),
})
@dataForComponentNS.route('', methods = ['POST'])
@dataForComponentNS.response(200, 'Sucess')
@dataForComponentNS.response(201, 'Created Sucess')
@dataForComponentNS.response(204, 'No Content')
@dataForComponentNS.response(400, 'Bad Request')
@dataForComponentNS.response(401, 'Unauthorized')
@dataForComponentNS.response(403, 'Forbidden')
@dataForComponentNS.response(404, 'Not Found')
@dataForComponentNS.response(405, 'Method Not Allowed')
@dataForComponentNS.response(409, 'Conflict')
@dataForComponentNS.response(500, 'Internal Server Error')
class dataForComponent(Resource):
    @dataForComponentNS.doc('dataForComponent')
    @dataForComponentNS.expect(dataForComponentML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        d = dataForComponentFunc(jsonData)
        return d.getData()
kafkaReplaytCondition = api.model('kafkaReplaytCondition',{
    "LINE_ID":fields.String(required = True, description = 'LINE_ID', default = 'BOND301', example = 'BOND301'),
})
kafkaReplaytNS = api.namespace('kafkaReplay', description = 'kafkaReplay')
kafkaReplayML = api.model('kafkaReplay', {
    "TOPIC":fields.String(required = True, description = 'TOPIC', default = 'INX-TN-J001', example = 'INX-TN-J001'),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-08-05 13:13:04.000', example = '2020-08-05 13:13:04.000'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-08-05 13:13:04.000', example = '2020-08-05 13:13:04.000'),
    "CONDITION":fields.Nested(kafkaReplaytCondition),
})
@kafkaReplaytNS.route('', methods = ['POST'])
@kafkaReplaytNS.response(200, 'Sucess')
@kafkaReplaytNS.response(201, 'Created Sucess')
@kafkaReplaytNS.response(204, 'No Content')
@kafkaReplaytNS.response(400, 'Bad Request')
@kafkaReplaytNS.response(401, 'Unauthorized')
@kafkaReplaytNS.response(403, 'Forbidden')
@kafkaReplaytNS.response(404, 'Not Found')
@kafkaReplaytNS.response(405, 'Method Not Allowed')
@kafkaReplaytNS.response(409, 'Conflict')
@kafkaReplaytNS.response(500, 'Internal Server Error')
class kafkaReplay(Resource):
    @kafkaReplaytNS.doc('kafkaReplay')
    @kafkaReplaytNS.expect(kafkaReplayML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "TOPIC" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData or "CONDITION" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        replay = kafkaReplayFunc(jsonData)
        return replay.getData()

vLogNS = api.namespace('vLog', description = 'vLog')
vLogML = api.model('vLog', {
    "ad":fields.String(required = True, description = 'ad', default = '', example = ''),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19 00:00:00', example = '2020-10-19 00:00:00'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20 23:59:59', example = '2020-10-20 23:59:59'),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'EACH', example = 'EACH'),
})
@vLogNS.route('', methods = ['POST'])
@vLogNS.response(200, 'Sucess')
@vLogNS.response(201, 'Created Sucess')
@vLogNS.response(204, 'No Content')
@vLogNS.response(400, 'Bad Request')
@vLogNS.response(401, 'Unauthorized')
@vLogNS.response(403, 'Forbidden')
@vLogNS.response(404, 'Not Found')
@vLogNS.response(405, 'Method Not Allowed')
@vLogNS.response(409, 'Conflict')
@vLogNS.response(500, 'Internal Server Error')
class vLog(Resource):
    @vLogNS.doc('vLog')
    @vLogNS.expect(vLogML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "ad" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData :
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = vLogFunc(jsonData)
        return v.getData()
wipLogCondition =api.model('wipLogCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY_ID', default = 'J003', example = 'J003'),
    "PROCESS":fields.String( description = 'PROCESS', default = 'BONDING', example = 'BONDING'),
})
wipLogNS = api.namespace('wipLog', description = 'wipLog')
wipLogML = api.model('wipLog', {
    "CONDITION":fields.Nested(wipLogCondition),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@wipLogNS.route('', methods = ['POST'])
@wipLogNS.response(200, 'Sucess')
@wipLogNS.response(201, 'Created Sucess')
@wipLogNS.response(204, 'No Content')
@wipLogNS.response(400, 'Bad Request')
@wipLogNS.response(401, 'Unauthorized')
@wipLogNS.response(403, 'Forbidden')
@wipLogNS.response(404, 'Not Found')
@wipLogNS.response(405, 'Method Not Allowed')
@wipLogNS.response(409, 'Conflict')
@wipLogNS.response(500, 'Internal Server Error')
class wipLog(Resource):
    @wipLogNS.doc('wipLog')
    @wipLogNS.expect(wipLogML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData :
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = wipLogFunc(jsonData)
        return v.getData()

wipLogNpCondition =api.model('wipLogCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "WORK_CTR":fields.String(required = True, description = 'WORK_CTR', default = '7001', example = '7001'),
    "PROCESS":fields.String( description = 'PROCESS', default = 'LAM', example = 'LAM'),
})
wipLogNpNS = api.namespace('wipLogNp', description = 'wipLogNp')
wipLogNpML = api.model('wipLogNp', {
    "CONDITION":fields.Nested(wipLogNpCondition),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@wipLogNpNS.route('', methods = ['POST'])
@wipLogNpNS.response(200, 'Sucess')
@wipLogNpNS.response(201, 'Created Sucess')
@wipLogNpNS.response(204, 'No Content')
@wipLogNpNS.response(400, 'Bad Request')
@wipLogNpNS.response(401, 'Unauthorized')
@wipLogNpNS.response(403, 'Forbidden')
@wipLogNpNS.response(404, 'Not Found')
@wipLogNpNS.response(405, 'Method Not Allowed')
@wipLogNpNS.response(409, 'Conflict')
@wipLogNpNS.response(500, 'Internal Server Error')
class wipLogNp(Resource):
    @wipLogNpNS.doc('wipLogNp')
    @wipLogNpNS.expect(wipLogNpML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData :
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = wipLogFuncNp(jsonData)
        return v.getData()
wipLogNewConceal = api.model('wipLogNewConceal', {
    "WORK_CTR":fields.String(required = True, description = 'WORK_CTR', default = '1000', example = '1000'),
    "PROCESS":fields.String( description = 'PROCESS', default = 'UNKNOWN', example = 'UNKNOWN'),
})
wipLogNewNCondition =api.model('wipLogNewCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "WORK_CTR":fields.String(required = True, description = 'WORK_CTR', default = '7001', example = '7001'),
    "PROCESS":fields.String( description = 'PROCESS', default = 'LAM', example = 'LAM'),
})
wipLogNewGroupBy = api.model('wipLogNewGroupBy', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "WORK_CTR":fields.String(required = True, description = 'WORK_CTR', default = '7001', example = '7001'),
    "PROCESS":fields.String( description = 'PROCESS', default = 'LAM', example = 'LAM'),
})
wipLogNewNS = api.namespace('wipLogNew', description = 'wipLogNew')
wipLogNewML = api.model('wipLogNew', {
    "CONDITION":fields.Nested(wipLogNpCondition),
    "GROUPBY": fields.String(required = True, description = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], default = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], example = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"]),
    "CONCEAL":fields.Nested(wipLogNewConceal),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@wipLogNewNS.route('', methods = ['POST'])
@wipLogNewNS.response(200, 'Sucess')
@wipLogNewNS.response(201, 'Created Sucess')
@wipLogNewNS.response(204, 'No Content')
@wipLogNewNS.response(400, 'Bad Request')
@wipLogNewNS.response(401, 'Unauthorized')
@wipLogNewNS.response(403, 'Forbidden')
@wipLogNewNS.response(404, 'Not Found')
@wipLogNewNS.response(405, 'Method Not Allowed')
@wipLogNewNS.response(409, 'Conflict')
@wipLogNewNS.response(500, 'Internal Server Error')
class wipLogNew(Resource):
    @wipLogNewNS.doc('wipLogNewNS')
    @wipLogNewNS.expect(wipLogNewML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData and "GROUPBY" not in jsonData and "CONCEAL" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = wipLogNewFunc(jsonData)
        return v.getData()

alternativeNS = api.namespace('alternative', description = 'alternativeNS')
alternativeML = api.model('alternative', {
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    "TYPE": fields.String(required = True, description = 'TYPE', default = 'WIP', example = 'WIP'),   
})
@alternativeNS.route('', methods = ['POST'])
@alternativeNS.response(200, 'Sucess')
@alternativeNS.response(201, 'Created Sucess')
@alternativeNS.response(204, 'No Content')
@alternativeNS.response(400, 'Bad Request')
@alternativeNS.response(401, 'Unauthorized')
@alternativeNS.response(403, 'Forbidden')
@alternativeNS.response(404, 'Not Found')
@alternativeNS.response(405, 'Method Not Allowed')
@alternativeNS.response(409, 'Conflict')
@alternativeNS.response(500, 'Internal Server Error')
class alternative(Resource):
    @alternativeNS.doc('alternativeNS')
    @alternativeNS.expect(alternativeML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "FACTORY_ID" not in jsonData or "TYPE" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = alternateFunc(jsonData)
        return v.getData()

fpyLogConceal = api.model('fpyLogConceal', {
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
})
fpyLogCondition =api.model('fpyLogCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
    "APPLICATION":fields.String( description = 'APPLICATION', default = 'AA', example = 'AA'),
    "PROD_NBR":fields.String(required = True, description = 'PROD_NBR', default = '2DD272IA0370M', example = '2DD272IA0370M'),
})
fpyLogNewNS = api.namespace('fpyLog', description = 'fpyLog')
fpyLogNewML = api.model('fpyLog', {
    "CONDITION":fields.Nested(fpyLogCondition),
    "GROUPBY": fields.String(required = True, description = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], default = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], example = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"]),
    "CONCEAL":fields.Nested(fpyLogConceal),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@fpyLogNewNS.route('', methods = ['POST'])
@fpyLogNewNS.response(200, 'Sucess')
@fpyLogNewNS.response(201, 'Created Sucess')
@fpyLogNewNS.response(204, 'No Content')
@fpyLogNewNS.response(400, 'Bad Request')
@fpyLogNewNS.response(401, 'Unauthorized')
@fpyLogNewNS.response(403, 'Forbidden')
@fpyLogNewNS.response(404, 'Not Found')
@fpyLogNewNS.response(405, 'Method Not Allowed')
@fpyLogNewNS.response(409, 'Conflict')
@fpyLogNewNS.response(500, 'Internal Server Error')
class fpyLog(Resource):
    @fpyLogNewNS.doc('fpyLogNewNS')
    @fpyLogNewNS.expect(fpyLogNewML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData and "GROUPBY" not in jsonData and "CONCEAL" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = fpyLogFunc(jsonData)
        return v.getData()

fpyLogApplicationConceal = api.model('fpyLogApplicationConceal', {
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
})
fpyLogApplicationCondition =api.model('fpyLogApplicationCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
    "APPLICATION":fields.String( description = 'APPLICATION', default = 'AA', example = 'AA'),
    "PROD_NBR":fields.String(required = True, description = 'PROD_NBR', default = '2DD272IA0370M', example = '2DD272IA0370M'),
})
fpyLogApplicationNS = api.namespace('fpyLogApplication', description = 'fpyLogApplication')
fpyLogApplicationML = api.model('fpyLogApplication', {
    "CONDITION":fields.Nested(fpyLogCondition),
    "GROUPBY": fields.String(required = True, description = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], default = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], example = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"]),
    "CONCEAL":fields.Nested(fpyLogConceal),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@fpyLogApplicationNS.route('', methods = ['POST'])
@fpyLogApplicationNS.response(200, 'Sucess')
@fpyLogApplicationNS.response(201, 'Created Sucess')
@fpyLogApplicationNS.response(204, 'No Content')
@fpyLogApplicationNS.response(400, 'Bad Request')
@fpyLogApplicationNS.response(401, 'Unauthorized')
@fpyLogApplicationNS.response(403, 'Forbidden')
@fpyLogApplicationNS.response(404, 'Not Found')
@fpyLogApplicationNS.response(405, 'Method Not Allowed')
@fpyLogApplicationNS.response(409, 'Conflict')
@fpyLogApplicationNS.response(500, 'Internal Server Error')
class fpyLogApplication(Resource):
    @fpyLogNewNS.doc('fpyLogApplicationNS')
    @fpyLogNewNS.expect(fpyLogApplicationML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData and "GROUPBY" not in jsonData and "CONCEAL" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = fpyLogApplicationFunc(jsonData)
        return v.getData()

dpsPlainConceal = api.model('dpsPlainConceal', {
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
})
dpsPlainCondition =api.model('dpsPlainCondition', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
    "APPLICATION":fields.String( description = 'APPLICATION', default = 'AA', example = 'AA'),
    "PROD_NBR":fields.String(required = True, description = 'PROD_NBR', default = '2DD272IA0370M', example = '2DD272IA0370M'),
})
dpsPlainNS = api.namespace('dpsPlain', description = 'dpsPlain')
dpsPlainML = api.model('dpsPlain', {
    "CONDITION":fields.Nested(fpyLogCondition),
    "GROUPBY": fields.String(required = True, description = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], default = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"], example = ["COMPANY_CODE","SITE","FACTORY_ID","ACCT_DATE"]),
    "CONCEAL":fields.Nested(fpyLogConceal),
    "STARTIME":fields.String(required = True, description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(required = True, description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@dpsPlainNS.route('', methods = ['POST'])
@dpsPlainNS.response(200, 'Sucess')
@dpsPlainNS.response(201, 'Created Sucess')
@dpsPlainNS.response(204, 'No Content')
@dpsPlainNS.response(400, 'Bad Request')
@dpsPlainNS.response(401, 'Unauthorized')
@dpsPlainNS.response(403, 'Forbidden')
@dpsPlainNS.response(404, 'Not Found')
@dpsPlainNS.response(405, 'Method Not Allowed')
@dpsPlainNS.response(409, 'Conflict')
@dpsPlainNS.response(500, 'Internal Server Error')
class dpsPlain(Resource):
    @dpsPlainNS.doc('dpsPlainNS')
    @dpsPlainNS.expect(dpsPlainML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "CONDITION" not in jsonData or "STARTIME" not in jsonData or "ENDTIME" not in jsonData and "GROUPBY" not in jsonData and "CONCEAL" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = dpsPlainFunc(jsonData)
        return v.getData()


singleCollectionGroupbyId = api.model('singleCollectionGroupbyId', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY_CODE', default = '$COMPANY_CODE', example = '$COMPANY_CODE'),
    "SITE":fields.String(required = True, description = 'SITE', default = '$SITE', example = '$SITE'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY_ID', default = '$FACTORY_ID', example = '$FACTORY_ID'),
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '$MAIN_WC', example = '$MAIN_WC'),
    "APPLICATION":fields.String( description = 'APPLICATION', default = '$APPLICATION', example = '$APPLICATION'),
    "PROD_NBR":fields.String(required = True, description = 'PROD_NBR', default = '$PROD_NBR', example = '$PROD_NBR'),
})
singleCollectionMacth =api.model('singleCollectionMacth', {
    "COMPANY_CODE":fields.String(required = True, description = 'COMPANY', default = 'INX', example = 'INX'),
    "SITE":fields.String(required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "MAIN_WC":fields.String(required = True, description = 'MAIN_WC', default = '1320', example = '1320'),
    "APPLICATION":fields.String( description = 'APPLICATION', default = 'AA', example = 'AA'),
    "PROD_NBR":fields.String(required = True, description = 'PROD_NBR', default = '2DD272IA0370M', example = '2DD272IA0370M')
})
singleCollectionProject =api.model('singleCollectionProject', {
    "_id":fields.Integer(required = True, description = '_id', default = 0, example = 0),
})
singleCollectionGroupby =api.model('singleCollectionGroupby', {
    "_id":fields.Nested(singleCollectionGroupbyId),
})
singleCollectionSort = api.model('singleCollectionSort', {
    "ACCT_DATE":fields.Integer(required = True, description = 'ACCT_DATE', default = 1, example = 1),
})
singleCollectionAGGREGATE = api.model('singleCollectionAGGREGATE', {
   
})
singleCollectionNS = api.namespace('singleCollection', description = 'singleCollection')
singleCollectionML = api.model('singleCollection', {
    "DATABASE":fields.String(required = True, description = 'DATABASE', default = 'IAMP', example = 'IAMP'),
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'wipHisAndCurrent', example = 'wipHisAndCurrent'),
    "AGGREGATE":fields.List(fields.Nested(singleCollectionAGGREGATE)),
    "STARTIME":fields.String(description = 'STARTIME', default = '2020-10-19', example = '2020-10-19'),
    "ENDTIME":fields.String(description = 'ENDTIME', default = '2020-10-20', example = '2020-10-20'),
})
@singleCollectionNS.route('', methods = ['POST'])
@singleCollectionNS.response(200, 'Sucess')
@singleCollectionNS.response(201, 'Created Sucess')
@singleCollectionNS.response(204, 'No Content')
@singleCollectionNS.response(400, 'Bad Request')
@singleCollectionNS.response(401, 'Unauthorized')
@singleCollectionNS.response(403, 'Forbidden')
@singleCollectionNS.response(404, 'Not Found')
@singleCollectionNS.response(405, 'Method Not Allowed')
@singleCollectionNS.response(409, 'Conflict')
@singleCollectionNS.response(500, 'Internal Server Error')
class singleCollection(Resource):
    @singleCollectionNS.doc('singleCollectionML')
    @singleCollectionNS.expect(singleCollectionML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "DATABASE" not in jsonData or "COLLECTION" not in jsonData or "AGGREGATE" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = singleCollectionFunc(jsonData)
        return v.getData()

mongoInsertManyData =api.model('mongoInsertManyData', {
})
mongoInsertManyNS = api.namespace('mongoInsertMany', description = 'mongoInsertMany')
mongoInsertManyML = api.model('mongoInsertMany', {
    "DATABASE":fields.String(required = True, description = 'DATABASE', default = 'IAMP', example = 'IAMP'),
    "COLLECTION":fields.String(required = True, description = 'COLLECTION', default = 'test', example = 'test'),
    "DATA":fields.List(fields.Nested(mongoInsertManyData)),
})
@mongoInsertManyNS.route('', methods = ['POST'])
@mongoInsertManyNS.response(200, 'Sucess')
@mongoInsertManyNS.response(201, 'Created Sucess')
@mongoInsertManyNS.response(204, 'No Content')
@mongoInsertManyNS.response(400, 'Bad Request')
@mongoInsertManyNS.response(401, 'Unauthorized')
@mongoInsertManyNS.response(403, 'Forbidden')
@mongoInsertManyNS.response(404, 'Not Found')
@mongoInsertManyNS.response(405, 'Method Not Allowed')
@mongoInsertManyNS.response(409, 'Conflict')
@mongoInsertManyNS.response(500, 'Internal Server Error')
class mongoInsertMany(Resource):
    @mongoInsertManyNS.doc('mongoInsertMany')
    @mongoInsertManyNS.expect(mongoInsertManyML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if  "DATABASE" not in jsonData or "COLLECTION" not in jsonData or "DATA" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = mongoInsertManyFunc(jsonData)
        return v.getData()

compensateDb2NS = api.namespace('CompensateDb2', description = 'CompensateDb2')
compensateDb2ML = api.model('CompensateDb2', {
    "FACTORY_ID":fields.String(required = True, description = 'FACTORY', default = 'J003', example = 'J003'),
    "TYPE":fields.String(required = True, description = 'TYPE', default = 'WIP', example = 'WIP'),
    "TIME":fields.String(description = 'TIME', default = '2020-10-19', example = '2020-10-19'),
})
@compensateDb2NS.route('', methods = ['POST'])
@compensateDb2NS.response(200, 'Sucess')
@compensateDb2NS.response(201, 'Created Sucess')
@compensateDb2NS.response(204, 'No Content')
@compensateDb2NS.response(400, 'Bad Request')
@compensateDb2NS.response(401, 'Unauthorized')
@compensateDb2NS.response(403, 'Forbidden')
@compensateDb2NS.response(404, 'Not Found')
@compensateDb2NS.response(405, 'Method Not Allowed')
@compensateDb2NS.response(409, 'Conflict')
@compensateDb2NS.response(500, 'Internal Server Error')
class compensateDb2(Resource):
    @compensateDb2NS.doc('compensateDb2')
    @compensateDb2NS.expect(compensateDb2ML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if  "FACTORY_ID" not in jsonData or "TYPE" not in jsonData or "TIME" not in jsonData:
            return {'Result':'NG', 'Reason': 'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        c = compensate(jsonData)
        return c.getData()

MenuNS = api.namespace('GetMenu', description = '選單')
MenuML = api.model('GetMenu', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'SUPPLY_LINE': fields.String( required = True, description = 'SUPPLY_CATEGORY', default = 'CELL', example = 'CELL'),
    'TYPE': fields.String( required = True, description = 'TYPE', default = 'AGV', example = 'AGV'),
})
@MenuNS.route('', methods = ['POST'])
@MenuNS.response(200, 'Sucess')
@MenuNS.response(201, 'Created Sucess')
@MenuNS.response(204, 'No Content')
@MenuNS.response(400, 'Bad Request')
@MenuNS.response(401, 'Unauthorized')
@MenuNS.response(403, 'Forbidden')
@MenuNS.response(404, 'Not Found')
@MenuNS.response(405, 'Method Not Allowed')
@MenuNS.response(409, 'Conflict')
@MenuNS.response(500, 'Internal Server Error')
class getMenu(Resource):
    @MenuNS.doc('Get Menu')
    @MenuNS.expect(MenuML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = request.json
        if "COMPANY_CODE" not in jsonData or "FACTORY_ID" not in jsonData or "SITE" not in jsonData or "SUPPLY_LINE" not in jsonData or "TYPE" not in jsonData:
            {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        COMPANY_CODE = jsonData["COMPANY_CODE"]
        FACTORY_ID = jsonData["FACTORY_ID"]
        SITE = jsonData["SITE"]
        SUPPLY_LINE = jsonData["SUPPLY_LINE"]
        TYPE = jsonData["TYPE"]
        indentity="IAMP"
        menu = Menu(COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_LINE,TYPE,indentity)
        return menu.getData()

INTKPINs = api.namespace('GetINTKPI', description = 'INT_KPI')
INTKPIML = api.model('GetINTKPI', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'APPLICATION': fields.String( required = True, description = 'APPLICATION', default = 'ALL', example = 'CE'),
    'KPITYPE': fields.String( required = True, description = 'KPITYPE', default = 'FPY', example = 'FPY'),
    'ACCT_DATE': fields.String( required = True, description = 'ACCT_DATE', default = '20210801', example = '20210801')
    })
@INTKPINs.route('', methods = ['POST'])
@INTKPINs.response(200, 'Sucess')
@INTKPINs.response(201, 'Created Sucess')
@INTKPINs.response(204, 'No Content')
@INTKPINs.response(400, 'Bad Request')
@INTKPINs.response(401, 'Unauthorized')
@INTKPINs.response(403, 'Forbidden')
@INTKPINs.response(404, 'Not Found')
@INTKPINs.response(405, 'Method Not Allowed')
@INTKPINs.response(409, 'Conflict')
@INTKPINs.response(500, 'Internal Server Error')
class getINTKPI(Resource):
    @INTKPINs.doc('Provide INT KPI')
    @INTKPINs.expect(INTKPIML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "KPITYPE" not in jsonData or "ACCT_DATE" not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        
        v = INTKPI(jsonData, _db_pool)
        return v.getData()

INTLV2Ns = api.namespace('GetINTLV2', description = 'INTLV2')
INTLV2ML = api.model('GetINTLV2', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'APPLICATION': fields.String( required = True, description = 'APPLICATION', default = 'ALL', example = 'ALL'),
    'KPITYPE': fields.String( required = True, description = 'KPITYPE', default = 'FPYLV2PIE', example = 'FPYLV2PIE'),
    'ACCT_DATE': fields.String( required = True, description = 'ACCT_DATE', default = '20210801', example = '20210801'),
    'PROD_NBR' : fields.String( required = True, description = '機種編碼', default = 'GP062CCAC100S', example = 'GP062CCAC100S') 
})
@INTLV2Ns.route('', methods = ['POST'])
@INTLV2Ns.response(200, 'Sucess')
@INTLV2Ns.response(201, 'Created Sucess')
@INTLV2Ns.response(204, 'No Content')
@INTLV2Ns.response(400, 'Bad Request')
@INTLV2Ns.response(401, 'Unauthorized')
@INTLV2Ns.response(403, 'Forbidden')
@INTLV2Ns.response(404, 'Not Found')
@INTLV2Ns.response(405, 'Method Not Allowed')
@INTLV2Ns.response(409, 'Conflict')
@INTLV2Ns.response(500, 'Internal Server Error')
class getINTLV2(Resource):
    @INTLV2Ns.doc('INTLV2')
    @INTLV2Ns.expect(INTLV2ML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "KPITYPE" not in jsonData or "ACCT_DATE" not in jsonData or 'PROD_NBR' not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        
        v = INTLV2(jsonData, _db_pool)
        return v.getData()

INTLV3Ns = api.namespace('GetINTLV3', description = 'INTLV3')
INTLV3ML = api.model('GetINTLV3', {
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'APPLICATION': fields.String( required = True, description = 'APPLICATION', default = 'ALL', example = 'ALL'),
    'KPITYPE': fields.String( required = True, description = 'KPITYPE', default = 'FPYLV3LINE', example = 'FPYLV3LINE'),
    'ACCT_DATE': fields.String( required = True, description = 'ACCT_DATE', default = '20210801', example = '20210801'),
    'PROD_NBR' : fields.String( required = True, description = '機種編碼', default = 'GP062CCAC100S', example = 'GP062CCAC100S'), 
    'OPER' : fields.String( required = True, description = '站點', default = 'PCBI', example = 'PCBI'), 
    'CHECKCODE' : fields.String( required = False, description = 'Defect or Reason Code', default = 'PCPF1', example = 'PCPF1') 
})
@INTLV3Ns.route('', methods = ['POST'])
@INTLV3Ns.response(200, 'Sucess')
@INTLV3Ns.response(201, 'Created Sucess')
@INTLV3Ns.response(204, 'No Content')
@INTLV3Ns.response(400, 'Bad Request')
@INTLV3Ns.response(401, 'Unauthorized')
@INTLV3Ns.response(403, 'Forbidden')
@INTLV3Ns.response(404, 'Not Found')
@INTLV3Ns.response(405, 'Method Not Allowed')
@INTLV3Ns.response(409, 'Conflict')
@INTLV3Ns.response(500, 'Internal Server Error')
class getINTLV3(Resource):
    @INTLV3Ns.doc('INTLV3')
    @INTLV3Ns.expect(INTLV3ML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData or "KPITYPE" not in jsonData or "ACCT_DATE" not in jsonData or 'PROD_NBR' not in jsonData or 'OPER' not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        
        v = INTLV3(jsonData, _db_pool)
        return v.getData()

INTTALKNs = api.namespace('GetINTTALK', description = 'INTTALK')
INTTALKML = api.model('GetINTTALK', {
    'FUNCTYPE': fields.String( required = True, description = 'FUNCTYPE', default = 'CREATE', example = 'CREATE'),
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'PROD_NBR' : fields.String( required = True, description = '機種編碼', default = 'GP062CCAC100S', example = 'GP062CCAC100S'), 
    'OPER' : fields.String( required = True, description = '站點', default = 'PCBI', example = 'PCBI'), 
    'CODE' : fields.String( required = True, description = 'Defect or Reason Code', default = 'PCPF1', example = 'PCPF1'),
    'CONTENTTYPE': fields.String( required = False, description = 'CONTENTTYPE', default = 'AA', example = 'AA'),
    'CONTENT': fields.String( required = False, description = 'CONTENT', default = 'CONTENT', example = 'CONTENT'),
    'ACCT_DATE': fields.String( required = False, description = 'ACCT_DATE', default = '20210801', example = '20210801'),
    'STARTDT': fields.String( required = False, description = 'STARTDT', default = '20210901', example = '20210901'),
    'ENDDT': fields.String( required = False, description = 'ENDDT', default = '20210920', example = '20210920'),
    'TOTAL': fields.String( required = False, description = 'TOTAL', default = '1000', example = '1000'),
    'RATE': fields.String( required = False, description = 'RATE', default = '0.5', example = '0.5')
    })
@INTTALKNs.route('', methods = ['POST'])
@INTTALKNs.response(200, 'Sucess')
@INTTALKNs.response(201, 'Created Sucess')
@INTTALKNs.response(204, 'No Content')
@INTTALKNs.response(400, 'Bad Request')
@INTTALKNs.response(401, 'Unauthorized')
@INTTALKNs.response(403, 'Forbidden')
@INTTALKNs.response(404, 'Not Found')
@INTTALKNs.response(405, 'Method Not Allowed')
@INTTALKNs.response(409, 'Conflict')
@INTTALKNs.response(500, 'Internal Server Error')
class getINTTALK(Resource):
    @INTTALKNs.doc('INT TALK')
    @INTTALKNs.expect(INTTALKML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData \
            or "PROD_NBR" not in jsonData or "OPER" not in jsonData or "CODE" not in jsonData \
            or "FUNCTYPE" not in jsonData :
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        
        v = INTTALK(jsonData)
        return v.getData()

INTRelationNs = api.namespace('GetINTRelation', description = 'NTRelation')
INTRelationML = api.model('GetINTRelation', {
    'FUNCTYPE': fields.String( required = True, description = 'FUNCTYPE', default = 'REASON_PROD2', example = 'REASON_PROD2'),
    'COMPANY_CODE': fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    'SITE': fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    'FACTORY_ID': fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
    'APPLICATION': fields.String( required = True, description = 'APPLICATION', default = 'ALL', example = 'ALL'),
    'ACCT_DATE': fields.String( required = True, description = 'ACCT_DATE', default = '20211125', example = '20211125'),
    'PROD_NBR' : fields.String( required = True, description = '機種編碼', default = 'GP062CCAC100S', example = 'GP062CCAC100S'), 
    'OPER' : fields.String( required = True, description = '站點', default = 'CKEN', example = 'CKEN'), 
    'CHECKCODE' : fields.String( required = True, description = 'Defect or Reason Code', default = 'FAGEE-1', example = 'FAGEE-1') 
})
@INTRelationNs.route('', methods = ['POST'])
@INTRelationNs.response(200, 'Sucess')
@INTRelationNs.response(201, 'Created Sucess')
@INTRelationNs.response(204, 'No Content')
@INTRelationNs.response(400, 'Bad Request')
@INTRelationNs.response(401, 'Unauthorized')
@INTRelationNs.response(403, 'Forbidden')
@INTRelationNs.response(404, 'Not Found')
@INTRelationNs.response(405, 'Method Not Allowed')
@INTRelationNs.response(409, 'Conflict')
@INTRelationNs.response(500, 'Internal Server Error')
class getINTRelation(Resource):
    @INTRelationNs.doc('INTRelation')
    @INTRelationNs.expect(INTRelationML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
           return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}

        jsonData = BaseType.validateType(request.json)
        
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData \
            or "FUNCTYPE" not in jsonData or "ACCT_DATE" not in jsonData or 'PROD_NBR' not in jsonData \
            or 'OPER' not in jsonData or 'CHECKCODE' not in jsonData:
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        v = INTRelation(jsonData, _db_pool)
        return v.getData()

intSDTemp = [
    {
      "model": {
        "TYPE": "PROD",
        "CODE": "2HE080IA1010S",
        "APPLICATION": "TABLET"
      },
      "PASS": {
        "filter": {
          "LCM_OWNER": [
            "LCM0",
            "LCME",
            "PROD",
            "QTAP",
            "RES0"
          ],
          "RW_COUNT": [
            0,
            1
          ]
        },
        "operdata": [
          {
            "OPER": {
              "NAME": "PCBI",
              "RANGE": {
                "fromt": 1300,
                "tot": 1301
              },
              "EXCLUSION": []
            },
            "sumqty": 2
          }
        ]
      },
      "DEFT": {
        "filter": {
          "LCM_OWNER": [
            "LCM0",
            "LCME",
            "PROD",
            "QTAP",
            "RES0"
          ],
          "RW_COUNT": [
            0,
            1
          ]
        },
        "operdata": [
          {
            "OPER": {
              "NAME": "PCBI",
              "RANGE": {
                "fromt": 1050,
                "tot": 1301
              },
              "EXCLUSION": []
            },
            "sumdata": [
              {
                "code": "PCBC7",
                "sumqty": 2
              }
            ]
          }
        ]
      }
    }
  ]

wayneTestInfoNS = api.namespace('GetWayneTestInfo', description = 'WayneTestInfo')
wayneTestInfoML = api.model('GetWayneTestInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'J003', example = 'J003'),
})
@wayneTestInfoNS.route('', methods = ['POST'])
@wayneTestInfoNS.response(200, 'Sucess')
@wayneTestInfoNS.response(201, 'Created Sucess')
@wayneTestInfoNS.response(204, 'No Content')
@wayneTestInfoNS.response(400, 'Bad Request')
@wayneTestInfoNS.response(401, 'Unauthorized')
@wayneTestInfoNS.response(403, 'Forbidden')
@wayneTestInfoNS.response(404, 'Not Found')
@wayneTestInfoNS.response(405, 'Method Not Allowed')
@wayneTestInfoNS.response(409, 'Conflict')
@wayneTestInfoNS.response(500, 'Internal Server Error')
class GetWayneTestInfo(Resource):
    @wayneTestInfoNS.doc('AppConfSysMain')
    @wayneTestInfoNS.expect(wayneTestInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        wayneTestInfo = WayneTestInfo(identity)
        return wayneTestInfo.getData()  

isfpUpphInfoNS = api.namespace('GetiSFPUpphInfo', description = '廠晨會看板-UPPH 推移圖')
isfpUpphInfoML = api.model('GetiSFPUpphInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpUpphInfoNS.route('', methods = ['POST'])
@isfpUpphInfoNS.response(200, 'Sucess')
@isfpUpphInfoNS.response(201, 'Created Sucess')
@isfpUpphInfoNS.response(204, 'No Content')
@isfpUpphInfoNS.response(400, 'Bad Request')
@isfpUpphInfoNS.response(401, 'Unauthorized')
@isfpUpphInfoNS.response(403, 'Forbidden')
@isfpUpphInfoNS.response(404, 'Not Found')
@isfpUpphInfoNS.response(405, 'Method Not Allowed')
@isfpUpphInfoNS.response(409, 'Conflict')
@isfpUpphInfoNS.response(500, 'Internal Server Error')
class GetUpphInfo(Resource):
    @isfpUpphInfoNS.doc('AppConfSysMain')
    @isfpUpphInfoNS.expect(isfpUpphInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpUpphInfo = iSFPUpphInfo(identity, start_time, end_time)
        return isfpUpphInfo.getData()     

isfpUpphLightInfoNS = api.namespace('GetiSFPUpphLightInfo', description = '廠晨會看板-UPPH 推移圖燈號')
isfpUpphLightInfoML = api.model('GetiSFPUpphLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpUpphLightInfoNS.route('', methods = ['POST'])
@isfpUpphLightInfoNS.response(200, 'Sucess')
@isfpUpphLightInfoNS.response(201, 'Created Sucess')
@isfpUpphLightInfoNS.response(204, 'No Content')
@isfpUpphLightInfoNS.response(400, 'Bad Request')
@isfpUpphLightInfoNS.response(401, 'Unauthorized')
@isfpUpphLightInfoNS.response(403, 'Forbidden')
@isfpUpphLightInfoNS.response(404, 'Not Found')
@isfpUpphLightInfoNS.response(405, 'Method Not Allowed')
@isfpUpphLightInfoNS.response(409, 'Conflict')
@isfpUpphLightInfoNS.response(500, 'Internal Server Error')
class GetUpphLightInfo(Resource):
    @isfpUpphLightInfoNS.doc('AppConfSysMain')
    @isfpUpphLightInfoNS.expect(isfpUpphLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpUpphLightInfo = iSFPUpphLightInfo(identity, start_time, end_time)
        return isfpUpphLightInfo.getData()               

isfpResignInfoNS = api.namespace('GetiSFPResignInfo', description = '廠晨會看板-離職率(%)')
isfpResignInfoML = api.model('GetiSFPResignInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpResignInfoNS.route('', methods = ['POST'])
@isfpResignInfoNS.response(200, 'Sucess')
@isfpResignInfoNS.response(201, 'Created Sucess')
@isfpResignInfoNS.response(204, 'No Content')
@isfpResignInfoNS.response(400, 'Bad Request')
@isfpResignInfoNS.response(401, 'Unauthorized')
@isfpResignInfoNS.response(403, 'Forbidden')
@isfpResignInfoNS.response(404, 'Not Found')
@isfpResignInfoNS.response(405, 'Method Not Allowed')
@isfpResignInfoNS.response(409, 'Conflict')
@isfpResignInfoNS.response(500, 'Internal Server Error')
class GetResignInfo(Resource):
    @isfpResignInfoNS.doc('AppConfSysMain')
    @isfpResignInfoNS.expect(isfpResignInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpResignInfo = iSFPResignInfo(identity, start_time, end_time)
        return isfpResignInfo.getData()

isfpAttendanceInfoNS = api.namespace('GetiSFPAttendanceInfo', description = '廠晨會看板-出勤率(%)')
isfpAttendanceInfoML = api.model('GetiSFPAttendanceInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpAttendanceInfoNS.route('', methods = ['POST'])
@isfpAttendanceInfoNS.response(200, 'Sucess')
@isfpAttendanceInfoNS.response(201, 'Created Sucess')
@isfpAttendanceInfoNS.response(204, 'No Content')
@isfpAttendanceInfoNS.response(400, 'Bad Request')
@isfpAttendanceInfoNS.response(401, 'Unauthorized')
@isfpAttendanceInfoNS.response(403, 'Forbidden')
@isfpAttendanceInfoNS.response(404, 'Not Found')
@isfpAttendanceInfoNS.response(405, 'Method Not Allowed')
@isfpAttendanceInfoNS.response(409, 'Conflict')
@isfpAttendanceInfoNS.response(500, 'Internal Server Error')
class GetAttendanceInfo(Resource):
    @isfpAttendanceInfoNS.doc('AppConfSysMain')
    @isfpAttendanceInfoNS.expect(isfpAttendanceInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpAttendanceInfo = iSFPAttendanceInfo(identity, start_time, end_time)
        return isfpAttendanceInfo.getData()       

isfpReAtInfoNS = api.namespace('GetiSFPReAtInfo', description = '廠晨會看板-離職率(%) & 出勤率(%) 區塊')
isfpReAtInfoML = api.model('GetiSFPReAtInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpReAtInfoNS.route('', methods = ['POST'])
@isfpReAtInfoNS.response(200, 'Sucess')
@isfpReAtInfoNS.response(201, 'Created Sucess')
@isfpReAtInfoNS.response(204, 'No Content')
@isfpReAtInfoNS.response(400, 'Bad Request')
@isfpReAtInfoNS.response(401, 'Unauthorized')
@isfpReAtInfoNS.response(403, 'Forbidden')
@isfpReAtInfoNS.response(404, 'Not Found')
@isfpReAtInfoNS.response(405, 'Method Not Allowed')
@isfpReAtInfoNS.response(409, 'Conflict')
@isfpReAtInfoNS.response(500, 'Internal Server Error')
class GetReAtInfo(Resource):
    @isfpReAtInfoNS.doc('AppConfSysMain')
    @isfpReAtInfoNS.expect(isfpReAtInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpReAtInfo = iSFPReAtInfo(identity, start_time, end_time)
        return isfpReAtInfo.getData()             

isfpScrapLightInfoNS = api.namespace('GetiSFPScrapLightInfo', description = '廠晨會看板-報廢推移圖燈號')
isfpScrapLightInfoML = api.model('GetiSFPScrapLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpScrapLightInfoNS.route('', methods = ['POST'])
@isfpScrapLightInfoNS.response(200, 'Sucess')
@isfpScrapLightInfoNS.response(201, 'Created Sucess')
@isfpScrapLightInfoNS.response(204, 'No Content')
@isfpScrapLightInfoNS.response(400, 'Bad Request')
@isfpScrapLightInfoNS.response(401, 'Unauthorized')
@isfpScrapLightInfoNS.response(403, 'Forbidden')
@isfpScrapLightInfoNS.response(404, 'Not Found')
@isfpScrapLightInfoNS.response(405, 'Method Not Allowed')
@isfpScrapLightInfoNS.response(409, 'Conflict')
@isfpScrapLightInfoNS.response(500, 'Internal Server Error')
class GetScrapLightInfo(Resource):
    @isfpScrapLightInfoNS.doc('AppConfSysMain')
    @isfpScrapLightInfoNS.expect(isfpScrapLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpScrapLightInfo = iSFPScrapLightInfo(identity, start_time, end_time)
        return isfpScrapLightInfo.getData()

isfpScrapInfoNS = api.namespace('GetiSFPScrapInfo', description = '廠晨會看板-報廢推移圖')
isfpScrapInfoML = api.model('GetiSFPScrapInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpScrapInfoNS.route('', methods = ['POST'])
@isfpScrapInfoNS.response(200, 'Sucess')
@isfpScrapInfoNS.response(201, 'Created Sucess')
@isfpScrapInfoNS.response(204, 'No Content')
@isfpScrapInfoNS.response(400, 'Bad Request')
@isfpScrapInfoNS.response(401, 'Unauthorized')
@isfpScrapInfoNS.response(403, 'Forbidden')
@isfpScrapInfoNS.response(404, 'Not Found')
@isfpScrapInfoNS.response(405, 'Method Not Allowed')
@isfpScrapInfoNS.response(409, 'Conflict')
@isfpScrapInfoNS.response(500, 'Internal Server Error')
class GetScrapInfo(Resource):
    @isfpScrapInfoNS.doc('AppConfSysMain')
    @isfpScrapInfoNS.expect(isfpScrapInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpScrapInfo = iSFPScrapInfo(identity, start_time, end_time)
        return isfpScrapInfo.getData()

isfpWOInfoNS = api.namespace('GetiSFPWOInfo', description = '廠晨會看板-工單結案率(%)')
isfpWOInfoML = api.model('GetiSFPWOInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpWOInfoNS.route('', methods = ['POST'])
@isfpWOInfoNS.response(200, 'Sucess')
@isfpWOInfoNS.response(201, 'Created Sucess')
@isfpWOInfoNS.response(204, 'No Content')
@isfpWOInfoNS.response(400, 'Bad Request')
@isfpWOInfoNS.response(401, 'Unauthorized')
@isfpWOInfoNS.response(403, 'Forbidden')
@isfpWOInfoNS.response(404, 'Not Found')
@isfpWOInfoNS.response(405, 'Method Not Allowed')
@isfpWOInfoNS.response(409, 'Conflict')
@isfpWOInfoNS.response(500, 'Internal Server Error')
class GetWOInfo(Resource):
    @isfpWOInfoNS.doc('AppConfSysMain')
    @isfpWOInfoNS.expect(isfpWOInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpWOInfo = iSFPWOInfo(identity, start_time, end_time)
        return isfpWOInfo.getData()    

isfpWIP30InfoNS = api.namespace('GetiSFPWIP30Info', description = '廠晨會看板- >30天的WIP')
isfpWIP30InfoML = api.model('GetiSFPWIP30Info', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpWIP30InfoNS.route('', methods = ['POST'])
@isfpWIP30InfoNS.response(200, 'Sucess')
@isfpWIP30InfoNS.response(201, 'Created Sucess')
@isfpWIP30InfoNS.response(204, 'No Content')
@isfpWIP30InfoNS.response(400, 'Bad Request')
@isfpWIP30InfoNS.response(401, 'Unauthorized')
@isfpWIP30InfoNS.response(403, 'Forbidden')
@isfpWIP30InfoNS.response(404, 'Not Found')
@isfpWIP30InfoNS.response(405, 'Method Not Allowed')
@isfpWIP30InfoNS.response(409, 'Conflict')
@isfpWIP30InfoNS.response(500, 'Internal Server Error')
class GetWIP30Info(Resource):
    @isfpWIP30InfoNS.doc('AppConfSysMain')
    @isfpWIP30InfoNS.expect(isfpWIP30InfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpWIP30Info = iSFPWIP30Info(identity, start_time, end_time)
        return isfpWIP30Info.getData() 

isfpWIP14InfoNS = api.namespace('GetiSFPWIP14Info', description = '廠晨會看板- >14天的WIP')
isfpWIP14InfoML = api.model('GetiSFPWIP14Info', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpWIP14InfoNS.route('', methods = ['POST'])
@isfpWIP14InfoNS.response(200, 'Sucess')
@isfpWIP14InfoNS.response(201, 'Created Sucess')
@isfpWIP14InfoNS.response(204, 'No Content')
@isfpWIP14InfoNS.response(400, 'Bad Request')
@isfpWIP14InfoNS.response(401, 'Unauthorized')
@isfpWIP14InfoNS.response(403, 'Forbidden')
@isfpWIP14InfoNS.response(404, 'Not Found')
@isfpWIP14InfoNS.response(405, 'Method Not Allowed')
@isfpWIP14InfoNS.response(409, 'Conflict')
@isfpWIP14InfoNS.response(500, 'Internal Server Error')
class GetWIP14Info(Resource):
    @isfpWIP14InfoNS.doc('AppConfSysMain')
    @isfpWIP14InfoNS.expect(isfpWIP14InfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpWIP14Info = iSFPWIP14Info(identity, start_time, end_time)
        return isfpWIP14Info.getData() 

isfpWOWIPInfoNS = api.namespace('GetiSFPWOWIPInfo', description = '廠晨會看板- WO 與 WIP 區塊')
isfpWOWIPInfoML = api.model('GetiSFPWOWIPInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpWOWIPInfoNS.route('', methods = ['POST'])
@isfpWOWIPInfoNS.response(200, 'Sucess')
@isfpWOWIPInfoNS.response(201, 'Created Sucess')
@isfpWOWIPInfoNS.response(204, 'No Content')
@isfpWOWIPInfoNS.response(400, 'Bad Request')
@isfpWOWIPInfoNS.response(401, 'Unauthorized')
@isfpWOWIPInfoNS.response(403, 'Forbidden')
@isfpWOWIPInfoNS.response(404, 'Not Found')
@isfpWOWIPInfoNS.response(405, 'Method Not Allowed')
@isfpWOWIPInfoNS.response(409, 'Conflict')
@isfpWOWIPInfoNS.response(500, 'Internal Server Error')
class GetWOWIPInfo(Resource):
    @isfpWOWIPInfoNS.doc('AppConfSysMain')
    @isfpWOWIPInfoNS.expect(isfpWOWIPInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpWOWIPInfo = iSFPWOWIPInfo(identity, start_time, end_time)
        return isfpWOWIPInfo.getData()         

isfpOutLightInfoNS = api.namespace('GetiSFPOutLightInfo', description = '廠晨會看板- OutPut 燈號')
isfpOutLightInfoML = api.model('GetiSFPOutLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
"LINE_TYPE":fields.String( required = True, description = 'LINE_TYPE', default = 'ALL', example = 'ALL'),
})
@isfpOutLightInfoNS.route('', methods = ['POST'])
@isfpOutLightInfoNS.response(200, 'Sucess')
@isfpOutLightInfoNS.response(201, 'Created Sucess')
@isfpOutLightInfoNS.response(204, 'No Content')
@isfpOutLightInfoNS.response(400, 'Bad Request')
@isfpOutLightInfoNS.response(401, 'Unauthorized')
@isfpOutLightInfoNS.response(403, 'Forbidden')
@isfpOutLightInfoNS.response(404, 'Not Found')
@isfpOutLightInfoNS.response(405, 'Method Not Allowed')
@isfpOutLightInfoNS.response(409, 'Conflict')
@isfpOutLightInfoNS.response(500, 'Internal Server Error')
class GetOutLightInfo(Resource):
    @isfpOutLightInfoNS.doc('AppConfSysMain')
    @isfpOutLightInfoNS.expect(isfpOutLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        line_type = jsonData["LINE_TYPE"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpOutLightInfo = iSFPOutLightInfo(identity, start_time, end_time, line_type)
        return isfpOutLightInfo.getData()  

isfpOutputInfoNS = api.namespace('GetiSFPOutputInfo', description = '廠晨會看板- OutPut')
isfpOutputInfoML = api.model('GetiSFPOutputInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
"LINE_TYPE":fields.String( required = True, description = 'LINE_TYPE', default = 'ALL', example = 'ALL'),
})
@isfpOutputInfoNS.route('', methods = ['POST'])
@isfpOutputInfoNS.response(200, 'Sucess')
@isfpOutputInfoNS.response(201, 'Created Sucess')
@isfpOutputInfoNS.response(204, 'No Content')
@isfpOutputInfoNS.response(400, 'Bad Request')
@isfpOutputInfoNS.response(401, 'Unauthorized')
@isfpOutputInfoNS.response(403, 'Forbidden')
@isfpOutputInfoNS.response(404, 'Not Found')
@isfpOutputInfoNS.response(405, 'Method Not Allowed')
@isfpOutputInfoNS.response(409, 'Conflict')
@isfpOutputInfoNS.response(500, 'Internal Server Error')
class GetOutputInfo(Resource):
    @isfpOutputInfoNS.doc('AppConfSysMain')
    @isfpOutputInfoNS.expect(isfpOutputInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        line_type = jsonData["LINE_TYPE"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpOutputInfo = iSFPOutputInfo(identity, start_time, end_time, line_type)
        return isfpOutputInfo.getData()                                      

isfpReachInfoNS = api.namespace('GetiSFPReachInfo', description = '廠晨會看板- 達產(%)')
isfpReachInfoML = api.model('GetiSFPReachInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpReachInfoNS.route('', methods = ['POST'])
@isfpReachInfoNS.response(200, 'Sucess')
@isfpReachInfoNS.response(201, 'Created Sucess')
@isfpReachInfoNS.response(204, 'No Content')
@isfpReachInfoNS.response(400, 'Bad Request')
@isfpReachInfoNS.response(401, 'Unauthorized')
@isfpReachInfoNS.response(403, 'Forbidden')
@isfpReachInfoNS.response(404, 'Not Found')
@isfpReachInfoNS.response(405, 'Method Not Allowed')
@isfpReachInfoNS.response(409, 'Conflict')
@isfpReachInfoNS.response(500, 'Internal Server Error')
class GetReachInfo(Resource):
    @isfpReachInfoNS.doc('AppConfSysMain')
    @isfpReachInfoNS.expect(isfpReachInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpReachInfo = iSFPReachInfo(identity, start_time, end_time)
        return isfpReachInfo.getData()

isfpTDInputInfoNS = api.namespace('GetiSFPTDInputInfo', description = '廠晨會看板- TD & Input 區塊')
isfpTDInputInfoML = api.model('GetiSFPTDInputInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpTDInputInfoNS.route('', methods = ['POST'])
@isfpTDInputInfoNS.response(200, 'Sucess')
@isfpTDInputInfoNS.response(201, 'Created Sucess')
@isfpTDInputInfoNS.response(204, 'No Content')
@isfpTDInputInfoNS.response(400, 'Bad Request')
@isfpTDInputInfoNS.response(401, 'Unauthorized')
@isfpTDInputInfoNS.response(403, 'Forbidden')
@isfpTDInputInfoNS.response(404, 'Not Found')
@isfpTDInputInfoNS.response(405, 'Method Not Allowed')
@isfpTDInputInfoNS.response(409, 'Conflict')
@isfpTDInputInfoNS.response(500, 'Internal Server Error')
class GetTDInputInfo(Resource):
    @isfpTDInputInfoNS.doc('AppConfSysMain')
    @isfpTDInputInfoNS.expect(isfpTDInputInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpTDInputInfo = iSFPTDInputInfo(identity, start_time, end_time)
        return isfpTDInputInfo.getData() 

isfpOEEInfoNS = api.namespace('GetiSFPOEEInfo', description = '廠晨會看板- 設備OEE(%)')
isfpOEEInfoML = api.model('GetiSFPOEEInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpOEEInfoNS.route('', methods = ['POST'])
@isfpOEEInfoNS.response(200, 'Sucess')
@isfpOEEInfoNS.response(201, 'Created Sucess')
@isfpOEEInfoNS.response(204, 'No Content')
@isfpOEEInfoNS.response(400, 'Bad Request')
@isfpOEEInfoNS.response(401, 'Unauthorized')
@isfpOEEInfoNS.response(403, 'Forbidden')
@isfpOEEInfoNS.response(404, 'Not Found')
@isfpOEEInfoNS.response(405, 'Method Not Allowed')
@isfpOEEInfoNS.response(409, 'Conflict')
@isfpOEEInfoNS.response(500, 'Internal Server Error')
class GetOEEInfo(Resource):
    @isfpOEEInfoNS.doc('AppConfSysMain')
    @isfpOEEInfoNS.expect(isfpOEEInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpOEEInfo = iSFPOEEInfo(identity, start_time, end_time)
        return isfpOEEInfo.getData()               

isfpFPYYLightInfoNS = api.namespace('GetiSFPFPYYLightInfo', description = '廠晨會看板- FPY(含快修) 燈號')
isfpFPYYLightInfoML = api.model('GetiSFPFPYYLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpFPYYLightInfoNS.route('', methods = ['POST'])
@isfpFPYYLightInfoNS.response(200, 'Sucess')
@isfpFPYYLightInfoNS.response(201, 'Created Sucess')
@isfpFPYYLightInfoNS.response(204, 'No Content')
@isfpFPYYLightInfoNS.response(400, 'Bad Request')
@isfpFPYYLightInfoNS.response(401, 'Unauthorized')
@isfpFPYYLightInfoNS.response(403, 'Forbidden')
@isfpFPYYLightInfoNS.response(404, 'Not Found')
@isfpFPYYLightInfoNS.response(405, 'Method Not Allowed')
@isfpFPYYLightInfoNS.response(409, 'Conflict')
@isfpFPYYLightInfoNS.response(500, 'Internal Server Error')
class GetFPYYLightInfo(Resource):
    @isfpFPYYLightInfoNS.doc('AppConfSysMain')
    @isfpFPYYLightInfoNS.expect(isfpFPYYLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpFPYYLightInfo = iSFPFPYYLightInfo(identity, start_time, end_time)
        return isfpFPYYLightInfo.getData() 

isfpFPYYInfoNS = api.namespace('GetiSFPFPYYInfo', description = '廠晨會看板- FPY(含快修) 推移圖')
isfpFPYYInfoML = api.model('GetiSFPFPYYInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpFPYYInfoNS.route('', methods = ['POST'])
@isfpFPYYInfoNS.response(200, 'Sucess')
@isfpFPYYInfoNS.response(201, 'Created Sucess')
@isfpFPYYInfoNS.response(204, 'No Content')
@isfpFPYYInfoNS.response(400, 'Bad Request')
@isfpFPYYInfoNS.response(401, 'Unauthorized')
@isfpFPYYInfoNS.response(403, 'Forbidden')
@isfpFPYYInfoNS.response(404, 'Not Found')
@isfpFPYYInfoNS.response(405, 'Method Not Allowed')
@isfpFPYYInfoNS.response(409, 'Conflict')
@isfpFPYYInfoNS.response(500, 'Internal Server Error')
class GetFPYYInfo(Resource):
    @isfpFPYYInfoNS.doc('AppConfSysMain')
    @isfpFPYYInfoNS.expect(isfpFPYYInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpFPYYInfo = iSFPFPYYInfo(identity, start_time, end_time)
        return isfpFPYYInfo.getData() 

isfpFPYScrapInfoNS = api.namespace('GetiSFPFPYScrapInfo', description = '廠晨會看板- FPY Total(%) & 破片報廢率(%) 區塊')
isfpFPYScrapInfoML = api.model('GetiSFPFPYScrapInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpFPYScrapInfoNS.route('', methods = ['POST'])
@isfpFPYScrapInfoNS.response(200, 'Sucess')
@isfpFPYScrapInfoNS.response(201, 'Created Sucess')
@isfpFPYScrapInfoNS.response(204, 'No Content')
@isfpFPYScrapInfoNS.response(400, 'Bad Request')
@isfpFPYScrapInfoNS.response(401, 'Unauthorized')
@isfpFPYScrapInfoNS.response(403, 'Forbidden')
@isfpFPYScrapInfoNS.response(404, 'Not Found')
@isfpFPYScrapInfoNS.response(405, 'Method Not Allowed')
@isfpFPYScrapInfoNS.response(409, 'Conflict')
@isfpFPYScrapInfoNS.response(500, 'Internal Server Error')
class GetFPYScrapInfo(Resource):
    @isfpFPYScrapInfoNS.doc('AppConfSysMain')
    @isfpFPYScrapInfoNS.expect(isfpFPYScrapInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpFPYScrapInfo = iSFPFPYScrapInfo(identity, start_time, end_time)
        return isfpFPYScrapInfo.getData() 

isfpOQCLightInfoNS = api.namespace('GetiSFPOQCLightInfo', description = '廠晨會看板- OQC 燈號')
isfpOQCLightInfoML = api.model('GetiSFPOQCLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpOQCLightInfoNS.route('', methods = ['POST'])
@isfpOQCLightInfoNS.response(200, 'Sucess')
@isfpOQCLightInfoNS.response(201, 'Created Sucess')
@isfpOQCLightInfoNS.response(204, 'No Content')
@isfpOQCLightInfoNS.response(400, 'Bad Request')
@isfpOQCLightInfoNS.response(401, 'Unauthorized')
@isfpOQCLightInfoNS.response(403, 'Forbidden')
@isfpOQCLightInfoNS.response(404, 'Not Found')
@isfpOQCLightInfoNS.response(405, 'Method Not Allowed')
@isfpOQCLightInfoNS.response(409, 'Conflict')
@isfpOQCLightInfoNS.response(500, 'Internal Server Error')
class GetOQCLightInfo(Resource):
    @isfpOQCLightInfoNS.doc('AppConfSysMain')
    @isfpOQCLightInfoNS.expect(isfpOQCLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpOQCLightInfo = iSFPOQCLightInfo(identity, start_time, end_time)
        return isfpOQCLightInfo.getData() 

isfpOQCInfoNS = api.namespace('GetiSFPOQCInfo', description = '廠晨會看板- OQC 判退推移圖')
isfpOQCInfoML = api.model('GetiSFPOQCInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpOQCInfoNS.route('', methods = ['POST'])
@isfpOQCInfoNS.response(200, 'Sucess')
@isfpOQCInfoNS.response(201, 'Created Sucess')
@isfpOQCInfoNS.response(204, 'No Content')
@isfpOQCInfoNS.response(400, 'Bad Request')
@isfpOQCInfoNS.response(401, 'Unauthorized')
@isfpOQCInfoNS.response(403, 'Forbidden')
@isfpOQCInfoNS.response(404, 'Not Found')
@isfpOQCInfoNS.response(405, 'Method Not Allowed')
@isfpOQCInfoNS.response(409, 'Conflict')
@isfpOQCInfoNS.response(500, 'Internal Server Error')
class GetOQCInfo(Resource):
    @isfpOQCInfoNS.doc('AppConfSysMain')
    @isfpOQCInfoNS.expect(isfpOQCInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpOQCInfo = iSFPOQCInfo(identity, start_time, end_time)
        return isfpOQCInfo.getData()        

isfpHotIPQAInfoNS = api.namespace('GetiSFPHotIPQAInfo', description = '廠晨會看板- 今日頭條 & IPQA(%) 區塊')
isfpHotIPQAInfoML = api.model('GetiSFPHotIPQAInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpHotIPQAInfoNS.route('', methods = ['POST'])
@isfpHotIPQAInfoNS.response(200, 'Sucess')
@isfpHotIPQAInfoNS.response(201, 'Created Sucess')
@isfpHotIPQAInfoNS.response(204, 'No Content')
@isfpHotIPQAInfoNS.response(400, 'Bad Request')
@isfpHotIPQAInfoNS.response(401, 'Unauthorized')
@isfpHotIPQAInfoNS.response(403, 'Forbidden')
@isfpHotIPQAInfoNS.response(404, 'Not Found')
@isfpHotIPQAInfoNS.response(405, 'Method Not Allowed')
@isfpHotIPQAInfoNS.response(409, 'Conflict')
@isfpHotIPQAInfoNS.response(500, 'Internal Server Error')
class GetHotIPQAInfo(Resource):
    @isfpHotIPQAInfoNS.doc('AppConfSysMain')
    @isfpHotIPQAInfoNS.expect(isfpHotIPQAInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpHotIPQAInfo = iSFPHotIPQAInfo(identity, start_time, end_time)
        return isfpHotIPQAInfo.getData()

isfpHoldLightInfoNS = api.namespace('GetiSFPHoldLightInfo', description = '廠晨會看板- Hold 燈號')
isfpHoldLightInfoML = api.model('GetiSFPHoldLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpHoldLightInfoNS.route('', methods = ['POST'])
@isfpHoldLightInfoNS.response(200, 'Sucess')
@isfpHoldLightInfoNS.response(201, 'Created Sucess')
@isfpHoldLightInfoNS.response(204, 'No Content')
@isfpHoldLightInfoNS.response(400, 'Bad Request')
@isfpHoldLightInfoNS.response(401, 'Unauthorized')
@isfpHoldLightInfoNS.response(403, 'Forbidden')
@isfpHoldLightInfoNS.response(404, 'Not Found')
@isfpHoldLightInfoNS.response(405, 'Method Not Allowed')
@isfpHoldLightInfoNS.response(409, 'Conflict')
@isfpHoldLightInfoNS.response(500, 'Internal Server Error')
class GetHoldLightInfo(Resource):
    @isfpHoldLightInfoNS.doc('AppConfSysMain')
    @isfpHoldLightInfoNS.expect(isfpHoldLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpHoldLightInfo = iSFPHoldLightInfo(identity, start_time, end_time)
        return isfpHoldLightInfo.getData()

isfpHoldInfoNS = api.namespace('GetiSFPHoldInfo', description = '廠晨會看板- Hold 推移圖')
isfpHoldInfoML = api.model('GetiSFPHoldInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpHoldInfoNS.route('', methods = ['POST'])
@isfpHoldInfoNS.response(200, 'Sucess')
@isfpHoldInfoNS.response(201, 'Created Sucess')
@isfpHoldInfoNS.response(204, 'No Content')
@isfpHoldInfoNS.response(400, 'Bad Request')
@isfpHoldInfoNS.response(401, 'Unauthorized')
@isfpHoldInfoNS.response(403, 'Forbidden')
@isfpHoldInfoNS.response(404, 'Not Found')
@isfpHoldInfoNS.response(405, 'Method Not Allowed')
@isfpHoldInfoNS.response(409, 'Conflict')
@isfpHoldInfoNS.response(500, 'Internal Server Error')
class GetHoldInfo(Resource):
    @isfpHoldInfoNS.doc('AppConfSysMain')
    @isfpHoldInfoNS.expect(isfpHoldInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpHoldInfo = iSFPHoldInfo(identity, start_time, end_time)
        return isfpHoldInfo.getData()

isfpScrapPInfoNS = api.namespace('GetiSFPScrapPInfo', description = '廠晨會看板- 破片報廢率(%)')
isfpScrapPInfoML = api.model('GetiSFPScrapPInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpScrapPInfoNS.route('', methods = ['POST'])
@isfpScrapPInfoNS.response(200, 'Sucess')
@isfpScrapPInfoNS.response(201, 'Created Sucess')
@isfpScrapPInfoNS.response(204, 'No Content')
@isfpScrapPInfoNS.response(400, 'Bad Request')
@isfpScrapPInfoNS.response(401, 'Unauthorized')
@isfpScrapPInfoNS.response(403, 'Forbidden')
@isfpScrapPInfoNS.response(404, 'Not Found')
@isfpScrapPInfoNS.response(405, 'Method Not Allowed')
@isfpScrapPInfoNS.response(409, 'Conflict')
@isfpScrapPInfoNS.response(500, 'Internal Server Error')
class GetScrapPInfo(Resource):
    @isfpScrapPInfoNS.doc('AppConfSysMain')
    @isfpScrapPInfoNS.expect(isfpScrapPInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpScrapPInfo = iSFPScrapPInfo(identity, start_time, end_time)
        return isfpScrapPInfo.getData()        

isfpFPYNLightInfoNS = api.namespace('GetiSFPFPYNLightInfo', description = '廠晨會看板- FPY(不含快修) 燈號')
isfpFPYNLightInfoML = api.model('GetiSFPFPYNLightInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpFPYNLightInfoNS.route('', methods = ['POST'])
@isfpFPYNLightInfoNS.response(200, 'Sucess')
@isfpFPYNLightInfoNS.response(201, 'Created Sucess')
@isfpFPYNLightInfoNS.response(204, 'No Content')
@isfpFPYNLightInfoNS.response(400, 'Bad Request')
@isfpFPYNLightInfoNS.response(401, 'Unauthorized')
@isfpFPYNLightInfoNS.response(403, 'Forbidden')
@isfpFPYNLightInfoNS.response(404, 'Not Found')
@isfpFPYNLightInfoNS.response(405, 'Method Not Allowed')
@isfpFPYNLightInfoNS.response(409, 'Conflict')
@isfpFPYNLightInfoNS.response(500, 'Internal Server Error')
class GetFPYNLightInfo(Resource):
    @isfpFPYNLightInfoNS.doc('AppConfSysMain')
    @isfpFPYNLightInfoNS.expect(isfpFPYNLightInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpFPYNLightInfo = iSFPFPYNLightInfo(identity, start_time, end_time)
        return isfpFPYNLightInfo.getData()

isfpFPYNInfoNS = api.namespace('GetiSFPFPYNInfo', description = '廠晨會看板- FPY(不含快修) 推移圖')
isfpFPYNInfoML = api.model('GetiSFPFPYNInfo', {
"COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
"SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
"FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'TEST', example = 'TEST'),
"START_TIME":fields.String( required = True, description = 'START_TIME', default = '20211123000000', example = '20211123000000'),
"END_TIME":fields.String( required = True, description = 'END_TIME', default = '20211128000000', example = '20211128000000'),
})
@isfpFPYNInfoNS.route('', methods = ['POST'])
@isfpFPYNInfoNS.response(200, 'Sucess')
@isfpFPYNInfoNS.response(201, 'Created Sucess')
@isfpFPYNInfoNS.response(204, 'No Content')
@isfpFPYNInfoNS.response(400, 'Bad Request')
@isfpFPYNInfoNS.response(401, 'Unauthorized')
@isfpFPYNInfoNS.response(403, 'Forbidden')
@isfpFPYNInfoNS.response(404, 'Not Found')
@isfpFPYNInfoNS.response(405, 'Method Not Allowed')
@isfpFPYNInfoNS.response(409, 'Conflict')
@isfpFPYNInfoNS.response(500, 'Internal Server Error')
class GetFPYNInfo(Resource):
    @isfpFPYNInfoNS.doc('AppConfSysMain')
    @isfpFPYNInfoNS.expect(isfpFPYNInfoML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        jsonData = BaseType.validateType(request.json)
        if "COMPANY_CODE" not in jsonData or "SITE" not in jsonData or "FACTORY_ID" not in jsonData:  
            return {'Result': 'NG','Reason':'Miss Parameter'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        identity = jsonData["COMPANY_CODE"] + "-" + jsonData["SITE"] + "-" + jsonData["FACTORY_ID"]
        start_time = jsonData["START_TIME"]
        end_time = jsonData["END_TIME"]
        log.logger.info(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        isfpFPYNInfo = iSFPFPYNInfo(identity, start_time, end_time)
        return isfpFPYNInfo.getData()

intSDETLNs = api.namespace('intSDETL', description = 'intSDETL')
intSDETLML_local = api.model('intSDETLML_local', {
    "COMPANY_CODE":fields.String( required = True, description = 'COMPANY_CODE', default = 'INX', example = 'INX'),
    "SITE":fields.String( required = True, description = 'SITE', default = 'TN', example = 'TN'),
    "FACTORY_ID":fields.String( required = True, description = 'FACTORY_ID', default = 'J001', example = 'J001'),
})
intSDETLML = api.model('intSDETL', {
    'DATATYPE': fields.String( required = True, description = 'DATATYPE', default = 'FPY', example = 'FPY'),
    "local":fields.Nested(intSDETLML_local),
    'ACCT_DATE': fields.String(required = True, description = 'ACCT_DATE', default = '20210715', example = '20210715'),
    'modeldata' : fields.String(required = True, description = 'modeldata', default = 'modeldata', example = intSDTemp)
    })
@intSDETLNs.route('', methods = ['POST'])
@intSDETLNs.response(200, 'Sucess')
@intSDETLNs.response(201, 'Created Sucess')
@intSDETLNs.response(204, 'No Content')
@intSDETLNs.response(400, 'Bad Request')
@intSDETLNs.response(401, 'Unauthorized')
@intSDETLNs.response(403, 'Forbidden')
@intSDETLNs.response(404, 'Not Found')
@intSDETLNs.response(405, 'Method Not Allowed')
@intSDETLNs.response(409, 'Conflict')
@intSDETLNs.response(500, 'Internal Server Error')
class intSDETL(Resource):
    @intSDETLNs.doc('intSiteDataETL')
    @intSDETLNs.expect(intSDETLML)
    def post(self):
        if not request:
            abort(400)
        elif not request.json:
            return {'Result':'NG', 'Reason': 'Input is Empty or Type is not JSON'}, 400,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        DBconfig = "INT_ORACLEDB_TEST"
        jsonData = BaseType.validateType(request.json)   
        ins = INTSDETL(DBconfig, jsonData)
        return ins.getData()

if __name__ == '__main__':
    app.run(threaded=True, use_reloader=True, host='0.0.0.0', port=5001, debug=False)#use_reloader=True,

