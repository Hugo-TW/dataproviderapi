# -*- coding: utf-8 -*-
from flask import Flask, jsonify
import json
from Dao import DaoHelper
from Logger import Logger
log = Logger('ALL.log',level='debug')
def ReadConfig(identity):
    with open('config.json') as f:
        data = json.load(f)
        dbAccount=None
        dbPassword=None
        SERVICE_NAME=None
        try:
            for config in data["config_list"]:
                iden=config["identity"]
                if identity == iden:
                    dbAccount=config["dbAccount"]
                    dbPassword=config["dbPassword"]
                    SERVICE_NAME=config["SERVICE_NAME"]
                    break
            return dbAccount,dbPassword,SERVICE_NAME
        except:
            return None,None,None
class Pack:
    def __init__(self,SITE,DAILY_OEE):
        self.SITE=SITE
        self.DAILY_OEE=DAILY_OEE
    def __repr__(self):
        return repr(self.SITE,self.DAILY_OEE)
class PackT:
    def __init__(self,rowCount,lists):
        self.rowCount=rowCount
        self.lists=lists
    def __repr__(self):
        return repr(self.rowCount,self.lists)
class KpiSite():
    def __init__(self,indentity):
        log.logger.info('KpiSite __init__')
        self.indentity=indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig(self.indentity)
    def GetJsonData(self):
        try:
            log.logger.info('KpiSite GetJsonData Start')
            daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            sql="select site, round(daily_oee, 0) || '%' as DAILY_OEE from dcs_mtr_site_oee_v"
            log.logger.info('SQL:'+sql)
            daoHelper.Connect()
            #取得資料值組
            data=daoHelper.Select(sql)
            daoHelper.Close()
            datajson=[]
            Test=""
            for da in data:
                datajson.append(Pack(da[0],da[1]))
            Test=PackT(len(data),datajson)
            data=json.dumps(Test, default=lambda o: o.__dict__, sort_keys=True, indent=2)
            #print(data)
            log.logger.info('Json: '+str(data))
            log.logger.info('KpiSite GetJsonData DONE')
            log.logger.info('Route /KpiSiteOEE DONE')
            return data,200,{"Content-Type": "application/json",'Connection':'close'}
        except Exception as e:
            log.logger.error(e)
            return jsonify({'Result': 'NG','Reason':e}),400 ,{"Content-Type": "application/json",'Connection':'close'}
