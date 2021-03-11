# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
from redis.sentinel import Sentinel
from Dao import DaoHelper 
from flask_cors import CORS
from Logger import Logger
log = Logger('ALL.log',level='debug')
class PACK:
    def __init__(self,companycode,site,mapid,factoryid,datatype,datavalue,lineid):
        self.companycode=companycode
        self.site=site
        self.mapid=mapid
        self.factoryid=factoryid
        self.datatype=datatype
        self.datavalue=datavalue
        self.lineid=lineid
    def __repr__(self):
        return repr(self.companycode,self.site,self.mapid,self.factoryid,self.datatype,self.datavalue,self.lineid)
#選擇使用SQL
def SwitchSQL(pack,companycode,site,mapid,condition):
    switcher={
        'pack':"select * from dcs_kpi_status where company_code='{0}'and site='{1}'and mapid='{2}'".format(companycode,site,mapid)+" "+condition
    }
    return switcher.get(pack,None)
#選擇json的Class
def SeletClass(pack,tuples):
    swichter={
        "pack":PACK(tuples[0],tuples[1],tuples[2],tuples[3],tuples[4],tuples[5],tuples[6])
    }
    return swichter.get(pack,None)
#讀取DB config檔
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
class Kpi():
    def __init__(self,identity,companycode,site,mapid,lineid,factoryid,Ocondition,Rcondition):
        log.logger.info('Kpi __init__')
        self.identity=identity#資料庫識別碼
        self.companycode=companycode
        self.site=site
        self.mapid=mapid
        self.lineid=lineid
        self.factoryid=factoryid
        self.data=None
        self.datajosn=[]
        self.Rcheck=False#Redis data check
        self.Ocheck=False#Oracle data check
        self.dbAccount,self.dbPassword,self.SERVICE_NAME=ReadConfig(self.identity)
        self.Ocondition=Ocondition#Oracle sql 條件式
        self.Rcondition=Rcondition#Redis Key 條件式
        self.sql=""
    def GetJsonData(self):
        log.logger.info('Kpi GetJsonData Start')
        #Redis Key
        mbtopic = self.companycode + "-" + self.site + "-" + self.mapid+self.Rcondition
        
        self.sql=SwitchSQL('pack',self.companycode,self.site,self.mapid,self.Ocondition)
        log.logger.info('SQL:'+self.sql)
        if self.sql is None:
            return jsonify({'Result': 'NG', 'Reason': 'DATATYPE IS NOT DEFINE'}),401

         #Redis哨兵
        sential=Sentinel([('10.55.8.62',26379)],socket_timeout=0.1,retry_on_timeout=0.1)  
        master1=sential.master_for('master1',socket_timeout=0.1)
        #刪除Redis Key
        master1.delete(mbtopic)
        #取得所有key name
        keys=master1.keys()
        for key in keys:
            ke=str(key,encoding = "utf-8")
            if mbtopic ==ke:#比對 key name是否存在    
                data=master1.get(mbtopic).decode('utf-8')
                self.Rcheck=True
                break
            else:
                self.Rcheck=False
        #Check data is exist or not and data is not in redis
        if self.data is None and self.Rcheck is False:
            #連接資料庫
            #dbAccount,dbPassword,SERVICE_NAME=ReadConfig(identity)
            if self.dbAccount is not None and self.dbPassword is not None and self.SERVICE_NAME is not None:
                daoHelper=DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
                daoHelper.Connect()
                #取得資料值組
                self.data=daoHelper.Select(self.sql)
                daoHelper.Close()
                if self.data is not None:
                    for da in self.data:
                        #轉Json
                        self.datajosn.append(SeletClass('pack',da))
                
                    self.data = json.dumps(self.datajosn, default=lambda o: o.__dict__, sort_keys=True, indent=1)
                    log.logger.info('Json:'+json.loads(self.data))
                    print(self.data)
                    self.Ocheck=True
                else:
                   self.Ocheck=False 
            else:
                log.logger.error('Db schema is not define in config.json')
                return jsonify({'Result': 'NG', 'Reason': 'CONFIG IS NOT DEFINE'}), 401
        if self.Ocheck is True and self.Rcheck is False:
            log.logger.info("Insert Data to Redis")
            master1.set(mbtopic,self.data)
        if self.Ocheck is False and self.Rcheck is False:
            log.logger.error('Data is not exist in Oracle or Redis')
            return jsonify({'Result': 'NG', 'Reason': 'DATA IS NOT EXIST'}),401
        log.logger.info("Kpi GetJsonData DONE")
        log.logger.info('Route /mb/api/DataProviderApi DONE')
        return self.data, 200,{"Content-Type": "application/json",'Connection':'close'}
