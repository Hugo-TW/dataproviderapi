# -*- coding: utf-8 -*-
import sys
import traceback
import json
from Dao import DaoHelper,ReadConfig
import cx_Oracle
import time, datetime
import os
from flask import Flask, jsonify
from Logger import Logger
log = Logger('ALL.log',level='debug')
class PackOne:
    def __init__(self,ACCOUNT,AUTHENTICATION_IMPL,PASSWORD,IS_AD_ACCOUNT,AUTH_LIST):
        self.ACCOUNT=ACCOUNT
        self.AUTHENTICATION_IMPL=AUTHENTICATION_IMPL
        self.PASSWORD=PASSWORD
        self.IS_AD_ACCOUNT=IS_AD_ACCOUNT
        self.AUTH_LIST=AUTH_LIST
        if  self.ACCOUNT is None:
             self.ACCOUNT=""
        if self.AUTHENTICATION_IMPL is None:
            self.AUTHENTICATION_IMPL=""
        if self.PASSWORD is None:
            self.PASSWORD=""
        if self.IS_AD_ACCOUNT is None:
            self.IS_AD_ACCOUNT=""
    def __repr__(self):
        return (self.ACCOUNT,self.AUTHENTICATION_IMPL,self.PASSWORD,self.IS_AD_ACCOUNT,self.AUTH_LIST)
class PackTwo:
    def __init__(self,AUTH_LIST):
        self.AUTH_LIST=AUTH_LIST
    def __repr__(self):
        return (self.AUTH_LIST)
class PackThree:
    def __init__(self,FUNC_ID,PAGE_FILENAME,SITE):
        self.FUNC_ID=FUNC_ID
        self.PAGE_FILENAME=PAGE_FILENAME
        self.SITE=SITE
        if self.FUNC_ID is None:
            self.FUNC_ID=""
        if self.PAGE_FILENAME is None:
            self.PAGE_FILENAME=""
        if self.SITE is None:
            self.SITE=""
    def __repr__(self):
        return (self.FUNC_ID,self.PAGE_FILENAME,self.SITE)
class AuthListByAccount():
    def __init__(self,account,identity):
        log.logger.info('AuthListByAccount __init__')
        self.account=account
        self.identity=identity       
    def GetDataJson(self):
        try:
            log.logger.info('AuthListByAccount GetDataJson Start')
            Test=""
            datajson=[]
            sql="SELECT ul.account,ul.authentication_impl,ul.password,ul.is_ad_account,ul.func_id,ul.PAGE_FILENAME,ul.site FROM AUTH_USER_LIST_V ul WHERE ul.account = UPPER ('{0}')".format(self.account)
            log.logger.info('SQL: '+sql)
            dbAccount,dbPassword,SERVICE_NAME=ReadConfig('config.json',self.identity).READ()
            daohelper=DaoHelper(dbAccount,dbPassword,SERVICE_NAME)
            daohelper.Connect()
            data=daohelper.Select(sql)
            daohelper.Close()
            account=None
            auth=None
            pwd=None
            isad=None
            for da in data:
                account=da[0]
                auth=da[1]
                pwd=da[2]
                isad=da[3]
                datajson.append(PackThree(da[4],da[5],da[6]))
            #datajson=PackTwo(datajson)
            Test=PackOne(account,auth,pwd,isad,datajson)
            dat=json.dumps(Test, default=lambda o: o.__dict__, sort_keys=True, indent=2)
            log.logger.info('Json:\n'+str(dat))
            log.logger.info('AuthListByAccount GetDataJson DONE')
            log.logger.info('Route /GetAuthListByAccount DONE')
            return dat,200,{"Content-Type": "application/json",'Connection':'close'}
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