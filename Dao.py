# -*- coding: utf-8 -*-
import cx_Oracle
import time, datetime
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import json
import sys
import traceback
from Logger import Logger
log = Logger('./log/cx_Oracle.log',level='debug')
#https://blog.csdn.net/qq_36743482/article/details/82593945 
class DaoHelper():
    def __init__(self,dbAccount, dbPassword,SERVICE_NAME):
        #log.logger.info('DaoHelper __init__')
        self.dbAccount=dbAccount
        self.dbPassword=dbPassword
        self.SERVICE_NAME=SERVICE_NAME
        self.conn=None
        self.cur=None
    def Connect(self):
        try:
            #log.logger.info('DaoHelper Connect')
            self.conn = cx_Oracle.connect(self.dbAccount, self.dbPassword,self.SERVICE_NAME)
            self.cur = self.conn.cursor()
            #log.logger.info('dbAccount:'+self.dbAccount)
            #log.logger.info('dbPassword:'+self.dbPassword)
            #log.logger.info('SERVICE_NAME:'+self.SERVICE_NAME) 
        except cx_Oracle.DatabaseError  as e:         
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            self.cur.close()
            self.conn.close()
            return detail
        except Exception as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Select(self,sql):
        try:
            #log.logger.info('DaoHelper Select')
            #log.logger.info('SQL:\n'+sql)
            self.cur.execute(sql)
            return self.cur.fetchall()
        except cx_Oracle.DatabaseError  as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.cur.close()
            self.conn.close()
            return e
    def Select(self,sql,dictparam:dict={}):
        try:
            #log.logger.info('DaoHelper Select')
            #log.logger.info('SQL:\n'+sql)
            self.cur.prepare(sql)
            self.cur.execute(None,dictparam)
            return self.cur.fetchall()
        except cx_Oracle.DatabaseError  as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.cur.close()
            self.conn.close()
            return e
    def SelectAndDescription(self,sql):
        try:
            #log.logger.info('DaoHelper SelectAndDescription')
            #log.logger.info('SQL:\n'+sql)
            #self.cur.prepare(sql)
            self.cur.execute(sql)
            return self.cur.description,self.cur.fetchall()
        except cx_Oracle.DatabaseError  as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail,lineNum
        except Exception as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail,lineNum
    def SelectAndDescription(self,sql,dictparam:dict={}):
        try:
            #log.logger.info('DaoHelper SelectAndDescription')
            #log.logger.info('SQL:\n'+sql)
            self.cur.prepare(sql)
            self.cur.execute(None,dictparam)
            return self.cur.description,self.cur.fetchall()
        except cx_Oracle.DatabaseError  as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail,lineNum
        except Exception as e:
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail,lineNum
    def Update(self,sql):
        try:
            #log.logger.info('DaoHelper Update')
            #log.logger.info('SQL:\n'+sql)
            self.cur.execute(sql)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail,lineNum
    def Update(self,sql,dictparam:dict={}):
        try:
            #log.logger.info('DaoHelper Update')
            #log.logger.info('SQL:\n'+sql)
            self.cur.prepare(sql)
            self.cur.execute(None,dictparam)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Delete(self,sql):
        try:
            #log.logger.info('DaoHelper Delete')
            #log.logger.info('SQL:\n'+sql)
            self.cur.execute(sql)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Delete(self,sql,dictparam:dict={}):
        try:
            #log.logger.info('DaoHelper Delete')
            #log.logger.info('SQL:\n'+sql)
            self.cur.prepare(sql)
            self.cur.execute(None,dictparam)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Insert(self,sql):
        try:
            #log.logger.info('DaoHelper Insert')
            #log.logger.info('SQL:\n'+sql)
            self.cur.execute(sql)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Insert(self,sql,dictparam:dict={}):
        try:
            #log.logger.info('DaoHelper Insert')
            #log.logger.info('SQL:\n'+sql)
            self.cur.prepare(sql)
            self.cur.execute(None,dictparam)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def InserMany(self,sql,param):
        try:
            #log.logger.info('DaoHelper InserMany')
            #log.logger.info('SQL:\n'+sql)
            #log.logger.info('PARAM:\n'+param)
            self.cur.executemany(sql, param)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail         
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail           
    def InsertToClob(self,sql,data,param):
        try:
            #log.logger.info('DaoHelper InsertToClob')
            #log.logger.info('SQL:\n'+sql)
            clob=self.cur.var(cx_Oracle.CLOB)
            clob.setvalue(0,data)
            self.cur.prepare(sql)
            self.cur.execute(None,param)
            self.conn.commit()
            return clob
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def executeBatch(self, sqlList):
        try:
            #log.logger.info('DaoHelper executeBatch')
            #log.logger.info('SQL:\n'+sqlList)
            for s in sqlList:
                self.cur.execute(s)
                # if(s.startswith("Delete")):
                #     self.cur.execute(s)
            self.conn.commit()
        except cx_Oracle.DatabaseError  as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
        except Exception as e:
            self.conn.rollback()
            self.cur.close()
            self.conn.close()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
    def Close(self):
        try:
            #log.logger.info('DaoHelper Close')
            self.cur.close()
            self.conn.close()
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            log.logger.error("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            return detail
class ReadConfig():
    def __init__(self,filename,identity):
        self.identity=identity
        self.filename=filename
    def READ(self):
        with open(self.filename) as f:
            data = json.load(f)
            dbAccount=None
            dbPassword=None
            SERVICE_NAME=None
            try:  
                for config in data["config_list"]:
                    iden=config["identity"]
                    if self.identity == iden:
                        dbAccount=config["dbAccount"]
                        dbPassword=config["dbPassword"]
                        SERVICE_NAME=config["SERVICE_NAME"]
                        break
                return dbAccount,dbPassword,SERVICE_NAME
            except:
                return None,None,None