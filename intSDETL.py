# -*- coding: utf-8 -*-
import json
import operator
from re import X
import sys
import traceback
import time
import datetime
import copy
import decimal
from Dao import DaoHelper,ReadConfig
from Logger import Logger

class INTSDETL():
    def __init__(self, DBconfig, jsonData):
        super().__init__()     
        self.__log = Logger('./log/' + self.__class__.__name__ + '.log',level='debug')
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.DBconfig = "INT_ORACLEDB_TEST"
        self.jsonData = jsonData

    def SetData(self):
        try:
            self.writeLog(
                f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            className = f"{self.__class__.__name__}"           
            tmpDATATYPE = self.jsonData["DATATYPE"]

            #一階 FPY KPI API
            if tmpDATATYPE == "FPY" or tmpDATATYPE == "MSHIP":                   
                returnData= self._insertData(self.jsonData) 
                return returnData, returnData["status_code"], {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
            else:
                return {'status': 'Fail','message': f'DATATYPE:{tmpDATATYPE} not Sup'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

        except Exception as e:
            error_class = e.__class__.__name__  # 取得錯誤類型
            detail = e.args[0]  # 取得詳細內容
            cl, exc, tb = sys.exc_info()  # 取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
            fileName = lastCallStack[0]  # 取得發生的檔案名稱
            lineNum = lastCallStack[1]  # 取得發生的行號
            funcName = lastCallStack[2]  # 取得發生的函數名稱
            self.writeError(
                f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
            return {'status': 'error', 'message': f'{funcName} erro'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

    
    def _insertData(self,DATA):
        try:
            _COMPANY_CODE = DATA["local"]["COMPANY_CODE"]
            _SITE= DATA["local"]["SITE"]
            _FACTORY_ID = DATA["local"]["FACTORY_ID"]
            _ACCT_DATE = DATA["ACCT_DATE"]
            _DATATYPE = DATA["DATATYPE"]
            _DATA = f'{DATA}'.encode()
            insertData = []
            oData = (
                    _COMPANY_CODE,
                    _SITE,
                    _FACTORY_ID, 
                    _ACCT_DATE,
                    _DATATYPE,
                    _DATA
                )
            insertData.append(oData)
            """
            CREATEDATA	DATE
            COMPANY_CODE	VARCHAR2(20 BYTE)
            SITE	VARCHAR2(20 BYTE)
            FACTORY_ID	VARCHAR2(20 BYTE)
            ACCT_DATE	VARCHAR2(20 BYTE)
            ORIGDATA	BLOB
            DATATYPE	VARCHAR2(20 BYTE)
            """
            delString = f"delete from INTMP_DB.SDETLUPLOADLOG where COMPANY_CODE = '{_COMPANY_CODE}' \
                and SITE = '{_SITE}' and FACTORY_ID = '{_FACTORY_ID}' and ACCT_DATE = '{_ACCT_DATE}' \
                and DATATYPE = '{_DATATYPE}' "
            insertString = "insert into INTMP_DB.SDETLUPLOADLOG("\
                "COMPANY_CODE,SITE,FACTORY_ID,ACCT_DATE,DATATYPE, ORIGDATA) "\
                "values (:1, :2, :3, :4, :5, :6)"
            self._getConnection(self.DBconfig)
            self._daoHelper.Delete(delString)
            self._daoHelper.InserMany(insertString,insertData)
            #SELECT JSON_SERIALIZE(ORIGDATA) AS data FROM sdetluploadlog
            selString = f"Select * from INTMP_DB.SDETLUPLOADLOG where COMPANY_CODE = '{_COMPANY_CODE}' \
                and SITE = '{_SITE}' and FACTORY_ID = '{_FACTORY_ID}' and ACCT_DATE = '{_ACCT_DATE}' \
                and DATATYPE = '{_DATATYPE}' "
            data = self._daoHelper.Select(selString)
            cc = data[0][5]
            check = eval(cc.read().decode())            
            returnData = {
                        "status_code": 201,
                        "status": "success",
                        "data": f'get {len(check["modeldata"])} record in modeldata',
                        "message": ""
                    }  
            return returnData

        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
            returnData = {"status_code": 400,'status': 'error', 'message': f'{funcName} error'}
            return returnData

    """Oracle DB"""
    def _getConnection( self, identity ):
        """連接資料庫
        identity : 資料庫名稱
        """
        try:
            self._getDbAccount(identity)
            self._daoHelper = DaoHelper(self._dbAccount, self._dbPassword,self._SERVICE_NAME)
            self._connect = self._daoHelper.Connect()
            #return self.__dbAccount, self.__dbPassword, self.__SERVICE_NAME
        except Exception as e:
            self._closeConnection()
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))
        
    def _getDbAccount( self, identity ):
        """ 取得資料庫帳密
            identity : 資料庫名稱
            回傳 : dbAccount、dbPassword、SERVICE_NAME
        """
        try:
            self._dbAccount, self._dbPassword, self._SERVICE_NAME = ReadConfig('config.json',identity).READ()
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(fileName, lineNum, funcName, error_class, detail))  
    def _closeConnection( self ):
        """關閉資料庫連線"""
        self._daoHelper.Close()

    def writeLog( self,text ):
        """Info Log"""
        self.__log.logger.info(text)
    def writeError( self, text ):
        """Error Log"""
        self.__log.logger.error(text)
    def writeWarning( self, text ):
        self.__log.logger.warning(text)
    def writeDebug( self, text ):
        self.__log.logger.debug(text)
    def writeCritical( self, text ):
        self.__log.logger.critical(text)