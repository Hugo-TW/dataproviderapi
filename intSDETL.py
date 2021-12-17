# -*- coding: utf-8 -*-
import json
from re import X
import sys
import traceback
import requests
from Dao import DaoHelper,ReadConfig
from Logger import Logger
from BaseType import BaseType
import datetime

class INTSDETL(BaseType):
    def __init__(self, DBconfig, jsonData):
        super().__init__()     
        self.__log = Logger('./log/' + self.__class__.__name__ + '.log',level='debug')
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.DBconfig = "INT_ORACLEDB_TEST"
        self.jsonData = jsonData
        self.baseConfig = {
            "server":{'bootstrap.servers': 'idts-kafka1.cminl.oa','message.max.bytes':15728640},
            "topic":"intsdetl",
            "headers":{'Content-type':'application/json','Connection':'close'},
        }

    def getData(self):
        try:
            self.writeLog(
                f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            className = f"{self.__class__.__name__}"   

            cData = self._dict_to_capital(self.jsonData)
            if cData["DATATYPE"] == "":
                return {'status': 'Fail','message': f'Input Data format error'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            tmpDATATYPE = cData["DATATYPE"]
            #一階 FPY KPI API
            if tmpDATATYPE == "FPY" or tmpDATATYPE == "MSHIP" or tmpDATATYPE == "EFA":   
                if len(cData["MODELDATA"]) == 0:
                    return {'status': 'Fail','message': f'Input Data format error'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
                
                returnData= self._insertData(cData) 
                returnData = self._sendDataToKafka(cData)
                return returnData, returnData["status_code"], {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
            #一階 FPY KPI API
            elif tmpDATATYPE == "DEFTREL" or tmpDATATYPE == "REASONREL":   
                if len(cData["RELDATA"]) == 0:
                    return {'status': 'Fail','message': f'Input Data format error'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
                
                returnData= self._insertData_REL(cData) 
                returnData = self._sendDataToKafka_REL(cData)
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

    def _insertData_REL(self,DATA):
        try:
            _COMPANY_CODE = DATA["LOCAL"]["COMPANY_CODE"]
            _SITE= DATA["LOCAL"]["SITE"]
            _FACTORY_ID = DATA["LOCAL"]["FACTORY_ID"]
            _ACCT_DATE = DATA["ACCT_DATE"]
            _DATATYPE = DATA["DATATYPE"]
            dData = {
                "COMPANY_CODE": _COMPANY_CODE,
                "SITE": _SITE,
                "FACTORY_ID": _FACTORY_ID,
                "ACCT_DATE": _ACCT_DATE,
                "DATATYPE": _DATATYPE
            }
            d = datetime.datetime
            dString = d.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S')
            iData = {
                "COMPANY_CODE": _COMPANY_CODE,
                "SITE": _SITE,
                "FACTORY_ID": _FACTORY_ID,
                "ACCT_DATE": _ACCT_DATE,
                "DATATYPE": _DATATYPE,                
                "DATAGETTIME": dString,
                "ORIGDATA": DATA
            }

            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("SDETLUPLOADLOG")
            count = self.deleteToMongo(dData)
            self.inserOneToMongo(iData)
            self.closeMongoConncetion()

            returnData = {
                        "status_code": 201,
                        "status": "success",
                        "data": f'get {len(DATA["RELDATA"])} record in RELDATA',
                        "message": ""
                    }  
            self.writeLog(f'{_COMPANY_CODE}-{_SITE}-{_FACTORY_ID}-{_ACCT_DATE}-{_DATATYPE}: {returnData}')
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
   
    def _sendDataToKafka_REL(self, DATA): 
        self.writeLog(f'Data1 Length:{len(DATA)}')        
        d = datetime.datetime
        dString = d.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S')
        DATA["DATAGETTIME"] = dString
        server = self.baseConfig["server"]
        topic = self.baseConfig["topic"]
        self.producer = self.createProducer(server)
        self.producerSend(topic, DATA) 
        _COMPANY_CODE = DATA["LOCAL"]["COMPANY_CODE"]
        _SITE= DATA["LOCAL"]["SITE"]
        _FACTORY_ID = DATA["LOCAL"]["FACTORY_ID"]
        _ACCT_DATE = DATA["ACCT_DATE"]
        _DATATYPE = DATA["DATATYPE"]
        returnData = {
                        "status_code": 201,
                        "status": "success",
                        "data": f'get {len(DATA["RELDATA"])} record in RELDATA',
                        "message": ""
                    }  
        self.writeLog(f'{_COMPANY_CODE}-{_SITE}-{_FACTORY_ID}-{_ACCT_DATE}-{_DATATYPE}: {returnData}')
        DATA = []
        return returnData
    
    def _insertData(self,DATA):
        try:
            _COMPANY_CODE = DATA["LOCAL"]["COMPANY_CODE"]
            _SITE= DATA["LOCAL"]["SITE"]
            _FACTORY_ID = DATA["LOCAL"]["FACTORY_ID"]
            _ACCT_DATE = DATA["ACCT_DATE"]
            _DATATYPE = DATA["DATATYPE"]
            dData = {
                "COMPANY_CODE": _COMPANY_CODE,
                "SITE": _SITE,
                "FACTORY_ID": _FACTORY_ID,
                "ACCT_DATE": _ACCT_DATE,
                "DATATYPE": _DATATYPE
            }
            d = datetime.datetime
            dString = d.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S')
            iData = {
                "COMPANY_CODE": _COMPANY_CODE,
                "SITE": _SITE,
                "FACTORY_ID": _FACTORY_ID,
                "ACCT_DATE": _ACCT_DATE,
                "DATATYPE": _DATATYPE,                
                "DATAGETTIME": dString,
                "ORIGDATA": DATA
            }

            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("SDETLUPLOADLOG")
            count = self.deleteToMongo(dData)
            self.inserOneToMongo(iData)
            self.closeMongoConncetion()

            returnData = {
                        "status_code": 201,
                        "status": "success",
                        "data": f'get {len(DATA["MODELDATA"])} record in modeldata',
                        "message": ""
                    }  
            self.writeLog(f'{_COMPANY_CODE}-{_SITE}-{_FACTORY_ID}-{_ACCT_DATE}-{_DATATYPE}: {returnData}')
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
   
    def _sendDataToKafka(self, DATA): 
        self.writeLog(f'Data1 Length:{len(DATA)}')        
        d = datetime.datetime
        dString = d.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S')
        DATA["DATAGETTIME"] = dString
        server = self.baseConfig["server"]
        topic = self.baseConfig["topic"]
        self.producer = self.createProducer(server)
        self.producerSend(topic, DATA) 
        _COMPANY_CODE = DATA["LOCAL"]["COMPANY_CODE"]
        _SITE= DATA["LOCAL"]["SITE"]
        _FACTORY_ID = DATA["LOCAL"]["FACTORY_ID"]
        _ACCT_DATE = DATA["ACCT_DATE"]
        _DATATYPE = DATA["DATATYPE"]
        returnData = {
                        "status_code": 201,
                        "status": "success",
                        "data": f'get {len(DATA["MODELDATA"])} record in modeldata',
                        "message": ""
                    }  
        self.writeLog(f'{_COMPANY_CODE}-{_SITE}-{_FACTORY_ID}-{_ACCT_DATE}-{_DATATYPE}: {returnData}')
        DATA = []
        return returnData

    def _dict_to_capital(self, dict_info):
        new_dict = {}
        if type(dict_info) != dict:
            return dict_info
        else:
            for i, j in dict_info.items():
                if type(j) == list:
                    new_dict2 = []
                    for x in j:
                        new_dict2.append(self._dict_to_capital(x))
                    new_dict[i.upper()] = new_dict2
                elif type(j) == dict:
                    new_dict[i.upper()] = self._dict_to_capital(j)
                else:
                    new_dict[i.upper()] = j
            return new_dict
             
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
