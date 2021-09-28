# -*- coding: utf-8 -*-
import json
import operator
from re import X
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
from Dao import DaoHelper,ReadConfig
from decimal import Decimal, ROUND_HALF_UP


class INTRelation(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        #INT_ORACLEDB_PROD / INT_ORACLEDB_TEST
        self.DBconfig = "INT_ORACLEDB_TEST"

    def getData(self):
        try:
            self.writeLog(
                f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Input Json:{self.jsonData}')
            bottomLine = "_"
            redisKey = ""
            tmp = []

            className = f"{self.__class__.__name__}"            
            tmpFuncType = self.jsonData["FUNCTYPE"]
            tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
            tmpSITE = self.jsonData["SITE"]
            tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
            tmpAPPLICATION = self.jsonData["APPLICATION"]
            tmpACCT_DATE = self.jsonData["ACCT_DATE"]
            tmpPROD_NBR = self.jsonData["PROD_NBR"]
            tmpOPER = self.jsonData["OPER"] # or RESPTYPE
            # Defect or Reason Code 
            tmpCHECKCODE = self.jsonData["CHECKCODE"] if "CHECKCODE" in self.jsonData else None
            expirSecond = 3600

            #redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpFuncType)
            tmp.append(tmpACCT_DATE)
            tmp.append(tmpPROD_NBR)
            tmp.append(tmpOPER)
            if tmpCHECKCODE != None:
                tmp.append(tmpCHECKCODE)
            redisKey = bottomLine.join(tmp)

            if tmpFACTORY_ID not in ["J001"]:
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            """
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=expirSecond)).timetuple()), "DataSource": "Redis"}
            """
            if tmpFuncType == "REASON":
                #取得 當日 panel his
                whereString = f" PROD_NBR= '{tmpPROD_NBR}' "
                sql = f"with panel_his_daily as (select * from INTMP_DB.PANELHISDAILY where {whereString}), " \
                      f"panel_his_mat as (select * from INTMP_DB.PANELHISDAILY_MAT where {whereString}) " \
                      "select * from panel_his_daily order by PANELID, TRANSDT asc"
                 
                self.getConnection(self.DBconfig)
                data = self.Select(sql)
                self.closeConnection()
                calData = []
                if(len(data) != 0):
                    for da in data:
                        d = datetime.datetime
                        TIMECLUST_d = d.strptime(da[3],'%Y%m%d%H%M%S')
                        TIMECLUST = d.strftime(TIMECLUST_d,'%Y%m%d%H')
                        datadict={                        
                            "PROD_NBR" : da[5],                        
                            "MFGDATE" : da[2],
                            "PANELID" : da[0],
                            "OPER" : da[1],
                            "TRANSDT" : da[3],
                            "OPERATOR" : da[7],
                            "EQPID" : da[8],
                            "RW_COUNT" : da[9],
                            "OUTPUT_FG" : da[4],
                            "TIMECLUST" : TIMECLUST
                        }
                        calData.append(datadict)

                C_DESC = self._code2Desc(tmpCHECKCODE)
                returnData = {                    
                    "RELATIONTYPE": tmpFuncType,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "C_CODE": tmpCHECKCODE,
                    "C_DESCR": C_DESC if C_DESC != None else tmpCHECKCODE,
                    "DATASERIES": calData
                }
                """
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                """
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            else:
                self.closeConnection()
                return {'Result': 'Fail', 'Reason': 'Parametes[KPITYPE] not in Rule'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

        except Exception as e:
            self.closeConnection()
            error_class = e.__class__.__name__  # 取得錯誤類型
            detail = e.args[0]  # 取得詳細內容
            cl, exc, tb = sys.exc_info()  # 取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
            fileName = lastCallStack[0]  # 取得發生的檔案名稱
            lineNum = lastCallStack[1]  # 取得發生的行號
            funcName = lastCallStack[2]  # 取得發生的函數名稱
            self.writeError(
                f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
            return {'Result': 'NG', 'Reason': f'{funcName} erro'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
    
    def _code2Desc(self, C_CODE):
        sql = f"select REASONCODE_DESC from INTMP_DB.REASONCODE where REASONCODE = '{C_CODE}'"
        #INT_ORACLEDB_PROD
        self.getConnection(self.DBconfig)
        data = self.Select(sql)
        self.closeConnection()
        returnString = None
        if(len(data) != 0):
            returnString = data[0][0]
        return returnString


