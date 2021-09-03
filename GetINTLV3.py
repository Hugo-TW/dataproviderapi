# -*- coding: utf-8 -*-
import json
from re import X
import sys
import traceback
import time
import datetime
import copy
import operator
import configparser

from flask_restplus.utils import not_none
from BaseType import BaseType

class INTLV3(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        #M011 => MOD1
        #J001 => MOD2
        #J003 => MOD3
        self.operSetData = {
            "M011": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim": 1000, "FPY": 0.95}
                    },
                    "numerator": {  # 分子
                        "PCBI": {"fromt": 1050, "tot": 1310},
                        "LAM": {"fromt": 1340, "tot": 1399},
                        "AAFC": {"fromt": 1400, "tot": 1499},
                        "CKEN": {"fromt": 1500, "tot": 1699},
                        "DKEN": {"fromt": 1700, "tot": 1799}
                    },
                    "denominator": {  # 分母
                        "PCBI": [1300, 1301],
                        "LAM": [1355],
                        "AAFC": [1420],
                        "CKEN": [1600],
                        "DKEN": [1600]
                    }
                },
                "M-SHIP": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.97},
                        "TABLET": {"qytlim": 1000, "target": 0.955},
                        "NB": {"qytlim": 1000, "target": 0.96},
                        "TV": {"qytlim": 1000, "target": 0.90},
                        "AA": {"qytlim": 1000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
                        "TV": {"qytlim": 1000, "target": 0.003},
                        "AA": {"qytlim": 1000, "target": 0.003}
                    },
                    "numerator": {},
                    "denominator": {}
                }
            },
            "J001": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim": 1000, "FPY": 0.95}
                    },
                    "numerator": {  # 分子
                        "PCBI": {"fromt": 1050, "tot": 1310},
                        "LAM": {"fromt": 1340, "tot": 1399},
                        "AAFC": {"fromt": 1400, "tot": 1499},
                        "CKEN": {"fromt": 1500, "tot": 1699},
                        "DKEN": {"fromt": 1700, "tot": 1799}
                    },
                    "denominator": {  # 分母
                        "PCBI": [1300, 1301],
                        "LAM": [1355],
                        "AAFC": [1420],
                        "CKEN": [1600],
                        "DKEN": [1600]
                    }
                },
                "M-SHIP": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.97},
                        "TABLET": {"qytlim": 1000, "target": 0.955},
                        "NB": {"qytlim": 1000, "target": 0.96},
                        "TV": {"qytlim": 1000, "target": 0.90},
                        "AA": {"qytlim": 1000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
                        "TV": {"qytlim": 1000, "target": 0.003},
                        "AA": {"qytlim": 1000, "target": 0.003}
                    },
                    "numerator": {},
                    "denominator": {}
                }
            },
            "J003": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim": 1000, "FPY": 0.95}
                    },
                    "numerator": {  # 分子
                        "PCBI": {"fromt": 1050, "tot": 1310},
                        "LAM": {"fromt": 1340, "tot": 1399},
                        "AAFC": {"fromt": 1400, "tot": 1499},
                        "CKEN": {"fromt": 1500, "tot": 1699},
                        "DKEN": {"fromt": 1700, "tot": 1799}
                    },
                    "denominator": {  # 分母
                        "PCBI": [1300, 1301],
                        "LAM": [1355],
                        "AAFC": [1420],
                        "CKEN": [1600],
                        "DKEN": [1600]
                    }
                },
                "M-SHIP": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.97},
                        "TABLET": {"qytlim": 1000, "target": 0.955},
                        "NB": {"qytlim": 1000, "target": 0.96},
                        "TV": {"qytlim": 1000, "target": 0.90},
                        "AA": {"qytlim": 1000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
                        "TV": {"qytlim": 1000, "target": 0.003},
                        "AA": {"qytlim": 1000, "target": 0.003}
                    },
                    "numerator": {},
                    "denominator": {}
                }
            }
        }

    def getData(self):
        try:
            self.writeLog(
                f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Input Json:{self.jsonData}')
            bottomLine = "_"
            redisKey = ""
            tmp = []

            className = f"{self.__class__.__name__}"
            tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
            tmpSITE = self.jsonData["SITE"]
            tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
            tmpAPPLICATION = self.jsonData["APPLICATION"]
            tmpKPITYPE = self.jsonData["KPITYPE"]
            tmpACCT_DATE = self.jsonData["ACCT_DATE"]
            tmpPROD_NBR = self.jsonData["PROD_NBR"]
            tmpOPER = self.jsonData["OPER"]
            # Defect or Reason Code 
            tmpCHECKCODE = self.jsonData["CHECKCODE"] if "CHECKCODE" in self.jsonData else None

            #redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpKPITYPE)
            tmp.append(tmpACCT_DATE)
            tmp.append(tmpPROD_NBR)
            tmp.append(tmpOPER)
            if tmpCHECKCODE != None:
                tmp.append(tmpCHECKCODE)
            redisKey = bottomLine.join(tmp)
            expirTimeKey = tmpFACTORY_ID + '_DEFT'

            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # Check Redis Data

            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

            if tmpKPITYPE == "FPYLV3LINE":
                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                magerData = self._groupINTLV3(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "DFCT_CODE": tmpCHECKCODE,
                    "ERRC_DESCR": self._deftCodeInf(tmpFACTORY_ID, tmpCHECKCODE) if tmpCHECKCODE != None else '',
                    "DATASERIES": magerData
                }

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "EFALV3LINE":

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                magerData = self._groupINTLV3(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "CHECKCODE": tmpCHECKCODE,
                    "DATASERIES": magerData
                }

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "FPYLV2LINE":

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 12)
                n2d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 11)
                n3d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 10)
                n4d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 9)
                n5d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 8)
                n6d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 7)
                n1w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 6)
                n2w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 5)
                n3w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n2w_array"], 4)
                n1m_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 3)
                n2m_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 2)
                n1s_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 1)
                
                magerData = self._grouptFPYLV2LINE(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)

                returnData = self._calFPYLV2LINEOPER(magerData)

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)  

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "EFALV2LINE":

                OPERDATA = [
                    {"PROCESS": "BONDING", "OPER": 1300, "DESC": "PCBI(HMT)"},
                    {"PROCESS": "BONDING", "OPER": 1301,
                        "DESC": "PCBT(串線PCBI)"},
                    {"PROCESS": "LAM", "OPER": 1340, "DESC": "BT"},
                    {"PROCESS": "LAM", "OPER": 1370, "DESC": "PT"},
                    {"PROCESS": "ASSY", "OPER": 1419,
                        "DESC": "OTPA(OTP AAFC)"},
                    {"PROCESS": "ASSY", "OPER": 1420, "DESC": "AAFC(同C-)"},
                    {"PROCESS": "TPI", "OPER": 1510, "DESC": "TPI"},
                    {"PROCESS": "OTPC", "OPER": 1590, "DESC": "Flicker check"},
                    {"PROCESS": "C-KEN", "OPER": 1600,
                        "DESC": "(A+B) Panel C-"}
                ]
                dataRange =  self._dataRange(tmpACCT_DATE)
                
                magerData = []
                for x in OPERDATA:
                    loopOPER = x ["PROCESS"]
                    n1d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1d"], dataRange["n1d_array"], 11)
                    n2d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2d"], dataRange["n2d_array"], 10)
                    n3d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3d"], dataRange["n3d_array"], 9)
                    n4d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n4d"], dataRange["n4d_array"], 8)
                    n5d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n5d"], dataRange["n5d_array"], 7)
                    n6d_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n6d"], dataRange["n6d_array"], 6)
                    n1w_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1w"], dataRange["n1w_array"], 5)
                    n2w_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2w"], dataRange["n2w_array"], 4)
                    n3w_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3w"], dataRange["n2w_array"], 3)
                    n1m_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1m"], dataRange["n1m_array"], 2)
                    n2m_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2m"], dataRange["n2m_array"], 1)
                    n1s_DATA = self._getEFALV3DATA(loopOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1s"], dataRange["n1s_array"], 0)
                    tempData = self._groupINTLV3(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)
                    magerData.append(tempData)

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return magerData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}


            else:
                return {'Result': 'Fail', 'Reason': 'Parametes[KPITYPE] not in Rule'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

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
            return {'Result': 'NG', 'Reason': f'{funcName} erro'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

    def _dataRange(self, ACCT_DATE):
        d = datetime.datetime
        #時間 array 組成
        day_delta = datetime.timedelta(days=1)
        n1d_sd = d.strptime(ACCT_DATE,'%Y%m%d')
        n2d_sd = n1d_sd - 1*day_delta
        n3d_sd = n1d_sd - 2*day_delta
        n4d_sd = n1d_sd - 3*day_delta
        n5d_sd = n1d_sd - 4*day_delta
        n6d_sd = n1d_sd - 5*day_delta
        
        #3周
        weeks_delta = datetime.timedelta(weeks=1)
        #計算是本周哪一天要扣除
        #因pytohn 一周開始是周一 故需要多扣一天
        weekcount = (n1d_sd.weekday() +1) % 7
        n1w_start = n1d_sd - weeks_delta - datetime.timedelta(days=weekcount)
        n1w_end = n1w_start + datetime.timedelta(days=6)
        n2w_start = n1d_sd - 2*weeks_delta - datetime.timedelta(days=weekcount) 
        n2w_end = n2w_start + datetime.timedelta(days=6)
        n3w_start = n1d_sd - 3*weeks_delta - datetime.timedelta(days=weekcount) 
        n3w_end = n3w_start + datetime.timedelta(days=6)

        # 兩月
        n1d_sd_mf = n1d_sd.replace(day=1)
        n1m_end = n1d_sd_mf - day_delta
        n1m_start = n1m_end.replace(day=1)
        n2m_end = n1m_start - day_delta
        n2m_start = n2m_end.replace(day=1)

        #一季
        n1s_end = n2m_start - day_delta
        n1s_start = n1s_end.replace(month= n1s_end.month-2).replace(day=1)
        
        returnData = {
            "n1d": d.strftime(n1d_sd,'%Y%m%d'),
            "n1d_array": self._dataArray(n1d_sd,n1d_sd),
            "n2d": d.strftime(n2d_sd,'%Y%m%d'),
            "n2d_array": self._dataArray(n2d_sd,n2d_sd),
            "n3d": d.strftime(n3d_sd,'%Y%m%d'),
            "n3d_array": self._dataArray(n3d_sd,n3d_sd),
            "n4d": d.strftime(n4d_sd,'%Y%m%d'),
            "n4d_array": self._dataArray(n4d_sd,n4d_sd),
            "n5d": d.strftime(n5d_sd,'%Y%m%d'),
            "n5d_array": self._dataArray(n5d_sd,n5d_sd),
            "n6d": d.strftime(n6d_sd,'%Y%m%d'),
            "n6d_array": self._dataArray(n6d_sd,n6d_sd),
            "n1w": f'W {d.strftime(n1w_start,"%U")}',
            "n1w_start": d.strftime(n1w_start,'%Y%m%d %U %j'),
            "n1w_end": d.strftime(n1w_end,'%Y%m%d %U %j'),
            "n1w_array": self._dataArray(n1w_start,n1w_end),
            "n2w": f'W {d.strftime(n2w_start,"%U")}',
            "n2w_start": d.strftime(n2w_start,'%Y%m%d %U %j'),
            "n2w_end": d.strftime(n2w_end,'%Y%m%d %U %j'),
            "n2w_array": self._dataArray(n2w_start,n2w_end),
            "n3w": f'W {d.strftime(n3w_start,"%U")}',
            "n3w_start": d.strftime(n3w_start,'%Y%m%d %U %j'),
            "n3w_end": d.strftime(n3w_end,'%Y%m%d %U %j'),
            "n3w_array": self._dataArray(n3w_start,n3w_end),
            "n1m": f'{d.strftime(n1m_start,"%Y%m")}',
            "n1m_start": d.strftime(n1m_start,'%Y%m%d %U %j'),
            "n1m_end": d.strftime(n1m_end,'%Y%m%d %U %j'),
            "n1m_array": self._dataArray(n1m_start,n1m_end),
            "n2m": f'{d.strftime(n2m_start,"%Y%m")}',
            "n2m_start": d.strftime(n2m_start,'%Y%m%d %U %j'),
            "n2m_end": d.strftime(n2m_end,'%Y%m%d %U %j'),
            "n2m_array": self._dataArray(n2m_start,n2m_end),
            "n1s": f'{d.strftime(n1s_start,"%Y%m")} - {d.strftime(n1s_end,"%Y%m")}',
            "n1s_start": d.strftime(n1s_start,'%Y%m%d %U %j'),
            "n1s_end": d.strftime(n1s_end,'%Y%m%d %U %j'),
            "n1s_array": self._dataArray(n1s_start,n1s_end),
        }
        return returnData

    def _dataArray(self, sd , ed):
        dataArray = []
        ed = ed + datetime.timedelta(days=1)
        d = datetime.datetime
        for i in range(int((ed - sd).days)):
            x = sd + datetime.timedelta(i)
            dataArray.append( d.strftime(x, '%Y%m%d'))
        return dataArray

    def _getFPYLV3DATA(self, OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        
        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER]
        denominatorValue = getFabData["FPY"]["denominator"][OPER]

        FPYLV3_Aggregate = [
            {
                "$match": {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                    "PROD_NBR": PROD_NBR,
                    "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"}, numeratorData["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"}, numeratorData["tot"]]}
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "APPLICATION" : "$APPLICATION",
                        "PROD_NBR": "$PROD_NBR"
                    },
                    "deftQty": {
                        "$sum": {"$toInt": "$QTY"}
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "deftQty": "$deftQty",
                    "passQty": 0
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            },
            {
                "$unionWith": {
                    "coll": "passHisAndCurrent",
                            "pipeline": [
                                {
                                    "$match": {
                                        "COMPANY_CODE": tmpCOMPANY_CODE,
                                        "SITE": tmpSITE,
                                        "FACTORY_ID": tmpFACTORY_ID,
                                        "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},                                       
                                        "PROD_NBR": PROD_NBR,
                                        "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue]},
                                        "TRANS_TYPE": { "$nin": [ "QRWK" ]}                
                                    }
                                },
                                {
                                    "$group": {
                                        "_id": {
                                            "APPLICATION" : "$APPLICATION",
                                            "PROD_NBR": "$PROD_NBR"
                                        },
                                        "passQty": {
                                            "$sum": {
                                                "$toInt": "$QTY"
                                            }
                                        }
                                    }
                                },
                                {
                                    "$addFields": {
                                        "APPLICATION" : "$_id.APPLICATION",
                                        "PROD_NBR": "$_id.PROD_NBR",
                                        "passQty": "$passQty",
                                        "deftQty": 0
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0
                                    }
                                }
                            ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "APPLICATION" : "$APPLICATION",
                        "PROD_NBR": "$PROD_NBR"
                    },
                    "deftQty": {
                        "$sum": "$deftQty"
                    },
                    "passQty": {
                        "$sum": "$passQty"
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "OPER" : OPER,
                    "DATARANGE": DATARANGENAME,
                    "XVALUE": TYPE,
                    "DEFECT_YIELD": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$passQty",
                                    0
                                ]
                            },
                            0,
                            {
                                "$divide": [
                                    "$deftQty",
                                    "$passQty"
                                ]
                            }
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            },
            {
                "$sort": {
                    "APPLICATION" : 1,
                    "PROD_NBR": 1
                }
            }
        ]

        if DEFECTCODE != None:
            FPYLV3_Aggregate[0]["$match"]["DFCT_CODE"] = DEFECTCODE
        self.writeLog(FPYLV3_Aggregate)
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            returnData = self.aggregate(FPYLV3_Aggregate)
            self.closeMongoConncetion()
            
            return returnData

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
            return "error"

    def _groupINTLV3(self, n1d,n2d,n3d,n4d,n5d,n6d,n1w,n2w,n3w,n1m,n2m,n1s): 
            magerData = [] 
            for d in n1s:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)
            for d in n2m:
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0       
                magerData.append(d)
            for d in n1m:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)              
            for d in n3w:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)             
            for d in n2w:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)             
            for d in n1w: 
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0      
                magerData.append(d)             
            for d in n6d:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)            
            for d in n5d:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)             
            for d in n4d:  
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0     
                magerData.append(d)             
            for d in n3d:       
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0
                magerData.append(d)             
            for d in n2d:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)                       
            for d in n1d:       
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0
                magerData.append(d) 
         

            return magerData

    def _deftCodeInf(self, fabId, code):
        config = configparser.ConfigParser()
        config.read('setting.ini')
        #------DB2----------
        database = config.get("DB",fabId)
        ip = config.get("IP",fabId)
        port = config.get("PORT",fabId)
        account = config.get("ACCOUNT",fabId)
        password = config.get("PASSWORD",fabId)
        schema = config.get("SCHEMA",fabId)
        self.writeLog(f"FACTORY_ID:{fabId},Db2:{database},IP:{ip}:{port},ACCOUNT:{account};PASSWORD:{password};SCHEMA:{schema}")
        db = self.getDb2Connection(database ,ip, port, account, password)
        sql = f"select * from {schema}.V_BSDEFCODE where ERRC_NBR = '{code}'  with ur"
        BSDEFCODE = self.db2Select(sql)                   
        self.db2CloseConnection()
        return BSDEFCODE[0]["ERRC_DESCR"]

    def _getEFALV3DATA(self, OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        
        OPERDATA = [
            {"PROCESS": "BONDING", "OPER": 1300, "DESC": "PCBI(HMT)"},
            {"PROCESS": "BONDING", "OPER": 1301,
                "DESC": "PCBT(串線PCBI)"},
            {"PROCESS": "LAM", "OPER": 1340, "DESC": "BT"},
            {"PROCESS": "LAM", "OPER": 1370, "DESC": "PT"},
            {"PROCESS": "ASSY", "OPER": 1419,
                "DESC": "OTPA(OTP AAFC)"},
            {"PROCESS": "ASSY", "OPER": 1420, "DESC": "AAFC(同C-)"},
            {"PROCESS": "TPI", "OPER": 1510, "DESC": "TPI"},
            {"PROCESS": "OTPC", "OPER": 1590, "DESC": "Flicker check"},
            {"PROCESS": "C-KEN", "OPER": 1600,
                "DESC": "(A+B) Panel C-"}
        ]

        OPERList = []
        for x in OPERDATA:
            OPERList.append(f'{x.get("OPER")}')

        FPYLV3_Aggregate = [
            {
                "$match": {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                    "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                    "MAIN_WC": {"$in": OPERList},
                    "PROD_NBR": PROD_NBR
                }
            },
            {
                "$group": {
                    "_id": {
                        "APPLICATION" : "$APPLICATION",
                        "PROD_NBR": "$PROD_NBR",
                        "DFCT_CODE": "$DFCT_CODE"
                    },
                    "deftQty": {
                        "$sum": {"$toInt": "$QTY"}
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "deftQty": "$deftQty",
                    "passQty": 0
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            },
            {
                "$unionWith": {
                    "coll": "passHisAndCurrent",
                            "pipeline": [
                                {
                                    "$match": {
                                        "COMPANY_CODE": tmpCOMPANY_CODE,
                                        "SITE": tmpSITE,
                                        "FACTORY_ID": tmpFACTORY_ID,
                                        "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                                        "MAIN_WC": {"$in": OPERList},
                                        "PROD_NBR": PROD_NBR
                                    }
                                },
                                {
                                    "$group": {
                                        "_id": {
                                            "APPLICATION" : "$APPLICATION",
                                            "PROD_NBR": "$PROD_NBR"
                                        },
                                        "passQty": {
                                            "$sum": {
                                                "$toInt": "$QTY"
                                            }
                                        }
                                    }
                                },
                                {
                                    "$addFields": {
                                        "APPLICATION" : "$_id.APPLICATION",
                                        "PROD_NBR": "$_id.PROD_NBR",
                                        "passQty": "$passQty",
                                        "deftQty": 0
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0
                                    }
                                }
                            ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "APPLICATION" : "$APPLICATION",
                        "PROD_NBR": "$PROD_NBR"
                    },
                    "deftQty": {
                        "$sum": "$deftQty"
                    },
                    "passQty": {
                        "$sum": "$passQty"
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "OPER" : OPER,
                    "DATARANGE": DATARANGENAME,
                    "XVALUE": TYPE,
                    "DEFECT_YIELD": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$passQty",
                                    0
                                ]
                            },
                            0,
                            {
                                "$divide": [
                                    "$deftQty",
                                    "$passQty"
                                ]
                            }
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            },
            {
                "$sort": {
                    "APPLICATION" : 1,
                    "PROD_NBR": 1
                }
            }
        ]

        if DEFECTCODE != None:
            FPYLV3_Aggregate[0]["$match"]["DFCT_CODE"] = DEFECTCODE
       
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            returnData = self.aggregate(FPYLV3_Aggregate)
            self.closeMongoConncetion()

            return returnData

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
            return "error"

    def _getFPYLV2LINEData(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]

        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER]

        deftAggregate = []

        #deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "PROD_NBR": PROD_NBR,
                "$expr": {
                    "$and": [
                        {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["fromt"]]},
                        {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["tot"]]}
                    ]
                }
            }
        }
        deftGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",                    
                    "APPLICATION": "$APPLICATION",              
                    "DFCT_CODE" : "$DFCT_CODE",
                    "ERRC_DESCR" : "$ERRC_DESCR",
                    
                },
                "DEFT_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        deftProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "DFCT_CODE" : "$_id.DFCT_CODE",                
                "ERRC_DESCR" : "$_id.ERRC_DESCR",
                "DEFT_QTY": "$DEFT_QTY"
            }
        }
        deftAdd = {
                "$addFields": {
                    "OPER": OPER,     
                    "DATARANGE": DATARANGENAME,
                    "XVALUE": TYPE
                }
            }
        deftSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "APPLICATION": 1,
                "DFCT_CODE" : 1,
                "ERRC_DESCR" : 1
            }
        }
        deftAggregate.extend([deftMatch1, deftGroup1, deftProject1, deftAdd, deftSort])
        
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftAggregate)
            self.closeMongoConncetion()

            returnData = dData

            return returnData

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
            return "error"

    def _grouptFPYLV2LINE(self, n1d,n2d,n3d,n4d,n5d,n6d,n1w,n2w,n3w,n1m,n2m,n1s): 
            magerData = [] 
            for d in n1s:     
                magerData.append(d)
            for d in n2m:     
                magerData.append(d)
            for d in n1m:     
                magerData.append(d)              
            for d in n3w:       
                magerData.append(d)             
            for d in n2w:      
                magerData.append(d)             
            for d in n1w:      
                magerData.append(d)             
            for d in n6d:      
                magerData.append(d)            
            for d in n5d:      
                magerData.append(d)             
            for d in n4d:      
                magerData.append(d)             
            for d in n3d:       
                magerData.append(d)             
            for d in n2d:     
                magerData.append(d)                       
            for d in n1d:       
                magerData.append(d)
            return magerData

    def _calFPYLV2LINEOPER(self, tempData):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]        
        tmpAPPLICATION =self.jsonData["APPLICATION"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        tmpOPER = self.jsonData["OPER"]

        allDFCTCount = {}
        for x in tempData:    
            if x["DFCT_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["DFCT_CODE"]] += x["DEFT_QTY"]
            else:
                allDFCTCount[x["DFCT_CODE"]] = x["DEFT_QTY"]
        top10 = dict(sorted(allDFCTCount.items(),key=lambda item:item[1],reverse=True) [:10])
               
        DATASERIES = []
        for x in tempData:  
            cDFct = x["DFCT_CODE"]  if x["DFCT_CODE"] in top10.keys() else "OTHER"
            cERRC = x["ERRC_DESCR"] if x["DFCT_CODE"] in top10.keys() else "OTHER" 

            rank = 999
            if cDFct in top10.keys():
                rank = 1
                for i in top10:
                    if i != x["DFCT_CODE"]:
                        rank +=1 
                    else:
                        break
            

            d = list(filter(lambda d: d["DFCT_CODE"] == cDFct and d["XVALUE"] == x["XVALUE"] , DATASERIES))
            if d == []:
                test = {
                        "OPER": x["OPER"],
                        "XVALUE": x["XVALUE"],
                        "YVALUE": x["DEFT_QTY"],
                        "RANK": rank,
                        "DFCT_CODE" : cDFct,
                        "ERRC_DESCR" : cERRC,
                        "DATARANGE": x["DATARANGE"]
                    }
                DATASERIES.append(test)
            
            else:
                for cx in DATASERIES:
                    if cx["OPER"] == x["OPER"] and cx["DFCT_CODE"] == cDFct :
                       cx["YVALUE"] += x["DEFT_QTY"]

        DATASERIES.sort(key = operator.itemgetter("XVALUE", "RANK"), reverse = False)

        returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "OPER": tmpOPER,
                    "DATASERIES": DATASERIES
                }

        return returnData
