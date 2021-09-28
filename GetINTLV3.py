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

from decimal import Decimal, ROUND_HALF_UP
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
                        "DKEN": [1700]
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
                        "CE": {"qytlim": 3000, "target": 0.003},
                        "TABLET": {"qytlim": 3000, "target": 0.003},
                        "NB": {"qytlim": 3000, "target": 0.003},
                        "TV": {"qytlim": 3000, "target": 0.003},
                        "AA": {"qytlim": 3000, "target": 0.003}
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
                        "DKEN": [1700]
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
                        "CE": {"qytlim": 3000, "target": 0.003},
                        "TABLET": {"qytlim": 3000, "target": 0.003},
                        "NB": {"qytlim": 3000, "target": 0.003},
                        "TV": {"qytlim": 3000, "target": 0.003},
                        "AA": {"qytlim": 3000, "target": 0.003}
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
                        "DKEN": [1700]
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
                        "CE": {"qytlim": 3000, "target": 0.003},
                        "TABLET": {"qytlim": 3000, "target": 0.003},
                        "NB": {"qytlim": 3000, "target": 0.003},
                        "TV": {"qytlim": 3000, "target": 0.003},
                        "AA": {"qytlim": 3000, "target": 0.003}
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
            tmpOPER = self.jsonData["OPER"] # or RESPTYPE
            # Defect or Reason Code 
            tmpCHECKCODE = self.jsonData["CHECKCODE"] if "CHECKCODE" in self.jsonData else None
            expirTimeKey = tmpFACTORY_ID + '_PASS'

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

            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # Check Redis Data

            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

            if tmpKPITYPE == "FPYLV3LINE":                
                expirTimeKey = tmpFACTORY_ID + '_PASS'
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
                expirTimeKey = tmpFACTORY_ID + '_REASON'

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
                expirTimeKey = tmpFACTORY_ID + '_PASS'

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                DATASERIES = self._grouptFPYLV2LINE(
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1d_DATA["dData"], n1d_DATA["pData"]), tmpOPER, dataRange["n1d"], 11),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2d_DATA["dData"], n2d_DATA["pData"]), tmpOPER, dataRange["n2d"], 10),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n3d_DATA["dData"], n3d_DATA["pData"]), tmpOPER, dataRange["n3d"], 9),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n4d_DATA["dData"], n4d_DATA["pData"]), tmpOPER, dataRange["n4d"], 8),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n5d_DATA["dData"], n5d_DATA["pData"]), tmpOPER, dataRange["n5d"], 7),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n6d_DATA["dData"], n6d_DATA["pData"]), tmpOPER, dataRange["n6d"], 6),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1w_DATA["dData"], n1w_DATA["pData"]), tmpOPER, dataRange["n1w"], 5),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2w_DATA["dData"], n2w_DATA["pData"]), tmpOPER, dataRange["n2w"], 4),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n3w_DATA["dData"], n3w_DATA["pData"]), tmpOPER, dataRange["n3w"], 3),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1m_DATA["dData"], n1m_DATA["pData"]), tmpOPER, dataRange["n1m"], 2),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2m_DATA["dData"], n2m_DATA["pData"]), tmpOPER, dataRange["n2m"], 1),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1s_DATA["dData"], n1s_DATA["pData"]), tmpOPER, dataRange["n1s"], 0))

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

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)  

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "FPYLV2LINEALL":
                expirTimeKey = tmpFACTORY_ID + '_PASS'

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                DATASERIES = self._grouptFPYLV2LINE(
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1d_DATA["dData"], n1d_DATA["pData"]), tmpOPER, dataRange["n1d"], 11),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2d_DATA["dData"], n2d_DATA["pData"]), tmpOPER, dataRange["n2d"], 10),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n3d_DATA["dData"], n3d_DATA["pData"]), tmpOPER, dataRange["n3d"], 9),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n4d_DATA["dData"], n4d_DATA["pData"]), tmpOPER, dataRange["n4d"], 8),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n5d_DATA["dData"], n5d_DATA["pData"]), tmpOPER, dataRange["n5d"], 7),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n6d_DATA["dData"], n6d_DATA["pData"]), tmpOPER, dataRange["n6d"], 6),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1w_DATA["dData"], n1w_DATA["pData"]), tmpOPER, dataRange["n1w"], 5),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2w_DATA["dData"], n2w_DATA["pData"]), tmpOPER, dataRange["n2w"], 4),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n3w_DATA["dData"], n3w_DATA["pData"]), tmpOPER, dataRange["n3w"], 3),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1m_DATA["dData"], n1m_DATA["pData"]), tmpOPER, dataRange["n1m"], 2),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n2m_DATA["dData"], n2m_DATA["pData"]), tmpOPER, dataRange["n2m"], 1),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPER(n1s_DATA["dData"], n1s_DATA["pData"]), tmpOPER, dataRange["n1s"], 0))

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "OPER": "ALL",
                    "DATASERIES": DATASERIES
                }

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)  

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "MSHIPLV2LINE":                
                expirTimeKey = tmpFACTORY_ID + '_SCRP'

                """
                (1)  前廠責：USL、TX LCD、FABX
                (2)  廠責：MFG、INT、EQP、ER 
                (3)  來料責：SQE
                """
                mshipDATA = {
                    "formerfab":{"in":"usl|lcd|fab","name": "前廠責", "id":"formerfab"},
                    "fab":{"in":"mfg|int|eqp|er","name": "廠責", "id":"fab"},
                    "incoming":{"in":"sqe","name": "SQE來料責", "id":"incoming"},
                }
                getFabData = mshipDATA[tmpOPER]   

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                DATASERIES = self._grouptMSHIPLV2LINE(
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n1d_DATA["scData"], n1d_DATA["shData"]), dataRange["n1d"], 11),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n2d_DATA["scData"], n2d_DATA["shData"]), dataRange["n2d"], 10),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n3d_DATA["scData"], n3d_DATA["shData"]), dataRange["n3d"], 9),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n4d_DATA["scData"], n4d_DATA["shData"]), dataRange["n4d"], 8),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n5d_DATA["scData"], n5d_DATA["shData"]), dataRange["n5d"], 7),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n6d_DATA["scData"], n6d_DATA["shData"]), dataRange["n6d"], 6),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n1w_DATA["scData"], n1w_DATA["shData"]), dataRange["n1w"], 5),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n2w_DATA["scData"], n2w_DATA["shData"]), dataRange["n2w"], 4),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n3w_DATA["scData"], n3w_DATA["shData"]), dataRange["n3w"], 3),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n1m_DATA["scData"], n1m_DATA["shData"]), dataRange["n1m"], 2),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n2m_DATA["scData"], n2m_DATA["shData"]), dataRange["n2m"], 1),
                    self._calMSHIPLV2(self._groupMSHIPLV2LINE(n1s_DATA["scData"], n1s_DATA["shData"]), dataRange["n1s"], 0))

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "RESP_OWNER": getFabData["name"],
                    "RESP_OWNER_E":  getFabData["id"],
                    "DATASERIES": DATASERIES
                }

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)  

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "MSHIPLV2LINEDG":                
                expirTimeKey = tmpFACTORY_ID + '_SCRP'

                dataRange =  self._dataRange(tmpACCT_DATE)

                n1d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n3w"], dataRange["n2w_array"], 3)
                n1m_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                DATASERIES = self._grouptMSHIPLV2LINE(
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n1d_DATA), dataRange["n1d"], 11),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n2d_DATA), dataRange["n2d"], 10),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n3d_DATA), dataRange["n3d"], 9),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n4d_DATA), dataRange["n4d"], 8),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n5d_DATA), dataRange["n5d"], 7),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n6d_DATA), dataRange["n6d"], 6),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n1w_DATA), dataRange["n1w"], 5),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n2w_DATA), dataRange["n2w"], 4),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n3w_DATA), dataRange["n3w"], 3),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n1m_DATA), dataRange["n1m"], 2),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n2m_DATA), dataRange["n2m"], 1),
                    self._calMSHIPLV2DG(self._groupMSHIPLV2LINEDG(n1s_DATA), dataRange["n1s"], 0))

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,         
                    "DATASERIES": DATASERIES
                }

                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60) 

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}


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
                    },
                    "RW_COUNT": {"$lte": "1"}
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
                                        "RW_COUNT": {"$lte": "1"}
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
        db = self.getDb2Connection(database ,ip, port, account, password)
        sql = f"select * from {schema}.V_BSDEFCODE where ERRC_NBR = '{code}'  with ur"
        BSDEFCODE = self.db2Select(sql)                   
        self.db2CloseConnection()
        return BSDEFCODE[0]["ERRC_DESCR"]

    def _getEFALV3DATA(self, OPER, PROD_NBR, REASONCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
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
        OPERList.append(OPER)
        ###
        #for x in OPERDATA:
        #    OPERList.append(f'{x.get("OPER")}')
        ###

        EFALV3_Aggregate = [
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
                        "DFCT_REASON": "$DFCT_REASON"
                    },
                    "reasonQty": {
                        "$sum": {"$toInt": "$QTY"}
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "reasonQty": "$reasonQty",
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
                                        "reasonQty": 0
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
                    "reasonQty": {
                        "$sum": "$reasonQty"
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
                                    "$reasonQty",
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

        if REASONCODE != None:
            EFALV3_Aggregate[0]["$match"]["DFCT_REASON"] = REASONCODE
       
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("reasonHisAndCurrent")
            returnData = self.aggregate(EFALV3_Aggregate)
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
        denominatorValue = getFabData["FPY"]["denominator"][OPER]

        passAggregate = []
        deftAggregate = []

        #pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "PROCESS": "$PROCESS",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": {"$toInt": "$MAIN_WC"}
                },
                "PASS_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        passProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "PROCESS": "$_id.PROCESS",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "PASS_QTY": "$PASS_QTY"
            }
        }
        passGroup2 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "APPLICATION": "$APPLICATION"
                },
                "PassSUMQty": {
                    "$sum": {"$toInt": "$PASS_QTY"}
                }
            }
        }
        passProject2 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "PassSUMQty": "$PassSUMQty"
            }
        }
        passAdd = {
                "$addFields": {
                    "OPER": OPER
                }
            }
        passSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

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
                },
                "RW_COUNT": {"$lte": "1"}
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
        passAggregate.extend([passMatch1, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])        

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("passHisAndCurrent")
            pData = self.aggregate(passAggregate)
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftAggregate)
            self.closeMongoConncetion()
            returnData = {
                "pData": pData,
                "dData": dData
            }
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

    def _groupPassDeftByPRODandOPER(self, dData, pData):
        deftData = []
        for d in dData:
            deftData.append(d)
        passData = []
        for p in pData:
            passData.append(p)
        data = []
        oData = {}
        if deftData != [] and passData != []:
            for d in deftData:
                p = list(filter(lambda d: d["PROD_NBR"]
                        == p["PROD_NBR"], passData))[0]
                oData["XVALUE"] = copy.deepcopy(d["XVALUE"])                
                oData["DATARANGE"] = copy.deepcopy(d["DATARANGE"])
                oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
                oData["SITE"] = copy.deepcopy(p["SITE"])
                oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
                oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                if "APPLICATION" in p.keys():
                    oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
                else:
                    oData["APPLICATION"] = None
                oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                oData["OPER"] = copy.deepcopy(p["OPER"])
                oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                oData["PassSUMQty"] = copy.deepcopy(p["PassSUMQty"])
                if d == []:
                    oData["DeftSUMQty"] = 0
                else:
                    oData["DeftSUMQty"] = copy.deepcopy(d["DEFT_QTY"])
                if oData["DeftSUMQty"] < oData["PassSUMQty"]:
                    data.append(copy.deepcopy(oData))
                oData = {}
        return data

    def _calFPYLV2LINEOPER(self, tempData, OPER, DATARANGE, DATARANGEID):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        allDFCTCount = {}
        for x in tempData:    
            if x["DFCT_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["DFCT_CODE"]] += x["DeftSUMQty"]
            else:
                allDFCTCount[x["DFCT_CODE"]] = x["DeftSUMQty"]
        top10 = dict(sorted(allDFCTCount.items(),key=lambda item:item[1],reverse=True) [:10])
               
        DATASERIES = []
        if tempData == []:
            test = {
                    "OPER": OPER,
                    "XVALUE": DATARANGEID,
                    "YVALUE": 0,
                    "RANK": 0,
                    "DATARANGE": DATARANGE,                    
                    "DFCT_CODE" : "OTHER",
                    "ERRC_DESCR" : "OTHER",                  
                    "PROD_NBR": tmpPROD_NBR,
                    "DeftSUM": 0,
                    "PassSUM": 0,
                    "DEFECT_RATE": 0
                }
            DATASERIES.append(test)
        else:
            for x in tempData:  
                cDFct = x["DFCT_CODE"]  if x["DFCT_CODE"] in top10.keys() else "OTHER"
                cERRC = x["ERRC_DESCR"] if x["DFCT_CODE"] in top10.keys() else "OTHER" 

                rank = 11
                if cDFct in top10.keys():
                    rank = 1
                    for i in top10:
                        if i != x["DFCT_CODE"]:
                            rank +=1 
                        else:
                            break
                

                d = list(filter(lambda d: d["DFCT_CODE"] == cDFct and d["XVALUE"] == x["XVALUE"] , DATASERIES))
                if d == []:
                    ds = Decimal(x["DeftSUMQty"])
                    ps = Decimal(x["PassSUMQty"])
                    dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    test = {
                            "OPER": x["OPER"],
                            "XVALUE": x["XVALUE"],
                            "YVALUE": dr*100,
                            "RANK": rank,
                            "DATARANGE": x["DATARANGE"],
                            "DFCT_CODE" : cDFct,
                            "ERRC_DESCR" : cERRC,                        
                            "PROD_NBR": tmpPROD_NBR,
                            "DeftSUM": x["DeftSUMQty"],
                            "PassSUM": x["PassSUMQty"],
                            "DEFECT_RATE": dr*100
                        }
                    DATASERIES.append(test)
                
                else:
                    for cx in DATASERIES:
                        if cx["OPER"] == x["OPER"] and cx["DFCT_CODE"] == cDFct :                        
                            cx["DeftSUM"] += x["DeftSUMQty"]
                            ds = Decimal(cx["DeftSUM"])
                            ps = Decimal(cx["PassSUM"])
                            dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                            cx["DEFECT_RATE"] = dr*100
                            cx["YVALUE"] =  dr*100

            DATASERIES.sort(key = operator.itemgetter("RANK", "RANK"), reverse = True)

        returnData = DATASERIES

        return returnData
  
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

            magerData.sort(key = operator.itemgetter("XVALUE", "XVALUE"), reverse = True)
            magerData.sort(key = operator.itemgetter("RANK", "RANK"), reverse = True)
            
            return magerData

    def _getFPYLV2LINEDataALL(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]

        getFabData = self.operSetData[tmpFACTORY_ID]          
        denominatorValue = getFabData["FPY"]["denominator"]

        passAggregate = []
        deftAggregate = []
        
        AlldenominatorValue = []
        for x in denominatorValue["PCBI"]:
            AlldenominatorValue.append(x)
        for x in denominatorValue["LAM"]:
            AlldenominatorValue.append(x)
        for x in denominatorValue["AAFC"]:
            AlldenominatorValue.append(x)
        for x in denominatorValue["CKEN"]:
            AlldenominatorValue.append(x)
        for x in denominatorValue["DKEN"]:
            AlldenominatorValue.append(x)

        #pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, AlldenominatorValue]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "PROCESS": "$PROCESS",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": {"$toInt": "$MAIN_WC"}
                },
                "PASS_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        passProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "PROCESS": "$_id.PROCESS",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "PASS_QTY": "$PASS_QTY"
            }
        }
        passGroup2 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "APPLICATION": "$APPLICATION"
                },
                "PassSUMQty": {
                    "$sum": {"$toInt": "$PASS_QTY"}
                }
            }
        }
        passProject2 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "PassSUMQty": "$PassSUMQty"
            }
        }
        passAdd = {
                "$addFields": {
                    "OPER": OPER
                }
            }
        passSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }       
           
        numeratorData = getFabData["FPY"]["numerator"]  
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
                    "$or":[
                        {"$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["PCBI"]["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["PCBI"]["tot"]]}
                        ]},
                        {"$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["LAM"]["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["LAM"]["tot"]]}
                        ]},
                        {"$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["AAFC"]["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["AAFC"]["tot"]]}
                        ]},
                        {"$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["CKEN"]["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["CKEN"]["tot"]]}
                        ]},
                        {"$and": [
                            {"$gte": [{"$toInt": "$MAIN_WC"},numeratorData["PCBI"]["fromt"]]},
                            {"$lte": [{"$toInt": "$MAIN_WC"},numeratorData["DKEN"]["tot"]]}
                        ]}
                    ]
                },
                "RW_COUNT": {"$lte": "1"}
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
        passAggregate.extend([passMatch1, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])        

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("passHisAndCurrent")
            pData = self.aggregate(passAggregate)
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftAggregate)
            self.closeMongoConncetion()
            returnData = {
                "pData": pData,
                "dData": dData
            }
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

    def _getMSHIPLV2LINE(self,getFabData, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]


        scrapAggregate = []

        # scrap
        scrapMatch = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                'RESP_OWNER': {'$regex': getFabData["in"], '$options': 'i' },
                "PROD_NBR": PROD_NBR
            }
        }        
        scrapGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "APPLICATION": "$APPLICATION",
                    "PROD_NBR": "$PROD_NBR",
                    "SCRAP_DESCR": "$SCRAP_DESCR",
                    "SCRAP_CODE" : "$SCRAP_CODE"
                },
                "TOBESCRAP_QTY": {
                    "$sum": {"$toInt": "$TOBESCRAP_QTY"}
                }
            }
        }
        scrapProject = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "APPLICATION": "$_id.APPLICATION",
                "PROD_NBR": "$_id.PROD_NBR",
                "SCRAP_DESCR": "$_id.SCRAP_DESCR",
                "SCRAP_CODE" : "$_id.SCRAP_CODE",
                "TOBESCRAP_SUMQTY": "$TOBESCRAP_QTY"
            }
        }
        scrapAdd = {
            "$addFields": {
                "RESP_OWNER": getFabData["name"],
                "RESP_OWNER_E":  getFabData["id"],
                "DATARANGE": DATARANGENAME,
                "XVALUE": TYPE
            }
        }
        scrapSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "APPLICATION": 1,
                "PROD_NBR": 1
            }
        }

        shipAggregate = []
        # ship
        shipAggregate = []
        shipMatch = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "TRANS_TYPE": "SHIP",
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "PROD_NBR": PROD_NBR
            }
        }
        shipGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "APPLICATION": "$APPLICATION",
                    "PROD_NBR": "$PROD_NBR"
                },
                "SHIPSUM": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        shipProject = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "APPLICATION": "$_id.APPLICATION",
                "PROD_NBR": "$_id.PROD_NBR",
                "SHIP_SUMQTY": "$SHIPSUM"
            }
        }
        shipAdd = {
            "$addFields": {
                "DATARANGE": DATARANGENAME,
                "XVALUE": TYPE
            }
        }
        shipSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "APPLICATION": 1,
                "PROD_NBR": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            scrapMatch["$match"]["APPLICATION"] = tmpAPPLICATION
            shipMatch["$match"]["APPLICATION"] = tmpAPPLICATION

        scrapAggregate.extend(
            [scrapMatch, scrapGroup, scrapProject, scrapAdd, scrapSort])
        shipAggregate.extend([shipMatch, shipGroup, shipProject, shipAdd, shipSort])

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("scrapHisAndCurrent")
            scrapData = self.aggregate(scrapAggregate)
            self.setMongoCollection("passHisAndCurrent")
            shipData = self.aggregate(shipAggregate)
            self.closeMongoConncetion()

            returnData = {
                "scData": scrapData,
                "shData": shipData
            }

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

    def _groupMSHIPLV2LINE(self, scData, shData):
        scapData = []
        for sc in scData:
            scapData.append(sc)
        shipData = []
        for sh in shData:
            shipData.append(sh)
        data = []
        oData = {}
        if scapData != [] and shipData != []:
            for d in scapData:   
                _shipData = list(
                    filter(lambda d: d["PROD_NBR"] == d["PROD_NBR"], shipData))   
                oData["RESP_OWNER"] = copy.deepcopy(d["XVALUE"])  
                oData["RESP_OWNER_E"] = copy.deepcopy(d["XVALUE"])  
                oData["SCRAP_DESCR"] = copy.deepcopy(d["SCRAP_DESCR"])  
                oData["SCRAP_CODE"] = copy.deepcopy(d["SCRAP_CODE"])  
                oData["XVALUE"] = copy.deepcopy(d["XVALUE"])                
                oData["DATARANGE"] = copy.deepcopy(d["DATARANGE"])
                oData["COMPANY_CODE"] = copy.deepcopy(
                    _shipData[0]["COMPANY_CODE"])
                oData["SITE"] = copy.deepcopy(_shipData[0]["SITE"])
                oData["FACTORY_ID"] = copy.deepcopy(_shipData[0]["FACTORY_ID"])
                if "APPLICATION" in _shipData[0].keys():
                    oData["APPLICATION"] = copy.deepcopy(
                        _shipData[0]["APPLICATION"])
                else:
                    oData["APPLICATION"] = None
                oData["PROD_NBR"] = copy.deepcopy(_shipData[0]["PROD_NBR"])
                oData["TOBESCRAP_SUMQTY"] = copy.deepcopy(d["TOBESCRAP_SUMQTY"])                    
                oData["SHIP_SUMQTY"] = copy.deepcopy(_shipData[0]["SHIP_SUMQTY"])
                if oData["TOBESCRAP_SUMQTY"] == 0:
                    oData["SCRAP_YIELD"] = 1
                else:
                    ds = Decimal(oData["TOBESCRAP_SUMQTY"])
                    ps = Decimal(oData["SHIP_SUMQTY"])
                    dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    oData["SCRAP_YIELD"] = dr
                if oData["SHIP_SUMQTY"] > oData["TOBESCRAP_SUMQTY"] > 0:
                    data.append(copy.deepcopy(oData))
                oData = {}
        return data

    def _calMSHIPLV2(self, tempData, DATARANGE, DATARANGEID):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        allDFCTCount = {}
        for x in tempData:    
            if x["SCRAP_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["SCRAP_CODE"]] += x["TOBESCRAP_SUMQTY"]
            else:
                allDFCTCount[x["SCRAP_CODE"]] = x["TOBESCRAP_SUMQTY"]
        top10 = dict(sorted(allDFCTCount.items(),key=lambda item:item[1],reverse=True) [:10])
               
        DATASERIES = []
        if tempData == []:
            test = {
                    "XVALUE": DATARANGEID,
                    "YVALUE": 0,
                    "RANK": 0,
                    "DATARANGE": DATARANGE,                    
                    "SCRAP_CODE" : "OTHER",
                    "SCRAP_DESCR" : "OTHER",                  
                    "PROD_NBR": tmpPROD_NBR,
                    "TOBESCRAP_SUMQTY": 0,
                    "SHIP_SUMQTY": 0,
                    "SCRAP_YIELD": 0
                }
            DATASERIES.append(test)
        else:
            for x in tempData:  
                cDFct = x["SCRAP_CODE"] if x["SCRAP_CODE"] in top10.keys() else "OTHER"
                cERRC = x["SCRAP_DESCR"] if x["SCRAP_CODE"] in top10.keys() else "OTHER" 

                rank = 11
                if cDFct in top10.keys():
                    rank = 1
                    for i in top10:
                        if i != x["SCRAP_CODE"]:
                            rank +=1 
                        else:
                            break
                

                d = list(filter(lambda d: d["SCRAP_CODE"] == cDFct and d["XVALUE"] == x["XVALUE"] , DATASERIES))
                if d == []:
                    ds = Decimal(x["TOBESCRAP_SUMQTY"])
                    ps = Decimal(x["SHIP_SUMQTY"])
                    dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    test = {
                            "XVALUE": x["XVALUE"],
                            "YVALUE": dr*100,
                            "RANK": rank,
                            "DATARANGE": x["DATARANGE"],
                            "SCRAP_CODE" : cDFct,
                            "SCRAP_DESCR" : cERRC,                        
                            "PROD_NBR": tmpPROD_NBR,
                            "SCRAP_SUMQTY": x["TOBESCRAP_SUMQTY"],
                            "SHIP_SUMQTY": x["SHIP_SUMQTY"],
                            "SCRAP_YIELD": dr*100
                        }
                    DATASERIES.append(test)
                
                else:
                    for cx in DATASERIES:
                        if  cx["SCRAP_CODE"] == cDFct :                        
                            cx["SCRAP_SUMQTY"] += x["TOBESCRAP_SUMQTY"]
                            ds = Decimal(cx["SCRAP_SUMQTY"])
                            ps = Decimal(cx["SHIP_SUMQTY"])
                            dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                            cx["SCRAP_YIELD"] = dr*100
                            cx["YVALUE"] =  dr*100

            DATASERIES.sort(key = operator.itemgetter("RANK", "RANK"), reverse = True)

        returnData = DATASERIES

        return returnData

    def _grouptMSHIPLV2LINE(self, n1d,n2d,n3d,n4d,n5d,n6d,n1w,n2w,n3w,n1m,n2m,n1s): 
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

    def _getMSHIPLV2LINEDG(self, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]

        passAggregate = []
        #pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "MAIN_WC": "1600"
            }
        }
        passGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "PROCESS": "$PROCESS",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": {"$toInt": "$MAIN_WC"}
                },
                "PASS_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        passProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "PROCESS": "$_id.PROCESS",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "PASS_QTY": "$PASS_QTY"
            }
        }
        passGroup2 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "APPLICATION": "$APPLICATION"
                },
                "PASS_QTY": {
                    "$sum": {"$toInt": "$PASS_QTY"}
                }
            }
        }
        passProject2 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "PASS_QTY": "$PASS_QTY"
            }
        }
        passSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }
        
        deftAggregate = []
        #deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "PROD_NBR": PROD_NBR,
                "IS_DOWNGRADE" : "Y"
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

        fixAggregate = []
        #deft
        fixMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "PROD_NBR": PROD_NBR,
                "MAIN_WC": "1600"
            }
        }
        fixGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",                    
                    "APPLICATION": "$APPLICATION",                    
                },
                "FIX_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        fixProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "FIX_QTY": "$FIX_QTY"
            }
        }
        fixAdd = {
                "$addFields": {   
                    "DATARANGE": DATARANGENAME,
                    "XVALUE": TYPE
                }
            }
        fixSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "APPLICATION": 1
            }
        }

        fixAggregate.extend([fixMatch1, fixGroup1, fixProject1, fixAdd, fixSort])
        deftAggregate.extend([deftMatch1, deftGroup1, deftProject1, deftAdd, deftSort])
        passAggregate.extend([passMatch1, passGroup1, passProject1, passGroup2, passProject2, passSort])        

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("passHisAndCurrent")
            pData = self.aggregate(passAggregate)
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftAggregate)
            self.setMongoCollection("reasonHisAndCurrent")
            fData = self.aggregate(fixAggregate)
            self.closeMongoConncetion()
            returnData = {
                "pData": pData,
                "dData": dData,
                "fData": fData
            }
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

    def _groupMSHIPLV2LINEDG(self, data):
        passData = []
        for p in data["pData"]:
            passData.append(p)
        deftData = []
        for d in data["dData"]:
            deftData.append(d)
        fixData = []
        for f in data["fData"]:
            fixData.append(f)

        data = []
        oData = {}
        if deftData != [] and passData != [] :
            for d in deftData:   
                _passData = list(
                    filter(lambda d: d["PROD_NBR"] == d["PROD_NBR"], passData))  
                _fixData = list(
                    filter(lambda d: d["PROD_NBR"] == d["PROD_NBR"], fixData))   
                if len(_passData) > 0: 
                    oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                    oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                    oData["XVALUE"] = copy.deepcopy(d["XVALUE"])                
                    oData["DATARANGE"] = copy.deepcopy(d["DATARANGE"])
                    oData["COMPANY_CODE"] = copy.deepcopy(
                        _passData[0]["COMPANY_CODE"])
                    oData["SITE"] = copy.deepcopy(_passData[0]["SITE"])
                    oData["FACTORY_ID"] = copy.deepcopy(_passData[0]["FACTORY_ID"])
                    if "APPLICATION" in _passData[0].keys():
                        oData["APPLICATION"] = copy.deepcopy(
                            _passData[0]["APPLICATION"])
                    else:
                        oData["APPLICATION"] = None
                    oData["PROD_NBR"] = copy.deepcopy(_passData[0]["PROD_NBR"])
                    oData["DEFT_QTY"] = copy.deepcopy(d["DEFT_QTY"])                    
                    oData["PASS_QTY"] = copy.deepcopy(_passData[0]["PASS_QTY"])
                    if len(_fixData) > 0: 
                        oData["FIX_QTY"] = copy.deepcopy(_fixData[0]["FIX_QTY"])
                    else:
                        oData["FIX_QTY"] = 0
                    oData["denominator_QTY"] = oData["PASS_QTY"]-oData["FIX_QTY"]

                    if oData["denominator_QTY"] > 0 and oData["denominator_QTY"] > oData["DEFT_QTY"]:
                        data.append(copy.deepcopy(oData))
                    oData = {}
        return data

    def _calMSHIPLV2DG(self, tempData, DATARANGE, DATARANGEID):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        allDFCTCount = {}
        for x in tempData:    
            if x["DFCT_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["DFCT_CODE"]] += x["DEFT_QTY"]
            else:
                allDFCTCount[x["DFCT_CODE"]] = x["DEFT_QTY"]
        top10 = dict(sorted(allDFCTCount.items(),key=lambda item:item[1],reverse=True) [:10])
               
        DATASERIES = []
        if tempData == []:
            test = {
                    "XVALUE": DATARANGEID,
                    "YVALUE": 0,
                    "RANK": 0,
                    "DATARANGE": DATARANGE,                    
                    "DFCT_CODE" : "OTHER",
                    "ERRC_DESCR" :  "OTHER",                     
                    "PROD_NBR": tmpPROD_NBR,
                    "PASS_QTY": 0,
                    "FIX_QTY": 0,
                    "DEFT_QTY": 0,
                    "DG_YIELD": 0,
                    "denominator_QTY" : 0
                }
            DATASERIES.append(test)
        else:
            for x in tempData:  
                cDFct = x["DFCT_CODE"] if x["DFCT_CODE"] in top10.keys() else "OTHER"
                cERRC = x["ERRC_DESCR"] if x["DFCT_CODE"] in top10.keys() else "OTHER" 

                rank = 11
                if cDFct in top10.keys():
                    rank = 1
                    for i in top10:
                        if i != x["DFCT_CODE"]:
                            rank +=1 
                        else:
                            break
                                            
                d = list(filter(lambda d: d["DFCT_CODE"] == cDFct and d["XVALUE"] == x["XVALUE"] , DATASERIES))
                if d == []:
                    ds = Decimal(x["DEFT_QTY"])
                    ps = Decimal(x["denominator_QTY"])
                    dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    test = {
                            "XVALUE": x["XVALUE"],
                            "YVALUE": dr*100,
                            "RANK": rank,
                            "DATARANGE": x["DATARANGE"],
                            "DFCT_CODE" : cDFct,
                            "ERRC_DESCR" : cERRC,                        
                            "PROD_NBR": tmpPROD_NBR,
                            "PASS_QTY": x["PASS_QTY"],
                            "FIX_QTY": x["FIX_QTY"],
                            "DEFT_QTY": x["DEFT_QTY"],
                            "DG_YIELD": dr*100,
                            "denominator_QTY" : x["denominator_QTY"]
                        }
                    DATASERIES.append(test)
                
                else:
                    for cx in DATASERIES:
                        if  cx["DFCT_CODE"] == cDFct :                        
                            cx["DEFT_QTY"] += x["DEFT_QTY"]
                            ds = Decimal(cx["DEFT_QTY"])
                            ps = Decimal(cx["denominator_QTY"])
                            dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                            cx["DG_YIELD"] = dr*100
                            cx["YVALUE"] =  dr*100

            DATASERIES.sort(key = operator.itemgetter("RANK", "RANK"), reverse = True)

        returnData = DATASERIES

        return returnData

    def _DecimaltoFloat(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)