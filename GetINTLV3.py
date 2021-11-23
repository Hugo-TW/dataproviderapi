# -*- coding: utf-8 -*-
import configparser
import copy
import datetime
import json
import operator
import sys
import time
import traceback
from decimal import ROUND_HALF_UP, Decimal
from re import X

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
        self.__tmpAPPLICATION = ""
        self.operSetData = {
            "M011": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95},
                        "IAVM": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95},
                        "AUTO": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95},
                        "mLED": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95},
                        "TFT Sensor": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95}
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
                        "CE": {"qytlim2": 1500, "qytlim": 500, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500,"qytlim": 500, "FPY": 0.89},
                        "NB": {"qytlim2": 1500,"qytlim": 500, "FPY": 0.93},
                        "TV": {"qytlim2": 1500,"qytlim": 500, "FPY": 0.90},
                        "AA": {"qytlim2": 1500,"qytlim": 500, "FPY": 0.95}
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
                        "CE": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95}
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
            "J004": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500,"qytlim": 1000, "FPY": 0.95}
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
        self.__indentity = "INT_ORACLEDB_TEST"

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
            """
            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
            """
            # Check Redis Data

            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

            if tmpKPITYPE == "FPYLV3LINE":                
                expirTimeKey = tmpFACTORY_ID + '_PASS'
                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3w"], dataRange["n3w_array"], 3)
                n1m_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getFPYLV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                magerData = self._groupINTLV3(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)

                ERRC_DESCR = ""
                if tmpSITE == "TN":
                    ERRC_DESCR = self._deftCodeInf(tmpFACTORY_ID, tmpCHECKCODE) if tmpCHECKCODE != None else ''
                else:
                    ERRC_DESCR = self._deftCodeInf("J001", tmpCHECKCODE) if tmpCHECKCODE != None else ''
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
                    "ERRC_DESCR": ERRC_DESCR.strip(),
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

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getEFALV3DATA(tmpOPER, tmpPROD_NBR, tmpCHECKCODE, dataRange["n3w"], dataRange["n3w_array"], 3)
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

            elif tmpKPITYPE == "EFALV2LINE":
                expirTimeKey = tmpFACTORY_ID + '_REASON'

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                BONDINGData = self._getEFALV2DATA("BONDING", tmpPROD_NBR, dataRange)
                LAMData = self._getEFALV2DATA("LAM", tmpPROD_NBR, dataRange)
                AAFCData = self._getEFALV2DATA("AAFC", tmpPROD_NBR, dataRange)
                CKENData = self._getEFALV2DATA("CKEN", tmpPROD_NBR, dataRange)

                magerData =  self._groupEFALV2(BONDINGData,LAMData,AAFCData,CKENData) 
                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
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

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV2LINEData(tmpOPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
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

                getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"] if tmpSITE == "TN" else {}
                xLimit = None
                yLimit = None
                if self.__tmpAPPLICATION in getLimitData.keys():
                    xLimit = getLimitData[self.__tmpAPPLICATION]["qytlim"]
                    yLimit = getLimitData[self.__tmpAPPLICATION]["FPY"] * 100
                else:
                    xLimit = 1000
                    yLimit = 90

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "OPER": tmpOPER,
                    "xLimit": xLimit,
                    "yLimit": 100-yLimit,
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

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
                n1m_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
                n2m_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
                n1s_DATA = self._getFPYLV2LINEDataALL(tmpOPER, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)
                
                DATASERIES = self._grouptFPYLV2LINE(
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n1d_DATA["dData"], n1d_DATA["pData"]), tmpOPER, dataRange["n1d"], 11),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n2d_DATA["dData"], n2d_DATA["pData"]), tmpOPER, dataRange["n2d"], 10),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n3d_DATA["dData"], n3d_DATA["pData"]), tmpOPER, dataRange["n3d"], 9),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n4d_DATA["dData"], n4d_DATA["pData"]), tmpOPER, dataRange["n4d"], 8),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n5d_DATA["dData"], n5d_DATA["pData"]), tmpOPER, dataRange["n5d"], 7),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n6d_DATA["dData"], n6d_DATA["pData"]), tmpOPER, dataRange["n6d"], 6),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n1w_DATA["dData"], n1w_DATA["pData"]), tmpOPER, dataRange["n1w"], 5),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n2w_DATA["dData"], n2w_DATA["pData"]), tmpOPER, dataRange["n2w"], 4),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n3w_DATA["dData"], n3w_DATA["pData"]), tmpOPER, dataRange["n3w"], 3),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n1m_DATA["dData"], n1m_DATA["pData"]), tmpOPER, dataRange["n1m"], 2),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n2m_DATA["dData"], n2m_DATA["pData"]), tmpOPER, dataRange["n2m"], 1),
                    self._calFPYLV2LINEOPER(self._groupPassDeftByPRODandOPERALL(n1s_DATA["dData"], n1s_DATA["pData"]), tmpOPER, dataRange["n1s"], 0))

                FPYLINE = self._getFPYLINEData(tmpPROD_NBR, dataRange)

                getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"] if tmpSITE == "TN" else {}
                xLimit = None
                yLimit = None
                if self.__tmpAPPLICATION in getLimitData.keys():
                    xLimit = getLimitData[self.__tmpAPPLICATION]["qytlim"]
                    yLimit = getLimitData[self.__tmpAPPLICATION]["FPY"] * 100
                else:
                    xLimit = 1000
                    yLimit = 90

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "OPER": "ALL",
                    "xLimit": xLimit,
                    "yLimit": 100-yLimit,
                    "DATASERIES": DATASERIES,
                    "FPYLINE": FPYLINE
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

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getMSHIPLV2LINE(getFabData, tmpPROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
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

                dataRange =  self._dataRangeMin(tmpACCT_DATE)

                n1d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)
                n2d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
                n3d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
                n4d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
                n5d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
                n6d_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
                n1w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
                n2w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
                n3w_DATA = self._getMSHIPLV2LINEDG(tmpPROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
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

            elif tmpKPITYPE == "MSHIPLINEALL":
                expirTimeKey = tmpFACTORY_ID + '_SCRP'

                dataRange =  self._dataRangeMin(tmpACCT_DATE)
                DATASERIES = self._getMSHIPSCRAPData(tmpPROD_NBR, dataRange)
                MSHIPLINE = self._getMSHIPLINEData(tmpPROD_NBR, dataRange)

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,  
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,                                      
                    "OPER": "ALL",
                    "DATASERIES": DATASERIES,
                    "MSHIPLINE": MSHIPLINE
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
        n1s_start = n1s_end.replace(month= n1s_end.month-2, day=1)
        
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

    def _dataRangeMin(self, ACCT_DATE):
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
        n1s_start = n1s_end.replace(month= n1s_end.month-2, day=1)
        
        returnData = {
            "n1d": d.strftime(n1d_sd,'%m%d'),
            "n1d_array": self._dataArray(n1d_sd,n1d_sd),
            "n2d": d.strftime(n2d_sd,'%m%d'),
            "n2d_array": self._dataArray(n2d_sd,n2d_sd),
            "n3d": d.strftime(n3d_sd,'%m%d'),
            "n3d_array": self._dataArray(n3d_sd,n3d_sd),
            "n4d": d.strftime(n4d_sd,'%m%d'),
            "n4d_array": self._dataArray(n4d_sd,n4d_sd),
            "n5d": d.strftime(n5d_sd,'%m%d'),
            "n5d_array": self._dataArray(n5d_sd,n5d_sd),
            "n6d": d.strftime(n6d_sd,'%m%d'),
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

    def _getFPYLV3DATAFromOracle(self, OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        _ACCT_DATE_ARRAY_LIST = ""
        for x in ACCT_DATE_ARRAY:
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST + f"'{x}',"
        if _ACCT_DATE_ARRAY_LIST != "":
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST[:-1]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        try:
            self.getConnection(self.__indentity)
            fpyString = f"with pass as( \
                            SELECT \
                                dmo.application    AS APPLICATION, \
                                dmo.code           AS prod_nbr, \
                                dop.name           AS OPER, \
                                '{DATARANGENAME}' AS DATARANGE, \
                                {TYPE} AS XVALUE, \
                                SUM(fpa.sumqty) AS PASSSUMQTY \
                            FROM \
                                INTMP_DB.fact_fpy_pass_sum fpa \
                                LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fpa.local_id \
                                LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fpa.model_id \
                                LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fpa.oper_id \
                            WHERE \
                                dlo.company_code = '{tmpCOMPANY_CODE}' \
                                AND dlo.site_code = '{tmpSITE}' \
                                AND dlo.factory_code = '{tmpFACTORY_ID}' \
                                AND dop.name = '{OPER}' \
                                AND fpa.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                                and dmo.code = '{PROD_NBR}' \
                            GROUP BY \
                                dmo.application, \
                                dmo.code, \
                                dop.name \
                            HAVING SUM(fpa.sumqty) > 0  \
                            Order by dmo.code), \
                            deft as ( \
                            SELECT \
                                dmo.application    AS APPLICATION, \
                                dmo.code           AS prod_nbr, \
                                dop.name           AS OPER, \
                                '{DATARANGENAME}' AS DATARANGE, \
                                {TYPE} AS XVALUE, \
                                SUM(fdf.sumqty) AS DEFTSUMQTY \
                            FROM \
                                INTMP_DB.fact_fpy_deft_sum fdf \
                                LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fdf.local_id \
                                LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fdf.model_id \
                                LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fdf.oper_id    \
                            WHERE \
                                dlo.company_code = '{tmpCOMPANY_CODE}' \
                                AND dlo.site_code = '{tmpSITE}' \
                                AND dlo.factory_code = '{tmpFACTORY_ID}' \
                                AND dop.name = '{OPER}' \
                                AND fdf.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                                and dmo.code = '{PROD_NBR}' \
                                and fdf.deftcode = '{DEFECTCODE}' \
                            GROUP BY \
                                dmo.application, \
                                dmo.code, \
                                dop.name \
                            HAVING SUM(fdf.sumqty) > 0  \
                            Order by dmo.code) \
                            select   \
                            pa.APPLICATION, \
                            pa.prod_nbr, \
                            pa.OPER, \
                            pa.DATARANGE, \
                            pa.XVALUE, \
                            nvl(df.DEFTSUMQTY,0) as DEFTQTY, \
                            pa.PASSSUMQTY as PASSQTY, \
                            nvl(trunc(df.DEFTSUMQTY/pa.PASSSUMQTY,6),0) as DEFECT_YIELD \
                            from pass pa left join deft df \
                            on df.APPLICATION = pa.APPLICATION \
                            and df.prod_nbr = pa.prod_nbr \
                            and df.OPER = pa.OPER \
                            and df.DATARANGE = pa.DATARANGE \
                            and df.XVALUE = pa.XVALUE"
            description , data = self.SelectAndDescription(fpyString)            
            rData = self._zipDescriptionAndData(description, data)  
            self.closeConnection()
            return rData

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

    def _getFPYLV3DATAFromMongo(self, OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
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
                    "DEFTQTY": {
                        "$sum": {"$toInt": "$QTY"}
                    }
                }
            },
            {
                "$addFields": {
                    "APPLICATION" : "$_id.APPLICATION",
                    "PROD_NBR": "$_id.PROD_NBR",
                    "DEFTQTY": "$DEFTQTY",
                    "PASSQTY": 0
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
                                        "PASSQTY": {
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
                                        "PASSQTY": "$PASSQTY",
                                        "DEFTQTY": 0
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
                    "DEFTQTY": {
                        "$sum": "$DEFTQTY"
                    },
                    "PASSQTY": {
                        "$sum": "$PASSQTY"
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
                                    "$PASSQTY",
                                    0
                                ]
                            },
                            0,
                            {
                                "$divide": [
                                    "$DEFTQTY",
                                    "$PASSQTY"
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

    def _getFPYLV3DATA(self, OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYLV3DATAFromMongo(OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
            else:
                data = self._getFPYLV3DATAFromOracle(OPER, PROD_NBR, DEFECTCODE, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
                
            return data

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
                    "WORK_CTR": "2110",
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
                    "PASSQTY": 0
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
                                        "PASSQTY": {
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
                                        "PASSQTY": "$PASSQTY",
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
                    "PASSQTY": {
                        "$sum": "$PASSQTY"
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
                                    "$PASSQTY",
                                    0
                                ]
                            },
                            0,
                            {
                                "$divide": [
                                    "$reasonQty",
                                    "$PASSQTY"
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
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYLV2LINEDataFromMongo(OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
            else:
                data = self._getFPYLV2LINEDataFromOracle(OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
                
            return data

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

    def _getFPYLV2LINEDataFromMongo(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
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
                "PASSSUMQTY": {
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
                "PASSSUMQTY": "$PASSSUMQTY"
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

    def _getFPYLV2LINEDataFromOracle(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        _ACCT_DATE_ARRAY_LIST = ""
        for x in ACCT_DATE_ARRAY:
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST + f"'{x}',"
        if _ACCT_DATE_ARRAY_LIST != "":
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST[:-1]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        try:
            self.getConnection(self.__indentity)
            passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            '{DATARANGENAME}' AS DATARANGE, \
                            {TYPE} AS XVALUE, \
                            SUM(fpa.sumqty) AS PASSSUMQTY \
                        FROM \
                            INTMP_DB.fact_fpy_pass_sum fpa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fpa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fpa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fpa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name = '{OPER}' \
                            AND fpa.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fpa.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(fpa.sumqty) > 0 "
            description , data = self.SelectAndDescription(passString)            
            pData = self._zipDescriptionAndData(description, data)  
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            '{DATARANGENAME}' AS DATARANGE, \
                            {TYPE} AS XVALUE, \
                            ddf.DEFTCODE as DFCT_CODE, \
                            ddf.DEFTCODE_DESC as ERRC_DESCR, \
                            SUM(fdf.sumqty) AS DEFT_QTY \
                        FROM \
                            INTMP_DB.fact_fpy_deft_sum fdf \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fdf.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fdf.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fdf.oper_id \
                            LEFT JOIN INTMP_DB.dime_deftcode ddf ON ddf.DEFTCODE= fdf.DEFTCODE \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name ='{OPER}' \
                            AND fdf.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fdf.mfgdate, \
                            dmo.application, \
                            dop.name, \
                            ddf.DEFTCODE, \
                            ddf.DEFTCODE_DESC \
                        HAVING SUM(fdf.sumqty) > 0 "
            description , data = self.SelectAndDescription(deftString)            
            dData = self._zipDescriptionAndData(description, data)  
            self.closeConnection()

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
                oData["OPER"] = copy.deepcopy(p["OPER"])
                oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                oData["PASSSUMQTY"] = copy.deepcopy(p["PASSSUMQTY"])
                if d == []:
                    oData["DEFTSUMQTY"] = 0
                else:
                    oData["DEFTSUMQTY"] = copy.deepcopy(d["DEFT_QTY"])
                if oData["DEFTSUMQTY"] < oData["PASSSUMQTY"]:
                    data.append(copy.deepcopy(oData))
                oData = {}
        return data

    def _calFPYLV2LINEOPER(self, tempData, OPER, DATARANGE, DATARANGEID):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        if tempData != []:
            self.__tmpAPPLICATION =  tempData[0]["APPLICATION"]

        allDFCTCount = {}
        for x in tempData:  
            if x["DFCT_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["DFCT_CODE"]] += x["DEFTSUMQTY"]
            else:
                allDFCTCount[x["DFCT_CODE"]] = x["DEFTSUMQTY"]
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
                    ds = Decimal(x["DEFTSUMQTY"])
                    ps = Decimal(x["PASSSUMQTY"])
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
                            "DeftSUM": x["DEFTSUMQTY"],
                            "PassSUM": x["PASSSUMQTY"],
                            "DEFECT_RATE": dr*100
                        }
                    DATASERIES.append(test)
                
                else:
                    for cx in DATASERIES:
                        if cx["OPER"] == x["OPER"] and cx["DFCT_CODE"] == cDFct :                        
                            cx["DeftSUM"] += x["DEFTSUMQTY"]
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
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYLV2LINEDataALLFromMongo(OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
            else:
                data = self._getFPYLV2LINEDataALLFromOracle(OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE)
             
            return data

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

    def _getFPYLV2LINEDataALLFromMongo(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]

        getFabData = self.operSetData[tmpFACTORY_ID]          
        denominatorValue = getFabData["FPY"]["denominator"]

        passAggregate_PCBI = []
        passAggregate_LAM = []
        passAggregate_AAFC = []
        passAggregate_CKEN = []
        passAggregate_DKEN = []
        deftAggregate = []
        
        #pass
        passMatchPCBI = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue["PCBI"]]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passMatchLAM = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue["LAM"]]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passMatchAAFC = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue["AAFC"]]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passMatchCKEN = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue["CKEN"]]},
                "RW_COUNT": {"$lte": "1"}
            }
        }
        passMatchDKEN = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "PROD_NBR": PROD_NBR,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue["DKEN"]]},
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
                "PASSSUMQTY": {
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
                "PASSSUMQTY": "$PASSSUMQTY"
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
        passAggregate_PCBI.extend([passMatchPCBI, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])
        passAggregate_LAM.extend([passMatchLAM, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])
        passAggregate_AAFC.extend([passMatchAAFC, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])
        passAggregate_CKEN.extend([passMatchCKEN, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])        
        passAggregate_DKEN.extend([passMatchDKEN, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])        

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("passHisAndCurrent")
            PCBI_Data = self.aggregate(passAggregate_PCBI)
            LAM_Data = self.aggregate(passAggregate_LAM)
            AAFC_Data = self.aggregate(passAggregate_AAFC)
            CKEN_Data = self.aggregate(passAggregate_CKEN)
            DKEN_Data = self.aggregate(passAggregate_DKEN)
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftAggregate)
            self.closeMongoConncetion()
            returnData = {
                "pData": {
                    "PCBI": PCBI_Data,
                    "LAM": LAM_Data,
                    "AAFC": AAFC_Data,
                    "CKEN": CKEN_Data,
                    "DKEN": DKEN_Data},
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

    def pass_FPYLV2LINEDataALL_SQL(self, OPER, PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE):        
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            '{DATARANGENAME}' AS DATARANGE, \
                            {TYPE} AS XVALUE, \
                            SUM(fpa.sumqty) AS PASSSUMQTY \
                        FROM \
                            INTMP_DB.fact_fpy_pass_sum fpa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fpa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fpa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fpa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name = '{OPER}' \
                            AND fpa.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fpa.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(fpa.sumqty) > 0 "
        self.getConnection(self.__indentity)
        description , data = self.SelectAndDescription(passString)
        pData = self._zipDescriptionAndData(description, data)  
        self.closeConnection()
        return pData

    def _getFPYLV2LINEDataALLFromOracle(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        _ACCT_DATE_ARRAY_LIST = ""
        for x in ACCT_DATE_ARRAY:
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST + f"'{x}',"
        if _ACCT_DATE_ARRAY_LIST != "":
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST[:-1]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        try:
            self.getConnection(self.__indentity)
            passArray = {
                    "PCBI": self.pass_FPYLV2LINEDataALL_SQL("PCBI", PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE),
                    "LAM": self.pass_FPYLV2LINEDataALL_SQL("LAM", PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE),
                    "AAFC":  self.pass_FPYLV2LINEDataALL_SQL("AAFC", PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE),
                    "CKEN": self.pass_FPYLV2LINEDataALL_SQL("CKEN", PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE),
                    "DKEN": self.pass_FPYLV2LINEDataALL_SQL("DKEN", PROD_NBR, DATARANGENAME, _ACCT_DATE_ARRAY_LIST, TYPE)
                }  
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            'ALL'           AS OPER, \
                            '{DATARANGENAME}' AS DATARANGE, \
                            {TYPE} AS XVALUE, \
                            ddf.DEFTCODE as DFCT_CODE, \
                            ddf.DEFTCODE_DESC as ERRC_DESCR, \
                            SUM(fdf.sumqty) AS DEFT_QTY \
                        FROM \
                            INTMP_DB.fact_fpy_deft_sum fdf \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fdf.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fdf.model_id \
                            LEFT JOIN INTMP_DB.dime_deftcode ddf ON ddf.DEFTCODE= fdf.DEFTCODE \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND fdf.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fdf.mfgdate, \
                            dmo.application, \
                            ddf.DEFTCODE, \
                            ddf.DEFTCODE_DESC \
                        HAVING SUM(fdf.sumqty) > 0 "
            self.getConnection(self.__indentity)
            description , data = self.SelectAndDescription(deftString)            
            dData = self._zipDescriptionAndData(description, data)  
            self.closeConnection()

            returnData = {
                "pData": passArray,
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

    def _groupPassDeftByPRODandOPERALL(self, dData, pData):        
        tempPassData = []
        for p in pData["PCBI"]:
            tempPassData.append(p)
        for p in pData["LAM"]:
            tempPassData.append(p)
        for p in pData["AAFC"]:
            tempPassData.append(p)
        for p in pData["CKEN"]:
            tempPassData.append(p)
        for p in pData["DKEN"]:
            tempPassData.append(p)

        passData = []
        PASSQTYSUM = 0
        PASSOPER = 0
        for x in tempPassData:
            if x["PASSSUMQTY"] > 0:
                PASSQTYSUM += x["PASSSUMQTY"]
                PASSOPER += 1

        if PASSOPER != 0:
            AVGPASS = round(PASSQTYSUM/PASSOPER, 4)
            passData.append({
                "COMPANY_CODE": tempPassData[0]["COMPANY_CODE"],
                "SITE": tempPassData[0]["SITE"],
                "FACTORY_ID": tempPassData[0]["FACTORY_ID"],
                "PROD_NBR": tempPassData[0]["PROD_NBR"],
                "APPLICATION": tempPassData[0]["APPLICATION"],
                "PASSSUMQTY": AVGPASS,
                "OPER": "ALL"
            })

        deftData = []
        for d in dData:
            deftData.append(d)
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
                oData["OPER"] = copy.deepcopy(p["OPER"])
                oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                oData["PASSSUMQTY"] = copy.deepcopy(p["PASSSUMQTY"])
                if d == []:
                    oData["DEFTSUMQTY"] = 0
                else:
                    oData["DEFTSUMQTY"] = copy.deepcopy(d["DEFT_QTY"])
                if oData["DEFTSUMQTY"] < oData["PASSSUMQTY"]:
                    data.append(copy.deepcopy(oData))
                oData = {}
        return data

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

    def _getFPYLINEData(self, PROD_NBR, dataRange):
        try:
            n1d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)            
            n2d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
            n3d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
            n4d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
            n5d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
            n6d_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
            n1w_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
            n2w_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
            n3w_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
            n1m_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
            n2m_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
            n1s_DATA = self._getFPYLINEDatabyDateRange(PROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)

            returnData =  DATASERIES = self._grouptFPYLV2LINE(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,
                n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)
            
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

    def _getFPYLINEDatabyDateRange(self, PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID):
        try:
            PCBIData = self._getFPYLINEDatabyOPER("PCBI", PROD_NBR, ACCT_DATE_ARRAY)
            PCBIResult = self._groupFPYLINEDatabyOPER(
                    PCBIData["dData"], PCBIData["pData"])
            LAMData = self._getFPYLINEDatabyOPER("LAM", PROD_NBR, ACCT_DATE_ARRAY)
            LAMResult = self._groupFPYLINEDatabyOPER(
                    LAMData["dData"], LAMData["pData"])
            AAFCData = self._getFPYLINEDatabyOPER("AAFC", PROD_NBR, ACCT_DATE_ARRAY)
            AAFCResult = self._groupFPYLINEDatabyOPER(
                    AAFCData["dData"], AAFCData["pData"])
            CKENData = self._getFPYLINEDatabyOPER("CKEN", PROD_NBR, ACCT_DATE_ARRAY)
            CKENResult = self._groupFPYLINEDatabyOPER(
                    CKENData["dData"], CKENData["pData"])
            DKENData = self._getFPYLINEDatabyOPER("DKEN", PROD_NBR, ACCT_DATE_ARRAY)
            DKENResult = self._groupFPYLINEDatabyOPER(
                    DKENData["dData"], DKENData["pData"])

            returnData = self._groupFPYLINEData(
                    PROD_NBR, PCBIResult, LAMResult, AAFCResult, CKENResult, DKENResult, DATARANGE, DATARANGEID)
            
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

    def _getFPYLINEDatabyOPER(self, OPER, PROD_NBR, ACCT_DATE_ARRAY):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYLINEDatabyOPERFromMongo(OPER, PROD_NBR, ACCT_DATE_ARRAY)
            else:
                data = self._getFPYLINEDatabyOPERFromOracle(OPER, PROD_NBR, ACCT_DATE_ARRAY)             
            return data

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

    def _getFPYLINEDatabyOPERFromMongo(self, OPER, PROD_NBR, ACCT_DATE_ARRAY):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER]
        denominatorValue = getFabData["FPY"]["denominator"][OPER]

        passAggregate = []
        deftAggregate = []

        # pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, denominatorValue]},
                "RW_COUNT": {"$lte": "1"},
                "PROD_NBR": PROD_NBR
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
                "PASSSUMQTY": {
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
                "OPER": OPER,
                "PASSSUMQTY": "$PASSSUMQTY"
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

        # deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {
                    "$and": [
                        {"$gte": [{"$toInt": "$MAIN_WC"},
                                  numeratorData["fromt"]]},
                        {"$lte": [{"$toInt": "$MAIN_WC"}, numeratorData["tot"]]}
                    ]
                },
                "RW_COUNT": {"$lte": "1"},
                "PROD_NBR": PROD_NBR
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
                    "MAIN_WC": {"$toInt": "$MAIN_WC"}
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
                "MAIN_WC": "$_id.MAIN_WC",
                "DEFT_QTY": "$DEFT_QTY"
            }
        }
        deftGroup2 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "APPLICATION": "$APPLICATION"
                },
                "DEFTSUMQTY": {
                    "$sum": {"$toInt": "$DEFT_QTY"}
                }
            }
        }
        deftProject2 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "APPLICATION": "$_id.APPLICATION",
                "OPER": OPER,
                "DEFTSUMQTY": "$DEFTSUMQTY"
            }
        }
        deftSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            passMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            deftMatch1["$match"]["APPLICATION"] = tmpAPPLICATION

        passAggregate.extend(
            [passMatch1, passGroup1, passProject1, passGroup2, passProject2, passSort])
        deftAggregate.extend(
            [deftMatch1, deftGroup1, deftProject1, deftGroup2, deftProject2, deftSort])

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
    
    def _getFPYLINEDatabyOPERFromOracle(self, OPER, PROD_NBR, ACCT_DATE_ARRAY):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        _ACCT_DATE_ARRAY_LIST = ""
        for x in ACCT_DATE_ARRAY:
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST + f"'{x}',"
        if _ACCT_DATE_ARRAY_LIST != "":
            _ACCT_DATE_ARRAY_LIST = _ACCT_DATE_ARRAY_LIST[:-1]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        try:
            self.getConnection(self.__indentity)
            passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            SUM(fpa.sumqty) AS PASSSUMQTY \
                        FROM \
                            INTMP_DB.fact_fpy_pass_sum fpa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fpa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fpa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fpa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name = '{OPER}' \
                            AND fpa.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fpa.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(fpa.sumqty) > 0 "
            description , data = self.SelectAndDescription(passString)            
            pData = self._zipDescriptionAndData(description, data)
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            SUM(fdf.sumqty) AS DEFTSUMQTY \
                        FROM \
                            INTMP_DB.fact_fpy_deft_sum fdf \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = fdf.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = fdf.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = fdf.oper_id \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name ='{OPER}' \
                            AND fdf.mfgdate in ({_ACCT_DATE_ARRAY_LIST}) \
                            AND dmo.code = '{PROD_NBR}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            fdf.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(fdf.sumqty) > 0 "
            description , data = self.SelectAndDescription(deftString)            
            dData = self._zipDescriptionAndData(description, data)  
            self.closeConnection()

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

    def _groupFPYLINEDatabyOPER(self, dData, pData):
        deftData = []
        for d in dData:
            deftData.append(d)
        passData = []
        for p in pData:
            passData.append(p)
        data = []
        oData = {}
        for p in passData:
            d = list(filter(lambda d: d["PROD_NBR"]
                     == p["PROD_NBR"], deftData))
            oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
            oData["SITE"] = copy.deepcopy(p["SITE"])
            oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
            oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
            if "APPLICATION" in p.keys():
                oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
            else:
                oData["APPLICATION"] = None
            oData["OPER"] = copy.deepcopy(p["OPER"])
            oData["PASSSUMQTY"] = copy.deepcopy(p["PASSSUMQTY"])
            if d == []:
                oData["DEFTSUMQTY"] = 0
            else:
                oData["DEFTSUMQTY"] = copy.deepcopy(d[0]["DEFTSUMQTY"])
            if oData["DEFTSUMQTY"] == 0:
                oData["DEFECT_RATE"] = 0
            else:
                if(oData["PASSSUMQTY"] != 0):
                    oData["DEFECT_RATE"] = round(
                        oData["DEFTSUMQTY"] / oData["PASSSUMQTY"], 4)
                else:
                    oData["DEFECT_RATE"] = 1
            oData["FPY_RATE"] = round(1 - oData["DEFECT_RATE"], 4)
            if oData["DEFTSUMQTY"] < oData["PASSSUMQTY"] and oData["FPY_RATE"] > 0:
                data.append(copy.deepcopy(oData))
            oData = {}
        return data
    
    def _groupFPYLINEData(self, PROD_NBR, PCBI, LAM, AAFC, CKEN, DKEN, DATARANGE, DATARANGEID):
        PRODData = []
        PASSQTYSUM = 0
        PASSOPER = 0
        DEFTQTYSUM = 0

        d1 = list(filter(lambda d: d["PROD_NBR"]
                      == PROD_NBR, PCBI))
        if d1 == []:
            PCBIFPY = 1
        else:
            PCBIFPY = copy.deepcopy(d1[0]["FPY_RATE"])
            PASSQTYSUM += d1[0]["PASSSUMQTY"]            
            DEFTQTYSUM += d1[0]["DEFTSUMQTY"]
            PASSOPER += 1            
        
        d2 = list(filter(lambda d: d["PROD_NBR"] == PROD_NBR, LAM))
        if d2 == []:
            LAMFPY = 1
        else:
            LAMFPY = copy.deepcopy(d2[0]["FPY_RATE"])
            PASSQTYSUM += d2[0]["PASSSUMQTY"]
            DEFTQTYSUM += d2[0]["DEFTSUMQTY"]
            PASSOPER += 1

        d3 = list(filter(lambda d: d["PROD_NBR"]
                    == PROD_NBR, AAFC))
        if d3 == []:
            AAFCFPY = 1
        else:
            AAFCFPY = copy.deepcopy(d3[0]["FPY_RATE"])
            PASSQTYSUM += d3[0]["PASSSUMQTY"]
            DEFTQTYSUM += d3[0]["DEFTSUMQTY"]
            PASSOPER += 1

        d4 = list(filter(lambda d: d["PROD_NBR"]
                    == PROD_NBR, CKEN))
        if d4 == []:
            CKENFPY = 1
        else:
            CKENFPY = copy.deepcopy(d4[0]["FPY_RATE"])
            PASSQTYSUM += d4[0]["PASSSUMQTY"]
            DEFTQTYSUM += d4[0]["DEFTSUMQTY"]
            PASSOPER += 1

        d5 = list(filter(lambda d: d["PROD_NBR"]
                    == PROD_NBR, DKEN))
        if d5 == []:
            DKENFPY = 1
        else:
            DKENFPY = copy.deepcopy(d5[0]["FPY_RATE"])
            PASSQTYSUM += d5[0]["PASSSUMQTY"]
            DEFTQTYSUM += d5[0]["DEFTSUMQTY"]
            PASSOPER += 1

        FPY = round(PCBIFPY * LAMFPY * AAFCFPY * CKENFPY * DKENFPY, 4)

        if d1 == [] and d2 == [] and d3 == [] and d4 == [] and d5 == []:
            return PRODData 
        else:
            PRODData.append({
                "XVALUE": DATARANGEID,
                "YVALUE": FPY,
                "RANK": 0,
                "DATARANGE": DATARANGE, 
                "PROD_NBR": PROD_NBR,
                "PCBIFPY": PCBIFPY,
                "LAMFPY": LAMFPY,
                "AAFCFPY": AAFCFPY,
                "CKENFPY": CKENFPY,
                "DKENFPY": DKENFPY,
                "FPY": FPY,
                "AvegPASSQTY": round(PASSQTYSUM / PASSOPER, 0),
                "DEFTSUMQTY": DEFTQTYSUM
            })      
        return PRODData 

    def _getMSHIPLINEData(self, PROD_NBR, dataRange):
        try:
            n1d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)            
            n2d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
            n3d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
            n4d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
            n5d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
            n6d_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
            n1w_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
            n2w_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
            n3w_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
            n1m_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
            n2m_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
            n1s_DATA = self._getMSHIPLINEDatabyDateRange(PROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)

            returnData =  DATASERIES = self._grouptFPYLV2LINE(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,
                n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)
            
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

    def _getMSHIPLINEDatabyDateRange(self, PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID):
        try:
            MSHIPData = self._getMSHIPData(PROD_NBR, ACCT_DATE_ARRAY)
            groupMSHIPDAT = self._groupMSHIPData(PROD_NBR, MSHIPData)

            returnData = []
            if len(groupMSHIPDAT) == 0 :
                    return returnData 
            else:
                returnData.append({
                    "XVALUE": DATARANGEID,
                    "YVALUE": groupMSHIPDAT["MSHIP"],
                    "RANK": 0,
                    "DATARANGE": DATARANGE, 
                    "PROD_NBR": PROD_NBR,
                    "TOBESCRAP_SUMQTY": groupMSHIPDAT["TOBESCRAP_SUMQTY"],
                    "SHIP_SUMQTY": groupMSHIPDAT["SHIP_SUMQTY"],
                    "TOTAL_YIELD": groupMSHIPDAT["TOTAL_YIELD"],
                    "DOWNGRADE_SUMQTY": groupMSHIPDAT["DOWNGRADE_SUMQTY"],
                    "TOTAL_SUMQTY": groupMSHIPDAT["TOTAL_SUMQTY"],
                    "GRADW_YIELD": groupMSHIPDAT["GRADW_YIELD"],
                    "MSHIP": groupMSHIPDAT["MSHIP"]
                })                  
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

    def _getMSHIPData(self, PROD_NBR, ACCT_DATE_ARRAY):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        scrapAggregate = []
        shipAggregate = []
        gradeAggregate = []

        # scrap
        scrapMatch = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,                
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
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
                    "PROD_NBR": "$PROD_NBR"
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
                "TOBESCRAP_SUMQTY": "$TOBESCRAP_QTY"
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

        # ship
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
        shipSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "APPLICATION": 1,
                "ACCT_DATE": 1,
                "PROD_NBR": 1
            }
        }

        # grade
        gradeMatch = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "FACTORY_ID": tmpFACTORY_ID,
                "SITE": tmpSITE,                
                "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                "MAIN_WC": {"$in": ["1600"]},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "PROD_NBR": PROD_NBR
            }
        }
        gradeGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "APPLICATION": "$APPLICATION",
                    "PROD_NBR": "$PROD_NBR"
                },
                "DOWNGRADE_SUMQTY": {
                    "$sum": {
                        "$cond": [
                            {
                                "$eq": ["$IS_DOWNGRADE", "Y"]
                            },
                            {
                                "$toInt": "$QTY"
                            },
                            {
                                "$toInt": 0
                            }
                        ]
                    }
                },
                "TOTAL_SUMQTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        gradeProject = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "APPLICATION": "$_id.APPLICATION",
                "PROD_NBR": "$_id.PROD_NBR",
                "DOWNGRADE_SUMQTY": "$DOWNGRADE_SUMQTY",
                "TOTAL_SUMQTY": "$TOTAL_SUMQTY"
            }
        }
        gradeSort = {
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
            gradeMatch["$match"]["APPLICATION"] = tmpAPPLICATION

        scrapAggregate.extend(
            [scrapMatch, scrapGroup, scrapProject, scrapSort])
        shipAggregate.extend([shipMatch, shipGroup, shipProject, shipSort])
        gradeAggregate.extend(
            [gradeMatch, gradeGroup, gradeProject, gradeSort])

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("scrapHisAndCurrent")
            scrapData = self.aggregate(scrapAggregate)
            self.setMongoCollection("passHisAndCurrent")
            shipData = self.aggregate(shipAggregate)
            self.setMongoCollection("passHisAndCurrent")
            gradeData = self.aggregate(gradeAggregate)
            self.closeMongoConncetion()
            returnData = {
                "scrapData": scrapData,
                "shipData": shipData,
                "gradeData": gradeData
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

    def _groupMSHIPData(self, PROD_NBR, MSHIPData):
        scrapData = []
        for scrap in MSHIPData["scrapData"]:
            scrapData.append(scrap)
        shipData = []
        for ship in MSHIPData["shipData"]:
            shipData.append(ship)
        gradeData = []
        for grade in MSHIPData["gradeData"]:
            gradeData.append(grade)

        oData = {}
        _scrapdata = list(
            filter(lambda d: d["PROD_NBR"] == PROD_NBR, scrapData))
        _shipData = list(
            filter(lambda d: d["PROD_NBR"] == PROD_NBR, shipData))
        _gradeData = list(
            filter(lambda d: d["PROD_NBR"] == PROD_NBR, gradeData))
        if _shipData != [] and _gradeData != []:
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
            if _scrapdata == []:
                oData["TOBESCRAP_SUMQTY"] = 0
            else:
                oData["TOBESCRAP_SUMQTY"] = copy.deepcopy(
                    _scrapdata[0]["TOBESCRAP_SUMQTY"])
            oData["SHIP_SUMQTY"] = copy.deepcopy(
                _shipData[0]["SHIP_SUMQTY"])
            if oData["TOBESCRAP_SUMQTY"] == 0:
                oData["TOTAL_YIELD"] = 1
            else:
                oData["TOTAL_YIELD"] = 1 - \
                    round(oData["TOBESCRAP_SUMQTY"] /
                            oData["SHIP_SUMQTY"], 6)
            oData["DOWNGRADE_SUMQTY"] = copy.deepcopy(
                _gradeData[0]["DOWNGRADE_SUMQTY"])
            oData["TOTAL_SUMQTY"] = copy.deepcopy(
                _gradeData[0]["TOTAL_SUMQTY"])

            if oData["DOWNGRADE_SUMQTY"] == 0:
                oData["GRADW_YIELD"] = 1
            else:
                oData["GRADW_YIELD"] = 1 - \
                    round(oData["DOWNGRADE_SUMQTY"] /
                            oData["TOTAL_SUMQTY"], 6)
            oData["MSHIP"] = round(
                oData["GRADW_YIELD"] * oData["TOTAL_YIELD"], 6)        
        return oData

    def _getMSHIPSCRAPData(self, PROD_NBR, dataRange):
        try:
            n1d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)            
            n2d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
            n3d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
            n4d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
            n5d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
            n6d_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
            n1w_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
            n2w_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
            n3w_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
            n1m_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
            n2m_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
            n1s_DATA = self._getMSHIPSCRAPDatabyDateRange(PROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)

            returnData =  DATASERIES = self._grouptFPYLV2LINE(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,
                n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)
            
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
    
    def _getMSHIPSCRAPDatabyDateRange(self, PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID):
        try:
            formerfabData = self._getMSHIPSCRAPDataformDB("formerfab",PROD_NBR,DATARANGE, ACCT_DATE_ARRAY, DATARANGEID)
            fabData = self._getMSHIPSCRAPDataformDB("fab",PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID)
            incomingData = self._getMSHIPSCRAPDataformDB("incoming",PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID)

            returnData = []
            for d in formerfabData["scrapData"]:   
                returnData.append(d)
            for d in fabData["scrapData"]:   
                returnData.append(d)
            for d in incomingData["scrapData"]:   
                returnData.append(d)
                            
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

    def _getMSHIPSCRAPDataformDB(self,type,PROD_NBR, DATARANGE, ACCT_DATE_ARRAY, DATARANGEID):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        """
        (1)  前廠責：USL、TX LCD、FABX
        (2)  廠責：MFG、INT、EQP、ER 
        (3)  來料責：SQE
        """
        mshipDATA = {
            "formerfab":{"in":"usl|lcd|fab","name": "前廠責"},
            "fab":{"in":"mfg|int|eqp|er","name": "廠責"},
            "incoming":{"in":"sqe","name": "SQE來料責"},
        }
        getFabData = mshipDATA[type]   

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
                    "PROD_NBR": "$PROD_NBR"
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
                "TOBESCRAP_SUMQTY": "$TOBESCRAP_QTY",
                "YVALUE": "$TOBESCRAP_QTY"
            }
        }
        scrapAdd = {
            "$addFields": {
                "XVALUE": DATARANGEID,
                "RANK": 0,
                "DATARANGE": DATARANGE, 
                "RESP_OWNER": getFabData["name"],
                "RESP_OWNER_E": type
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

        if tmpAPPLICATION != "ALL":
            scrapMatch["$match"]["APPLICATION"] = tmpAPPLICATION

        scrapAggregate.extend(
            [scrapMatch, scrapGroup, scrapProject, scrapAdd, scrapSort])

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("scrapHisAndCurrent")
            scrapData = self.aggregate(scrapAggregate)
            self.closeMongoConncetion()

            returnData = {
                "scrapData": scrapData,
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

    def _zipDescriptionAndData(self, description, data):
        """ 取得 description和data壓縮後資料
            description :row column description 
            data : row data 
            回傳 [{key:value}]
        """
        try:
            col_names = [row[0] for row in description]
            dictdatan = [dict(zip(col_names, da)) for da in data]
            return dictdatan
        except Exception as e:
            error_class = e.__class__.__name__  # 取得錯誤類型
            detail = e.args[0]  # 取得詳細內容
            cl, exc, tb = sys.exc_info()  # 取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
            fileName = lastCallStack[0]  # 取得發生的檔案名稱
            lineNum = lastCallStack[1]  # 取得發生的行號
            funcName = lastCallStack[2]  # 取得發生的函數名稱
            self.writeError("File:[{0}] , Line:{1} , in {2} : [{3}] {4}".format(
                fileName, lineNum, funcName, error_class, detail))
            return None
    
    def _getEFALV2DATA(self, OPER, tmpPROD_NBR, dataRange):   
        try:
            n1d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n1d"], dataRange["n1d_array"], 11)            
            n2d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n2d"], dataRange["n2d_array"], 10)
            n3d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n3d"], dataRange["n3d_array"], 9)
            n4d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n4d"], dataRange["n4d_array"], 8)
            n5d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n5d"], dataRange["n5d_array"], 7)
            n6d_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n6d"], dataRange["n6d_array"], 6)
            n1w_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n1w"], dataRange["n1w_array"], 5)
            n2w_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n2w"], dataRange["n2w_array"], 4)
            n3w_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n3w"], dataRange["n3w_array"], 3)
            n1m_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n1m"], dataRange["n1m_array"], 2)
            n2m_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n2m"], dataRange["n2m_array"], 1)
            n1s_DATA = self._getEFALV2DATAbyDateRange(OPER, tmpPROD_NBR, dataRange["n1s"], dataRange["n1s_array"], 0)

            returnData = self._grouptFPYLV2LINE(n1d_DATA,n2d_DATA,n3d_DATA,n4d_DATA,n5d_DATA,
                n6d_DATA,n1w_DATA,n2w_DATA,n3w_DATA,n1m_DATA,n2m_DATA,n1s_DATA)
            
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

    def _getEFALV2DATAbyDateRange(self, OPER, PROD_NBR, DATARANGENAME, ACCT_DATE_ARRAY, TYPE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
             
        OPERDATA = {
            "BONDING":{"OPER": [1300,1301]},
            "LAM":{"OPER": [1340,1370]},
            "AAFC":{"OPER": [1419,1420]},
            "CKEN":{"OPER": [1600]}        
            } 
        #"TPI":{"OPER": [1510]},
        #"OTPC":{"OPER": [1590]}
        OPERARRAY = OPERDATA[OPER]["OPER"]
        
        EFALV2_Aggregate = [
            {
                "$match": {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,  
                    "ACCT_DATE": {"$in": ACCT_DATE_ARRAY},
                    "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                    "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERARRAY]}
                }
            },
            {
                "$group": {
                    "_id": {
                        "FACTORY_ID" : "$FACTORY_ID"
                    },
                    "DEFTQTY": {
                        "$sum": {"$toInt": "$QTY"}
                    }
                }
            },
            {
                "$addFields": {
                    "FACTORY_ID" : "$_id.FACTORY_ID",
                    "DEFTQTY": "$DEFTQTY",
                    "PASSQTY": 0
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
                                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERARRAY]}
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "FACTORY_ID" : "$FACTORY_ID"
                                },
                                "PASSQTY": {
                                    "$sum": {
                                        "$toInt": "$QTY"
                                    }
                                }
                            }
                        },
                        {
                            "$addFields": {
                                "FACTORY_ID" : "$_id.FACTORY_ID",
                                "PASSQTY": "$PASSQTY",
                                "DEFTQTY": 0
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
                        "FACTORY_ID" : "$FACTORY_ID"
                    },
                    "DEFTQTY": {
                        "$sum": "$DEFTQTY"
                    },
                    "PASSQTY": {
                        "$sum": "$PASSQTY"
                    }
                }
            },
            {
                "$addFields": {
                    "OPER" : OPER,
                    "DATARANGE": DATARANGENAME,
                    "XVALUE": TYPE,
                    "RANK": 0,
                    "DEFECT_YIELD": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$PASSQTY",
                                    0
                                ]
                            },
                            0,
                            {
                                "$divide": [
                                    "$DEFTQTY",
                                    "$PASSQTY"
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
            }
        ]

        if PROD_NBR != '':
            EFALV2_Aggregate[0]["$match"]["PROD_NBR"] = PROD_NBR
            EFALV2_Aggregate[4]["$unionWith"]["pipeline"][0]["$match"]["PROD_NBR"] = PROD_NBR
               
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            returnData = self.aggregate(EFALV2_Aggregate)
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

    def _groupEFALV2(self, data1,data2,data3,data4): 
        magerData = [] 
        for d in data1:   
            d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
            magerData.append(d)
        for d in data2:
            d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0       
            magerData.append(d)
        for d in data3:   
            d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
            magerData.append(d)              
        for d in data4:   
            d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
            magerData.append(d)
        return magerData

