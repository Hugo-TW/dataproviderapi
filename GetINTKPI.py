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
import decimal


class INTKPI(BaseType):
    def __init__(self, jsonData, _db_pool):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        # M011 => MOD1
        # J001 => MOD2
        # J003 => MOD3        
        self.operSetData = {
            "M011": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95},
                        "IAVM": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95},
                        "AUTO": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95},
                        "mLED": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95},
                        "TFT Sensor": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95}
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
                        "TABLET": {"qytlim2": 1500, "qytlim": 500, "FPY": 0.89},
                        "NB": {"qytlim2": 1500, "qytlim": 500, "FPY": 0.93},
                        "TV": {"qytlim2": 1500, "qytlim": 500, "FPY": 0.90},
                        "AA": {"qytlim2": 1500, "qytlim": 500, "FPY": 0.95}
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
                        "CE": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95}
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
                        "CE": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.94},
                        "TABLET": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.89},
                        "NB": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.93},
                        "TV": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.90},
                        "AA": {"qytlim2": 1500, "qytlim": 1000, "FPY": 0.95}
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
        self.pool = _db_pool

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
            tmpOPER = self.jsonData["OPER"] if "OPER" in self.jsonData else "CKEN"
            tmpPROD_NBR = self.jsonData["PROD_NBR"] if "PROD_NBR" in self.jsonData else ""
            tmpNG2NG = self.jsonData["NG2NG"] if "NG2NG" in self.jsonData else True
            tmpFILTERQTY= self.jsonData["FILTERQTY"] if "FILTERQTY" in self.jsonData else True

            # redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpKPITYPE)
            tmp.append(tmpACCT_DATE)
            tmp.append(tmpOPER)
            tmp.append(tmpPROD_NBR) 
            tmp.append(f'{tmpNG2NG}')       
            tmp.append(f'{tmpFILTERQTY}')             
            redisKey = bottomLine.join(tmp)
            print(redisKey)
            expirTimeKey = tmpFACTORY_ID + '_PASS'
            """
            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
            """
            # Check Redis Data
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

            # 一階 FPY KPI API
            if tmpKPITYPE == "FPY":
                expirTimeKey = tmpFACTORY_ID + '_PASS'

                PCBIData = self._getFPYData("PCBI")
                PCBIResult = self._groupPassDeftByPRODandOPER(
                    PCBIData["dData"], PCBIData["pData"])
                LAMData = self._getFPYData("LAM")
                LAMResult = self._groupPassDeftByPRODandOPER(
                    LAMData["dData"], LAMData["pData"])
                AAFCData = self._getFPYData("AAFC")
                AAFCResult = self._groupPassDeftByPRODandOPER(
                    AAFCData["dData"], AAFCData["pData"])
                CKENData = self._getFPYData("CKEN")
                CKENResult = self._groupPassDeftByPRODandOPER(
                    CKENData["dData"], CKENData["pData"])
                DKENData = self._getFPYData("DKEN")
                DKENResult = self._groupPassDeftByPRODandOPER(
                    DKENData["dData"], DKENData["pData"])

                PRODFPYBaseData = self._groupPRODFPYBaseData(
                    PCBIResult, LAMResult, AAFCResult, CKENResult, DKENResult)
                returnData = self._calFPYData(PRODFPYBaseData)

                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 二階 FPY 泡泡圖 API
            elif tmpKPITYPE == "PRODFPY":
                expirTimeKey = tmpFACTORY_ID + '_PASS'
                # Check Redis Data
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.writeLog(f"Cache Data From Redis")
                    return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

                PCBIData = self._getFPYData("PCBI")
                PCBIResult = self._groupPassDeftByPRODandOPER(
                    PCBIData["dData"], PCBIData["pData"])
                LAMData = self._getFPYData("LAM")
                LAMResult = self._groupPassDeftByPRODandOPER(
                    LAMData["dData"], LAMData["pData"])
                AAFCData = self._getFPYData("AAFC")
                AAFCResult = self._groupPassDeftByPRODandOPER(
                    AAFCData["dData"], AAFCData["pData"])
                CKENData = self._getFPYData("CKEN")
                CKENResult = self._groupPassDeftByPRODandOPER(
                    CKENData["dData"], CKENData["pData"])
                DKENData = self._getFPYData("DKEN")
                DKENResult = self._groupPassDeftByPRODandOPER(
                    DKENData["dData"], DKENData["pData"])

                PRODFPYBaseData = self._groupPRODFPYBaseData(
                    PCBIResult, LAMResult, AAFCResult, CKENResult, DKENResult)
                returnData = self._calPRODFPYData(PRODFPYBaseData)

                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 二階 FPY 泡泡圖 API
            elif tmpKPITYPE == "PRODFPYList":
                expirTimeKey = tmpFACTORY_ID + '_PASS'
                # Check Redis Data
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.writeLog(f"Cache Data From Redis")
                    return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

                PCBIData = self._getFPYData("PCBI")
                PCBIResult = self._groupPassDeftByPRODandOPER(
                    PCBIData["dData"], PCBIData["pData"])
                LAMData = self._getFPYData("LAM")
                LAMResult = self._groupPassDeftByPRODandOPER(
                    LAMData["dData"], LAMData["pData"])
                AAFCData = self._getFPYData("AAFC")
                AAFCResult = self._groupPassDeftByPRODandOPER(
                    AAFCData["dData"], AAFCData["pData"])
                CKENData = self._getFPYData("CKEN")
                CKENResult = self._groupPassDeftByPRODandOPER(
                    CKENData["dData"], CKENData["pData"])
                DKENData = self._getFPYData("DKEN")
                DKENResult = self._groupPassDeftByPRODandOPER(
                    DKENData["dData"], DKENData["pData"])

                PRODFPYBaseData = self._groupPRODFPYBaseData(
                    PCBIResult, LAMResult, AAFCResult, CKENResult, DKENResult)
                returnData = self._calPRODFPYListData(PRODFPYBaseData)

                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 一階 MSHIP KPI API
            elif tmpKPITYPE == "MSHIP":
                expirTimeKey = tmpFACTORY_ID + '_SCRP'
                MSHIPData = self._getMSHIPData()
                groupMSHIPDAT = self._groupMSHIPData(MSHIPData)
                returnData = self._calMSHIPData(groupMSHIPDAT)
                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 二階 MSHIP 泡泡圖 API
            elif tmpKPITYPE == "PRODMSHIP":
                expirTimeKey = tmpFACTORY_ID + '_SCRP'
                # Check Redis Data
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.writeLog(f"Cache Data From Redis")
                    return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

                MSHIPData = self._getMSHIPData()
                groupMSHIPDAT = self._groupMSHIPData(MSHIPData)
                returnData = self._calPRODMSHIPData(groupMSHIPDAT)

                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 一階 EFA KPI API
            elif tmpKPITYPE == "EFA":
                expirTimeKey = tmpFACTORY_ID + '_PASS'
                efaData = self._getEFAData(tmpOPER)
                groupEFAData = self._groupEFAData(
                    efaData["dData"], efaData["pData"])
                returnData = self._calEFAData(groupEFAData)

                # 存到 redis 暫存
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # 二階 EFA 泡泡圖 API
            elif tmpKPITYPE == "PRODEFA":
                expirTimeKey = tmpFACTORY_ID + '_DEFT'

                efaData = self._getEFADatabyDeft(tmpOPER)
                groupEFAData = self._groupEFADatabyDeft(
                    efaData["dData"], efaData["pData"], tmpOPER)
                returnData = self._calPRODEFAData(groupEFAData, tmpOPER)

                # 存到 redis 暫存
                """self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)"""

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "PRODEFALIST":
                expirTimeKey = tmpFACTORY_ID + '_DEFT'

                efaData = self._getEFADatabyDeft(tmpOPER)
                groupEFAData = self._groupEFADatabyDeft(
                    efaData["dData"], efaData["pData"], tmpOPER)
                returnData = self._calPRODEFAListData(groupEFAData, tmpOPER)

                # 存到 redis 暫存
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

    def _getFPYDataFromOracle(self, OPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "
        try:
            passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            fpa.mfgdate        AS acct_date, \
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
                            AND fpa.mfgdate = '{tmpACCT_DATE}' \
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
            description, data = self.pSelectAndDescription(passString)
            pData = self._zipDescriptionAndData(description, data)
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            fdf.mfgdate        AS acct_date, \
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
                            AND fdf.mfgdate = '{tmpACCT_DATE}' \
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
            description, data = self.pSelectAndDescription(deftString)
            dData = self._zipDescriptionAndData(description, data)

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

    def _getFPYDataFromMongo(self, OPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]        
        tmpNG2NG = self.jsonData["NG2NG"] if "NG2NG" in self.jsonData else True

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
                "ACCT_DATE": tmpACCT_DATE,
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
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
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
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                "$expr": {
                    "$and": [
                        {"$gte": [{"$toInt": "$MAIN_WC"},
                                  numeratorData["fromt"]]},
                        {"$lte": [{"$toInt": "$MAIN_WC"}, numeratorData["tot"]]}
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
                    "ACCT_DATE": "$ACCT_DATE",
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
                            "ACCT_DATE": "$_id.ACCT_DATE",
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
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            passMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            deftMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            
        if tmpNG2NG is not True:
            deftMatch1["$match"]["TRANS_TYPE"] = {"$ne":"QRWK"}


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

    def _getFPYData(self, OPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYDataFromMongo(OPER)
            else:
                data = self._getFPYDataFromOracle(OPER)
            returnData = {
                "pData": data["pData"],
                "dData": data["dData"]
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
        for p in passData:
            d = list(filter(lambda d: d["PROD_NBR"]
                     == p["PROD_NBR"], deftData))
            oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
            oData["SITE"] = copy.deepcopy(p["SITE"])
            oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
            oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
            oData["ACCT_DATE"] = datetime.datetime.strptime(
                p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
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

    def _groupPRODFPYBaseData(self, PCBI, LAM, AAFC, CKEN, DKEN):
        PRODList = []
        for x in PCBI:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in LAM:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in AAFC:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in CKEN:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in DKEN:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})

        PRODData = []
        PASSQTYSUM = 0
        DEFTQTYSUM = 0
        PASSOPER = 0
        tmpFILTERQTY= self.jsonData["FILTERQTY"] if "FILTERQTY" in self.jsonData else True
        for prod in PRODList:
            passQtyLimt = 0
            fpyLimt = 0
            if tmpFILTERQTY == True:
                passQtyLimt = 300 if prod["APPLICATION"] == "NB" else 500
                fpyLimt = 0.8 if prod["APPLICATION"] == "NB" else 0.8

            d1 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"] 
                      and d["FPY_RATE"] >= fpyLimt
                      and d["PASSSUMQTY"] >= passQtyLimt, PCBI))
            if d1 == []:
                PCBIFPY = 1
            else:
                PCBIFPY = copy.deepcopy(d1[0]["FPY_RATE"])
                PASSQTYSUM += d1[0]["PASSSUMQTY"]
                DEFTQTYSUM += d1[0]["DEFTSUMQTY"]
                PASSOPER += 1

            d2 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"] 
                      and d["FPY_RATE"] >= fpyLimt
                      and d["PASSSUMQTY"] >= passQtyLimt, LAM))
            if d2 == []:
                LAMFPY = 1
            else:
                LAMFPY = copy.deepcopy(d2[0]["FPY_RATE"])
                PASSQTYSUM += d2[0]["PASSSUMQTY"]
                DEFTQTYSUM += d2[0]["DEFTSUMQTY"]
                PASSOPER += 1

            d3 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"] 
                      and d["FPY_RATE"] >= fpyLimt
                      and d["PASSSUMQTY"] >= passQtyLimt, AAFC))
            if d3 == []:
                AAFCFPY = 1
            else:
                AAFCFPY = copy.deepcopy(d3[0]["FPY_RATE"])
                PASSQTYSUM += d3[0]["PASSSUMQTY"]
                DEFTQTYSUM += d3[0]["DEFTSUMQTY"]
                PASSOPER += 1

            d4 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"] 
                      and d["FPY_RATE"] >= fpyLimt
                      and d["PASSSUMQTY"] >= passQtyLimt, CKEN))
            if d4 == []:
                CKENFPY = 1
            else:
                CKENFPY = copy.deepcopy(d4[0]["FPY_RATE"])
                PASSQTYSUM += d4[0]["PASSSUMQTY"]
                DEFTQTYSUM += d4[0]["DEFTSUMQTY"]
                PASSOPER += 1

            d5 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"] 
                      and d["FPY_RATE"] >= fpyLimt
                      and d["PASSSUMQTY"] >= passQtyLimt, DKEN))
            if d5 == []:
                DKENFPY = 1
            else:
                DKENFPY = copy.deepcopy(d5[0]["FPY_RATE"])
                PASSQTYSUM += d5[0]["PASSSUMQTY"]
                DEFTQTYSUM += d5[0]["DEFTSUMQTY"]
                PASSOPER += 1

            FPY = round(PCBIFPY * LAMFPY * AAFCFPY * CKENFPY * DKENFPY, 4)

            if PASSOPER > 0:
                PRODData.append({
                    "PROD_NBR": prod['PROD_NBR'],
                    "APPLICATION": prod["APPLICATION"],
                    "PCBIFPY": PCBIFPY,
                    "LAMFPY": LAMFPY,
                    "AAFCFPY": AAFCFPY,
                    "CKENFPY": CKENFPY,
                    "DKENFPY": DKENFPY,
                    "FPY": FPY,
                    "AvegPASSQTY": round(PASSQTYSUM / PASSOPER, 0),
                    "DEFTSUM": DEFTQTYSUM
                })
            PASSQTYSUM = 0
            PASSOPER = 0

        return PRODData

    def _calFPYData(self, PRODFPYBaseData):
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"] if tmpSITE == "TN" else {
        }

        GREEN_VALUE = 0
        YELLOW_VALUE = 0
        RED_VALUE = 0

        for x in PRODFPYBaseData:
            targrtFPY = 0.90
            targrtQTY1 = 500
            targrtQTY2 = 1500
            if x["APPLICATION"] in getLimitData.keys():
                targrtFPY = getLimitData[x["APPLICATION"]]["FPY"]
                targrtQTY1 = getLimitData[x["APPLICATION"]]["qytlim"]
                targrtQTY2 = getLimitData[x["APPLICATION"]]["qytlim2"]

            if x["FPY"] >= targrtFPY:
                GREEN_VALUE += 1
            else:
                if targrtQTY1 > x["AvegPASSQTY"]:
                    GREEN_VALUE += 1
                else:
                    if targrtQTY2 > x["AvegPASSQTY"]:
                        YELLOW_VALUE += 1
                    else:
                        RED_VALUE += 1

        returnData = {
            "CLASS_TYPE": "FPY",
            "GREEN_VALUE": GREEN_VALUE,
            "YELLOW_VALUE": YELLOW_VALUE,
            "RED_VALUE": RED_VALUE
        }

        return returnData

    def _calPRODFPYListData(self, PRODFPYBaseData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"] if tmpSITE == "TN" else {
        }

        COLOR = "#118AB2"
        SYMBOL = "undefined"

        DATASERIES = []
        if tmpAPPLICATION == "ALL":
            d = PRODFPYBaseData
            xLimit = 500
            yLimit = 90
        else:
            d = list(filter(lambda d: d["APPLICATION"]
                     == tmpAPPLICATION, PRODFPYBaseData))
            if tmpAPPLICATION in getLimitData.keys():
                xLimit = getLimitData[tmpAPPLICATION]["qytlim"]
                yLimit = getLimitData[tmpAPPLICATION]["FPY"] * 100
            else:
                xLimit = 1000
                yLimit = 90

        # red ef476f
        # yellow ffd166
        # green 06d6a0
        # blue 118AB2
        # midGreen 073b4c

        for x in d:
            targrtFPY = 0.90
            targrtQTY1 = 500
            targrtQTY2 = 1500
            if x["APPLICATION"] in getLimitData.keys():
                targrtFPY = getLimitData[x["APPLICATION"]]["FPY"]
                targrtQTY1 = getLimitData[x["APPLICATION"]]["qytlim"]
                targrtQTY2 = getLimitData[x["APPLICATION"]]["qytlim2"]

            QUADRANT = 0

            if x["FPY"] >= targrtFPY:
                COLOR = "#06d6a0"
                SYMBOL = "undefined"
                if targrtQTY1 > x["AvegPASSQTY"]:
                    QUADRANT = 1
                else:
                    QUADRANT = 2
            else:
                if targrtQTY1 > x["AvegPASSQTY"]:
                    COLOR = "#06d6a0"
                    SYMBOL = "undefined"
                    QUADRANT = 3
                else:
                    if targrtQTY2 > x["AvegPASSQTY"]:
                        COLOR = "#ffd166"
                        SYMBOL = "undefined"
                        QUADRANT = 4
                    else:
                        COLOR = "#EF476F"
                        SYMBOL = "twinkle"
                        QUADRANT = 5

            DATASERIES.append({
                "APPLICATION": x["APPLICATION"],
                "PROD_NBR": x["PROD_NBR"],
                "YIELD": x["FPY"],
                "PASSSUM": x["AvegPASSQTY"],
                "DEFTSUM": x["DEFTSUM"],
                "COLOR": COLOR,
                "SYMBOL": SYMBOL,
                "QUADRANT": QUADRANT
            })
        # 因為使用 operator.itemgetter 方法 排序順序要反過來執行
        # 不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key=operator.itemgetter("PASSSUM"), reverse=True)
        DATASERIES.sort(key=operator.itemgetter("YIELD"), reverse=False)
        DATASERIES.sort(key=operator.itemgetter("QUADRANT"), reverse=True)

        length = len(DATASERIES)
        rank = 1
        for x in range(length):
            DATASERIES[x]["RANK"] = rank
            rank += 1

        selectlistData = []
        for x in DATASERIES:
            _pass = f'{round(int(x["PASSSUM"])/1000,1)}k' if int(
                x["PASSSUM"]) > 99 else f'{int(x["PASSSUM"])}'
            selectlistData.append(
                {
                    "value": x["PROD_NBR"],
                    "text": f'({x["RANK"]}){x["PROD_NBR"]}-FPY:{round(decimal.Decimal(x["YIELD"])*100,2)}%-'
                    f'Pass:{_pass}'
                }
            )

        returnData = {
            "TITLE": "快選機種",
            "SELECTLIST": selectlistData
        }

        return returnData

    def _calPRODFPYData(self, PRODFPYBaseData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"] if tmpSITE == "TN" else {
        }

        COLOR = "#118AB2"
        SYMBOL = "undefined"

        DATASERIES = []
        if tmpAPPLICATION == "ALL":
            d = PRODFPYBaseData
            xLimit = 500
            yLimit = 90
        else:
            d = list(filter(lambda d: d["APPLICATION"]
                     == tmpAPPLICATION, PRODFPYBaseData))
            if tmpAPPLICATION in getLimitData.keys():
                xLimit = getLimitData[tmpAPPLICATION]["qytlim"]
                yLimit = getLimitData[tmpAPPLICATION]["FPY"] * 100
            else:
                xLimit = 1000
                yLimit = 90

        # red ef476f
        # yellow ffd166
        # green 06d6a0
        # blue 118AB2
        # midGreen 073b4c

        for x in d:
            targrtFPY = 0.90
            targrtQTY1 = 500
            targrtQTY2 = 1500
            if x["APPLICATION"] in getLimitData.keys():
                targrtFPY = getLimitData[x["APPLICATION"]]["FPY"]
                targrtQTY1 = getLimitData[x["APPLICATION"]]["qytlim"]
                targrtQTY2 = getLimitData[x["APPLICATION"]]["qytlim2"]

            QUADRANT = 0

            if x["FPY"] >= targrtFPY:
                COLOR = "#06d6a0"
                SYMBOL = "undefined"
                if targrtQTY1 > x["AvegPASSQTY"]:
                    QUADRANT = 1
                else:
                    QUADRANT = 2
            else:
                if targrtQTY1 > x["AvegPASSQTY"]:
                    COLOR = "#06d6a0"
                    SYMBOL = "undefined"
                    QUADRANT = 3
                else:
                    if targrtQTY2 > x["AvegPASSQTY"]:
                        COLOR = "#ffd166"
                        SYMBOL = "undefined"
                        QUADRANT = 4
                    else:
                        COLOR = "#EF476F"
                        SYMBOL = "twinkle"
                        QUADRANT = 5

            DATASERIES.append({
                "APPLICATION": x["APPLICATION"],
                "PROD_NBR": x["PROD_NBR"],
                "YIELD": x["FPY"],
                "QTY": x["AvegPASSQTY"],
                "COLOR": COLOR,
                "SYMBOL": SYMBOL,
                "QUADRANT": QUADRANT
            })
        # 因為使用 operator.itemgetter 方法 排序順序要反過來執行
        # 不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key=operator.itemgetter("QTY"), reverse=True)
        DATASERIES.sort(key=operator.itemgetter("YIELD"), reverse=False)
        DATASERIES.sort(key=operator.itemgetter("QUADRANT"), reverse=True)

        length = len(DATASERIES)
        rank = 1
        for x in range(length):
            DATASERIES[x]["RANK"] = rank
            rank += 1

        returnData = {
            "XLIMIT": xLimit,
            "YLIMIT": yLimit,
            "DATASERIES": DATASERIES
        }

        return returnData

    def _getMSHIPData(self):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getMSHIPDataFromMongoDB()
            else:
                data = self._getMSHIPDataFromOracle()

            returnData = {
                "scrapData": data["scrapData"],
                "shipData": data["shipData"],
                "gradeData": data["gradeData"]
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

    def _getMSHIPDataFromOracle(self):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "
        try:
            shipString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            mpa.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            SUM(mpa.sumqty) AS SHIP_SUMQTY \
                        FROM \
                            INTMP_DB.fact_mship_pass_sum mpa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = mpa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = mpa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = mpa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND mpa.mfgdate = '{tmpACCT_DATE}' \
                            AND dop.name ='SHIP' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            mpa.mfgdate, \
                            dmo.application \
                        HAVING SUM(mpa.sumqty) >= 0 "
            description, data = self.pSelectAndDescription(shipString)
            shipData = self._zipDescriptionAndData(description, data)

            scrapString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            msc.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            SUM(msc.sumqty) AS TOBESCRAP_SUMQTY \
                        FROM \
                            INTMP_DB.fact_mship_scrap_sum msc \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = msc.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = msc.model_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND msc.mfgdate = '{tmpACCT_DATE}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            msc.mfgdate, \
                            dmo.application \
                        HAVING SUM(msc.sumqty) >= 0 "
            description, data = self.pSelectAndDescription(scrapString)
            scrapData = self._zipDescriptionAndData(description, data)

            gradeString = f"with ps as( \
                                SELECT \
                                dlo.company_code   AS company_code, \
                                dlo.site_code      AS site, \
                                dlo.factory_code   AS factory_id, \
                                dmo.code           AS prod_nbr, \
                                mpa.mfgdate        AS acct_date, \
                                dmo.application    AS APPLICATION, \
                                SUM(mpa.sumqty) AS TOTAL_SUMQTY \
                            FROM \
                                INTMP_DB.fact_mship_pass_sum mpa \
                                LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = mpa.local_id \
                                LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = mpa.model_id \
                                LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = mpa.oper_id \
                            WHERE \
                                dlo.company_code = '{tmpCOMPANY_CODE}' \
                                AND dlo.site_code = '{tmpSITE}' \
                                AND dlo.factory_code = '{tmpFACTORY_ID}' \
                                AND mpa.mfgdate = '{tmpACCT_DATE}' \
                                AND dop.name ='DOWNGRADE' \
                                {applicatiionWhere} \
                            GROUP BY \
                                dlo.company_code, \
                                dlo.site_code, \
                                dlo.factory_code, \
                                dmo.code, \
                                mpa.mfgdate, \
                                dmo.application \
                            HAVING SUM(mpa.sumqty) >= 0 ), \
                            dg as ( \
                            SELECT \
                                dlo.company_code   AS company_code, \
                                dlo.site_code      AS site, \
                                dlo.factory_code   AS factory_id, \
                                dmo.code           AS prod_nbr, \
                                mdg.mfgdate        AS acct_date, \
                                dmo.application    AS APPLICATION, \
                                SUM(mdg.sumqty) AS DOWNGRADE_SUMQTY \
                            FROM \
                                INTMP_DB.fact_mship_dg_sum mdg \
                                LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = mdg.local_id \
                                LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = mdg.model_id \
                                LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = mdg.oper_id \
                            WHERE \
                                dlo.company_code = '{tmpCOMPANY_CODE}' \
                                AND dlo.site_code = '{tmpSITE}' \
                                AND dlo.factory_code = '{tmpFACTORY_ID}' \
                                AND mdg.mfgdate = '{tmpACCT_DATE}' \
                                AND dop.name ='DOWNGRADE' \
                                {applicatiionWhere} \
                            GROUP BY \
                                dlo.company_code, \
                                dlo.site_code, \
                                dlo.factory_code, \
                                dmo.code, \
                                mdg.mfgdate, \
                                dmo.application \
                            HAVING SUM(mdg.sumqty) >= 0) \
                            select   \
                            ps.company_code, \
                            ps.site, \
                            ps.prod_nbr, \
                            ps.acct_date, \
                            ps.APPLICATION, \
                            nvl(dg.DOWNGRADE_SUMQTY,0) as DOWNGRADE_SUMQTY, \
                            nvl(ps.TOTAL_SUMQTY,0) as TOTAL_SUMQTY \
                            from ps left join dg \
                            on ps.company_code = dg.company_code \
                            and ps.site = dg.site \
                            and ps.prod_nbr = dg.prod_nbr \
                            and ps.acct_date = dg.acct_date \
                            and ps.APPLICATION = dg.APPLICATION "
            description, data = self.pSelectAndDescription(gradeString)
            gradeData = self._zipDescriptionAndData(description, data)

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

    def _getMSHIPDataFromMongoDB(self):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
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
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]}
            }
        }
        scrapGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
                "APPLICATION": 1,
                "ACCT_DATE": 1,
                "PROD_NBR": 1
            }
        }

        # ship
        shipAggregate = []
        shipMatch = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "TRANS_TYPE": "SHIP",
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]}
            }
        }
        shipGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
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
                "ACCT_DATE": tmpACCT_DATE,
                "MAIN_WC": {"$in": ["1600"]},
                "LCM_OWNER": {"$in": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]}
            }
        }
        gradeGroup = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "ACCT_DATE": "$ACCT_DATE",
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
                "APPLICATION": 1,
                "ACCT_DATE": 1,
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

    def _groupMSHIPData(self, MSHIPData):
        scrapData = []
        for scrap in MSHIPData["scrapData"]:
            scrapData.append(scrap)
        shipData = []
        for ship in MSHIPData["shipData"]:
            shipData.append(ship)
        gradeData = []
        for grade in MSHIPData["gradeData"]:
            gradeData.append(grade)

        # PRODList
        PRODList = []
        for x in shipData:
            if "APPLICATION" not in x:
                x["APPLICATION"] = "ALL"
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in gradeData:
            if "APPLICATION" not in x:
                x["APPLICATION"] = "ALL"
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        #
        mshipData = []
        oData = {}
        for prod in PRODList:
            _scrapdata = list(
                filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"], scrapData))
            _shipData = list(
                filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"], shipData))
            _gradeData = list(
                filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"], gradeData))
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
                oData["ACCT_DATE"] = datetime.datetime.strptime(
                    _shipData[0]["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
                if _scrapdata == []:
                    oData["TOBESCRAP_SUMQTY"] = 0
                else:
                    oData["TOBESCRAP_SUMQTY"] = copy.deepcopy(
                        _scrapdata[0]["TOBESCRAP_SUMQTY"])
                oData["SHIP_SUMQTY"] = copy.deepcopy(
                    _shipData[0]["SHIP_SUMQTY"])
                if oData["TOBESCRAP_SUMQTY"] == 0 or oData["SHIP_SUMQTY"] == 0:
                    oData["TOTAL_YIELD"] = 1
                else:
                    oData["TOTAL_YIELD"] = 1 - \
                        round(oData["TOBESCRAP_SUMQTY"] /
                              oData["SHIP_SUMQTY"], 4)
                oData["DOWNGRADE_SUMQTY"] = copy.deepcopy(
                    _gradeData[0]["DOWNGRADE_SUMQTY"])
                oData["TOTAL_SUMQTY"] = copy.deepcopy(
                    _gradeData[0]["TOTAL_SUMQTY"])

                if oData["DOWNGRADE_SUMQTY"] == 0 or oData["TOTAL_SUMQTY"] == 0:
                    oData["GRADW_YIELD"] = 1
                else:
                    oData["GRADW_YIELD"] = 1 - \
                        round(oData["DOWNGRADE_SUMQTY"] /
                              oData["TOTAL_SUMQTY"], 4)
                oData["MSHIP"] = round(
                    oData["GRADW_YIELD"] * oData["TOTAL_YIELD"], 4)
                if oData["MSHIP"] > 0:
                    mshipData.append(copy.deepcopy(oData))
                oData = {}
        return mshipData

    def _calMSHIPData(self, groupMSHIPData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["M-SHIP"]["limit"] if tmpSITE == "TN" else {}

        GREEN_VALUE = 0
        YELLOW_VALUE = 0
        RED_VALUE = 0

        for x in groupMSHIPData:
            targrt = 0.90
            targrtQTY = 1000
            if x["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[x["APPLICATION"]]["target"]
                targrtQTY = getLimitData[x["APPLICATION"]]["qytlim"]

            if x["MSHIP"] >= targrt:
                GREEN_VALUE += 1
            else:
                if targrtQTY > x["TOTAL_SUMQTY"]:
                    YELLOW_VALUE += 1
                else:
                    RED_VALUE += 1

        returnData = {
            "CLASS_TYPE": "M-SHIP",
            "GREEN_VALUE": GREEN_VALUE,
            "YELLOW_VALUE": YELLOW_VALUE,
            "RED_VALUE": RED_VALUE
        }

        return returnData

    def _calPRODMSHIPData(self, PRODMSHIPBaseData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["M-SHIP"]["limit"] if tmpSITE == "TN" else {}

        COLOR = "#118AB2"
        SYMBOL = "undefined"

        DATASERIES = []
        if tmpAPPLICATION == "ALL":
            d = PRODMSHIPBaseData
            xLimit = 1000
            yLimit = 90
        else:
            d = list(filter(lambda d: d["APPLICATION"]
                     == tmpAPPLICATION, PRODMSHIPBaseData))
            if tmpAPPLICATION in getLimitData.keys():
                yLimit = getLimitData[tmpAPPLICATION]["target"]
                xLimit = getLimitData[tmpAPPLICATION]["qytlim"]
            else:
                xLimit = 1000
                yLimit = 90

        # red ef476f
        # yellow ffd166
        # green 06d6a0
        # blue 118AB2
        # midGreen 073b4c

        for x in d:
            targrt = 0.90
            targrtQTY = 1000
            if x["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[x["APPLICATION"]]["target"]
                targrtQTY = getLimitData[x["APPLICATION"]]["qytlim"]

            QUADRANT = 0

            if x["MSHIP"] >= targrt:
                COLOR = "#06d6a0"
                SYMBOL = "undefined"
                if targrtQTY > x["SHIP_SUMQTY"]:
                    QUADRANT = 1
                else:
                    QUADRANT = 2
            else:
                if targrtQTY > x["SHIP_SUMQTY"]:
                    COLOR = "#ffd166"
                    SYMBOL = "undefined"
                    QUADRANT = 3
                else:
                    COLOR = "#EF476F"
                    SYMBOL = "twinkle"
                    QUADRANT = 4

            DATASERIES.append({
                "APPLICATION": x["APPLICATION"],
                "PROD_NBR": x["PROD_NBR"],
                "YIELD": x["MSHIP"],
                "QTY": x["SHIP_SUMQTY"],
                "COLOR": COLOR,
                "SYMBOL": SYMBOL,
                "QUADRANT": QUADRANT
            })
        # 因為使用 operator.itemgetter 方法 排序順序要反過來執行
        # 不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key=operator.itemgetter("QTY"), reverse=True)
        DATASERIES.sort(key=operator.itemgetter("YIELD"), reverse=False)
        DATASERIES.sort(key=operator.itemgetter("QUADRANT"), reverse=True)

        length = len(DATASERIES)
        rank = 1
        for x in range(length):
            DATASERIES[x]["RANK"] = rank
            rank += 1

        returnData = {
            "XLIMIT": xLimit,
            "YLIMIT": yLimit,
            "DATASERIES": DATASERIES
        }

        return returnData

    def _getEFASet(self, COMPANY_CODE, SITE, FACTORY_ID, OPER):
        EFAOPERDATA = {
            "M011": {
                "numerator": {"BONDING": {"OPER": [1300]},
                              "LAM": {"OPER": []},
                              "AAFC": {"OPER": [1400]},
                              "TPI": {"OPER": []},
                              "OTPC": {"OPER": [1530]},
                              "CKEN": {"OPER": [1600]}},
                "denominator": {"BONDING": {"OPER": [1300]},
                                "LAM": {"OPER": []},
                                "AAFC": {"OPER": [1400]},
                                "TPI": {"OPER": []},
                                "OTPC": {"OPER": [1530]},
                                "CKEN": {"OPER": [1600]}},
                "LCM_OWNER": ["INT0", "PROY", "PROD", "QTAP", "RES0"]
            },
            "J001": {
                "numerator": {"BONDING": {"OPER": [1300, 1301]},
                              "LAM": {"OPER": [1340, 1370]},
                              "AAFC": {"OPER": [1419, 1420]},
                              "TPI": {"OPER": [1510]},
                              "OTPC": {"OPER": [1590]},
                              "CKEN": {"OPER": [1600]}},
                "denominator": {"BONDING": {"OPER": [1300, 1301]},
                                "LAM": {"OPER": [1340, 1370]},
                                "AAFC": {"OPER": [1419, 1420]},
                                "TPI": {"OPER": [1510]},
                                "OTPC": {"OPER": [1590]},
                                "CKEN": {"OPER": [1600]}},
                "LCM_OWNER": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]
            },
            "J003": {
                "numerator": {"BONDING": {"OPER": [1300]},
                              "LAM": {"OPER": [1340]},
                              "AAFC": {"OPER": [1420]},
                              "TPI": {"OPER": [1510]},
                              "OTPC": {"OPER": []},
                              "CKEN": {"OPER": [1600]}},
                "denominator": {"BONDING": {"OPER": [1300]},
                                "LAM": {"OPER": [1340]},
                                "AAFC": {"OPER": [1420]},
                                "TPI": {"OPER": [1510]},
                                "OTPC": {"OPER": []},
                                "CKEN": {"OPER": [1600]}},
                "LCM_OWNER": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]
            },
            "J004": {
                "numerator": {"BONDING": {"OPER": [1300]},
                              "LAM": {"OPER": [1340, 1370, 1380, 1398, 1399]},
                              "AAFC": {"OPER": [1420, 1430, 1426, 1427]},
                              "TPI": {"OPER": []},
                              "OTPC": {"OPER": []},
                              "CKEN": {"OPER": [1503, 1510, 1520, 1580, 1581, 1600, 1630, 1740, 1750, 1760, 1751, 1752]}},
                "denominator": {"BONDING": {"OPER": [1300]},
                                "LAM": {"OPER": [1355]},
                                "AAFC": {"OPER": [1700]},
                                "TPI": {"OPER": []},
                                "OTPC": {"OPER": []},
                                "CKEN": {"OPER": [1700]}},
                "LCM_OWNER": ["LCME", "PROD", "QTAP", "RES0"]
            },
            "OTHER": {
                "numerator": {"BONDING": {"OPER": [1300, 1301]},
                              "LAM": {"OPER": [1340, 1370]},
                              "AAFC": {"OPER": [1419, 1420]},
                              "TPI": {"OPER": [1510]},
                              "OTPC": {"OPER": [1590]},
                              "CKEN": {"OPER": [1600]}},
                "denominator": {"BONDING": {"OPER": [1300, 1301]},
                                "LAM": {"OPER": [1340, 1370]},
                                "AAFC": {"OPER": [1419, 1420]},
                                "TPI": {"OPER": [1510]},
                                "OTPC": {"OPER": [1590]},
                                "CKEN": {"OPER": [1600]}},
                "LCM_OWNER": ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]
            },
        }
        EFA_nu_setting = {}
        EFA_de_setting = {}
        EFA_lo_setting = []
        if SITE == "TN":
            EFA_nu_setting = EFAOPERDATA[FACTORY_ID]["numerator"]
            EFA_de_setting = EFAOPERDATA[FACTORY_ID]["denominator"]
            EFA_lo_setting = EFAOPERDATA[FACTORY_ID]["LCM_OWNER"]
        else:
            EFA_nu_setting = EFAOPERDATA["OTHER"]["numerator"]
            EFA_de_setting = EFAOPERDATA["OTHER"]["denominator"]
            EFA_lo_setting = EFAOPERDATA["OTHER"]["LCM_OWNER"]
        _OPERLIST= {}
        OPERCODEList_nu = []
        OPERCODEList_de = []
        OPERNAMEList_nu = ""
        OPERNAMEList_de = ""
        LCMOWNER = []
        if OPER == "ALL":
            LCMOWNER = ["INT0", "LCM0", "LCME", "PROD", "QTAP", "RES0"]
            for key, value in EFA_nu_setting.items():
                OPERCODEList_nu.extend(value.get("OPER"))
                OPERNAMEList_nu = OPERNAMEList_nu + f"'{key}',"
            for key, value in EFA_de_setting.items():
                OPERCODEList_de.extend(value.get("OPER"))
                OPERNAMEList_de = OPERNAMEList_de + f"'{key}',"
        else:
            LCMOWNER = EFA_lo_setting
            OPERCODEList_nu.extend(EFA_nu_setting[OPER]["OPER"])
            OPERCODEList_de.extend(EFA_de_setting[OPER]["OPER"])
            OPERNAMEList_nu = f"'{OPER}',"
            OPERNAMEList_de = f"'{OPER}',"
        if OPERNAMEList_nu != "":
            OPERNAMEList_nu = OPERNAMEList_nu[:-1]
        if OPERNAMEList_de != "":
            OPERNAMEList_de = OPERNAMEList_de[:-1]
                
        _OPERLIST= {
            "COMPANY_CODE": COMPANY_CODE,
            "SITE": SITE,
            "FACTORY_ID": FACTORY_ID,
            "OPER": OPER,
            "OPERLIST":{
                "numerator": OPERCODEList_nu,
                "denominator": OPERCODEList_de
            },
            "NAMELIST":{
                "numerator": OPERNAMEList_nu,
                "denominator": OPERNAMEList_de
            },
            "LCMOWNER": LCMOWNER
        }
        return _OPERLIST

    def _getEFADatabyDeft(self, tmpOPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]        
        EFASet= self._getEFASet( tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID, tmpOPER)
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getEFADatabyDeftFromMongoDB(EFASet)
            else:
                data = self._getEFADatabyDeftFromOracle(EFASet)
            returnData = {
                "pData": data["pData"],
                "dData": data["dData"]
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

    def _getEFADatabyDeftFromOracle(self, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"] if "PROD_NBR" in self.jsonData else ""        
        tmpKPITYPE = self.jsonData["KPITYPE"]
        OPERList = EFASet["NAMELIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "
        if tmpPROD_NBR != '' and tmpKPITYPE == 'PRODEFA':
            applicatiionWhere = f"AND dmo.code = '{tmpPROD_NBR}' "
        try:
            passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            epa.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS MAIN_WC, \
                            SUM(epa.sumqty) AS PASS_QTY \
                        FROM \
                            INTMP_DB.fact_efa_pass_sum epa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = epa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = epa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = epa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList['denominator']}) \
                            AND epa.mfgdate = '{tmpACCT_DATE}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            epa.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(epa.sumqty) > 0 "
            description, data = self.pSelectAndDescription(passString)
            pData = self._zipDescriptionAndData(description, data)
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            edf.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS MAIN_WC, \
                            edf.deftcode    AS DFCT_CODE, \
                            SUM(edf.sumqty) AS DEFT_QTY \
                        FROM \
                            INTMP_DB.fact_efa_deft_sum edf \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = edf.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = edf.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = edf.oper_id \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList['numerator']}) \
                            AND edf.mfgdate = '{tmpACCT_DATE}' \
                            AND edf.deftcode in (select code from INTMP_DB.codefilter where type = 'DEFT' \
                                and COMPANYCODE = '{tmpCOMPANY_CODE}' and SITE = '{tmpSITE}' and factoryid = '{tmpFACTORY_ID}' ) \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            edf.mfgdate, \
                            dmo.application, \
                            dop.name, \
                            edf.deftcode \
                        HAVING SUM(edf.sumqty) > 0 "
            description, data = self.pSelectAndDescription(deftString)
            dData = self._zipDescriptionAndData(description, data)

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

    def _getEFADatabyDeftFromMongoDB(self, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"] if "PROD_NBR" in self.jsonData else ""
        tmpKPITYPE = self.jsonData["KPITYPE"]
        passAggregate = []
        deftAggregate = []

        OPERList = EFASet["OPERLIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        # pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList['denominator']]},
                "LCM_OWNER": {"$in": LCMOWNER}
            }
        }
        passGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION"
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
                "ACCT_DATE": "$_id.ACCT_DATE",
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
                "ACCT_DATE": 1,
                "APPLICATION": 1
            }
        }

        # deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList['numerator']]},
                "LCM_OWNER": {"$in": LCMOWNER}
            }
        }
        deftlookup1 = {
            "$lookup": {
                "from": "CODEFILTER",
                "as": "CODEFILTER",
                "let": {
                        "dfctCode": "$DFCT_CODE",
                        "cc": "$COMPANY_CODE",
                        "si": "$SITE",
                        "fa": "$FACTORY_ID",
                },
                "pipeline": [{'$match': {'TYPE': 'DEFT', 
                '$expr': {'$and': [{'$eq': ['$$dfctCode', '$CODE']},
                {'$eq': ['$$cc', '$COMPANYCODE']},
                {'$eq': ['$$si', '$SITE']},
                {'$eq': ['$$fa', '$FACTORYID']}]}}}, 
                {'$project': {'DFCT_CODE': '$CODE'}}]
            }
        }
        deftunwind1 = {
            "$unwind": "$CODEFILTER"
        }

        deftGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "DFCT_CODE": "$DFCT_CODE"
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
                "ACCT_DATE": "$_id.ACCT_DATE",
                "APPLICATION": "$_id.APPLICATION",
                "DFCT_CODE": "$_id.DFCT_CODE",
                "DEFT_QTY": "$DEFT_QTY"
            }
        }
        deftSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
                "APPLICATION": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            passMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            deftMatch1["$match"]["APPLICATION"] = tmpAPPLICATION

        if tmpPROD_NBR != '' and tmpKPITYPE == 'PRODEFA':
            passMatch1["$match"]["PROD_NBR"] = tmpPROD_NBR
            deftMatch1["$match"]["PROD_NBR"] = tmpPROD_NBR

        passAggregate.extend([passMatch1, passGroup1, passProject1, passSort])
        deftAggregate.extend(
            [deftMatch1, deftlookup1, deftunwind1, deftGroup1, deftProject1, deftSort])
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

    def _groupEFADatabyDeft(self, dData, pData, OPER):
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
            if d == []:
                oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
                oData["SITE"] = copy.deepcopy(p["SITE"])
                oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
                oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                oData["ACCT_DATE"] = datetime.datetime.strptime(
                    p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
                if "APPLICATION" in p.keys():
                    oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
                else:
                    oData["APPLICATION"] = None
                oData["OPER"] = OPER
                oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
                oData["DFCT_CODE"] = ""
                oData["DEFT_QTY"] = 0.00
                oData["DEFECT_RATE"] = 0.00
                data.append(copy.deepcopy(oData))
                oData = {}
            else:
                for dd in d:
                    oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
                    oData["SITE"] = copy.deepcopy(p["SITE"])
                    oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
                    oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                    oData["ACCT_DATE"] = datetime.datetime.strptime(
                        p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
                    if "APPLICATION" in p.keys():
                        oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
                    else:
                        oData["APPLICATION"] = None
                    oData["OPER"] = OPER
                    oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
                    oData["DFCT_CODE"] = copy.deepcopy(dd["DFCT_CODE"])
                    oData["DEFT_QTY"] = copy.deepcopy(dd["DEFT_QTY"])
                    oData["DEFECT_RATE"] = round(
                        oData["DEFT_QTY"] / oData["PASS_QTY"], 4)
                    data.append(copy.deepcopy(oData))
                    oData = {}
        return data

    def _getEFAData(self, tmpOPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]      
        EFASet= self._getEFASet( tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID, tmpOPER)
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getEFADataFromMongoDB(EFASet)
            else:
                data = self._getEFADataFromOracle(EFASet)
            returnData = {
                "pData": data["pData"],
                "dData": data["dData"]
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

    def _getEFADataFromOracle(self, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        OPERList = EFASet["NAMELIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "
        try:
            passString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            epa.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS MAIN_WC, \
                            SUM(epa.sumqty) AS PASS_QTY \
                        FROM \
                            INTMP_DB.fact_efa_pass_sum epa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = epa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = epa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = epa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList['denominator']}) \
                            AND epa.mfgdate = '{tmpACCT_DATE}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            epa.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(epa.sumqty) > 0 "
            description, data = self.pSelectAndDescription(passString)
            pData = self._zipDescriptionAndData(description, data)
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            edf.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS MAIN_WC, \
                            SUM(edf.sumqty) AS DEFT_QTY \
                        FROM \
                            INTMP_DB.fact_efa_deft_sum edf \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = edf.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = edf.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = edf.oper_id \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList['numerator']}) \
                            AND edf.mfgdate = '{tmpACCT_DATE}' \
                            AND edf.deftcode in (select code from INTMP_DB.codefilter where type = 'DEFT' \
                                    and COMPANYCODE = '{tmpCOMPANY_CODE}' and SITE = '{tmpSITE}' and factoryid = '{tmpFACTORY_ID}' ) \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            edf.mfgdate, \
                            dmo.application, \
                            dop.name \
                        HAVING SUM(edf.sumqty) > 0 "
            description, data = self.pSelectAndDescription(deftString)
            dData = self._zipDescriptionAndData(description, data)

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

    def _getEFADataFromMongoDB(self, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        passAggregate = []
        deftAggregate = []

        OPERList = EFASet["OPERLIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        # pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": LCMOWNER},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList['denominator']]}
            }
        }
        passGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": "$MAIN_WC"
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
                "ACCT_DATE": "$_id.ACCT_DATE",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "PASS_QTY": "$PASS_QTY"
            }
        }
        passSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
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
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": LCMOWNER},
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList['numerator']]}
            }
        }
        deftlookup1 = {
            "$lookup": {
                "from": "CODEFILTER",
                "as": "CODEFILTER",
                "let": {
                        "dfctCode": "$DFCT_CODE",
                        "cc": "$COMPANY_CODE",
                        "si": "$SITE",
                        "fa": "$FACTORY_ID",
                },
                "pipeline": [{'$match': {'TYPE': 'DEFT', 
                '$expr': {'$and': [{'$eq': ['$$dfctCode', '$CODE']},
                {'$eq': ['$$cc', '$COMPANYCODE']},
                {'$eq': ['$$si', '$SITE']},
                {'$eq': ['$$fa', '$FACTORYID']}]}}}, 
                {'$project': {'DFCT_CODE': '$CODE'}}]
            }
        }
        deftunwind1 = {
            "$unwind": "$CODEFILTER"
        }
        deftGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": "$MAIN_WC"
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
                "ACCT_DATE": "$_id.ACCT_DATE",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "DEFT_QTY": "$DEFT_QTY"
            }
        }
        deftSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            passMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            deftMatch1["$match"]["APPLICATION"] = tmpAPPLICATION

        passAggregate.extend([passMatch1, passGroup1, passProject1, passSort])
        deftAggregate.extend(
            [deftMatch1, deftlookup1, deftunwind1, deftGroup1, deftProject1, deftSort])

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

    def _groupEFAData(self, dData, pData):
        deftData = []
        for d in dData:
            deftData.append(d)
        passData = []
        for p in pData:
            passData.append(p)
        data = []
        oData = {}
        for p in passData:
            d = list(filter(lambda d: d["PROD_NBR"] == p["PROD_NBR"]
                            and d["MAIN_WC"] == p["MAIN_WC"], deftData))
            if d == []:
                oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
                oData["SITE"] = copy.deepcopy(p["SITE"])
                oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
                oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                oData["ACCT_DATE"] = datetime.datetime.strptime(
                    p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
                if "APPLICATION" in p.keys():
                    oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
                else:
                    oData["APPLICATION"] = None
                oData["MAIN_WC"] = copy.deepcopy(p["MAIN_WC"])
                oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
                oData["DEFT_QTY"] = 0.00
                oData["DEFECT_RATE"] = 0.00
                data.append(copy.deepcopy(oData))
                oData = {}
            else:
                for dd in d:
                    oData["COMPANY_CODE"] = copy.deepcopy(p["COMPANY_CODE"])
                    oData["SITE"] = copy.deepcopy(p["SITE"])
                    oData["FACTORY_ID"] = copy.deepcopy(p["FACTORY_ID"])
                    oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                    oData["ACCT_DATE"] = datetime.datetime.strptime(
                        p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d')
                    if "APPLICATION" in p.keys():
                        oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
                    else:
                        oData["APPLICATION"] = None
                    oData["MAIN_WC"] = copy.deepcopy(p["MAIN_WC"])
                    oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
                    oData["DEFT_QTY"] = copy.deepcopy(dd["DEFT_QTY"])
                    oData["DEFECT_RATE"] = round(
                        oData["DEFT_QTY"] / oData["PASS_QTY"], 4)
                    data.append(copy.deepcopy(oData))
                    oData = {}
        return data

    def _getProdReasonData(self, Prod, REASON, tmpOPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]      
        EFASet = self._getEFASet( tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID, tmpOPER)
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getProdReasonDataFromMongoDB(
                    Prod, REASON, EFASet)
            else:
                data = self._getProdReasonDataFromOracle(
                    Prod, REASON, EFASet)

            returnData = data
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

    def _getProdReasonDataFromOracle(self, Prod, REASON, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        OPERList = EFASet["NAMELIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        _REASON_ARRAY_LIST = ""
        for x in REASON:
            _REASON_ARRAY_LIST = _REASON_ARRAY_LIST + f"'{x}',"
        if _REASON_ARRAY_LIST != "":
            _REASON_ARRAY_LIST = _REASON_ARRAY_LIST[:-1]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "
        try:
            reasonString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            ers.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS MAIN_WC, \
                            ers.reasoncode     AS DFCT_REASON,\
                            SUM(ers.sumqty) AS REASON_QTY \
                        FROM \
                            INTMP_DB.fact_efa_reason_sum ers \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = ers.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = ers.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = ers.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList['numerator']}) \
                            AND ers.mfgdate = '{tmpACCT_DATE}' \
                            AND ers.reasoncode in ({_REASON_ARRAY_LIST}) \
                            AND dmo.code  = '{Prod}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            ers.mfgdate, \
                            dmo.application, \
                            dop.name, \
                            ers.reasoncode \
                        HAVING SUM(ers.sumqty) > 0 "
            description, data = self.pSelectAndDescription(reasonString)
            rData = self._zipDescriptionAndData(description, data)

            returnData = rData
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

    def _getProdReasonDataFromMongoDB(self, Prod, REASON, EFASet):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        OPERList = EFASet["OPERLIST"]
        LCMOWNER = EFASet["LCMOWNER"]

        reasonAggregate = []
        # reason
        reasonMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList['numerator']]},
                "PROD_NBR": Prod,
                "DFCT_REASON": {"$in": REASON},
                "LCM_OWNER": {"$in": LCMOWNER}
            }
        }
        reasonGroup1 = {
            "$group": {
                "_id": {
                    "COMPANY_CODE": "$COMPANY_CODE",
                    "SITE": "$SITE",
                    "FACTORY_ID": "$FACTORY_ID",
                    "PROD_NBR": "$PROD_NBR",
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": "$MAIN_WC",
                    "DFCT_REASON": "$DFCT_REASON"
                },
                "REASON_QTY": {
                    "$sum": {"$toInt": "$QTY"}
                }
            }
        }
        reasonProject1 = {
            "$project": {
                "_id": 0,
                "COMPANY_CODE": "$_id.COMPANY_CODE",
                "SITE": "$_id.SITE",
                "FACTORY_ID": "$_id.FACTORY_ID",
                "PROD_NBR": "$_id.PROD_NBR",
                "ACCT_DATE": "$_id.ACCT_DATE",
                "APPLICATION": "$_id.APPLICATION",
                "MAIN_WC": "$_id.MAIN_WC",
                "DFCT_REASON": "$_id.DFCT_REASON",
                "REASON_QTY": "$PASS_QTY"
            }
        }
        reasonSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

        reasonAggregate.extend(
            [reasonMatch1, reasonGroup1, reasonProject1, reasonSort])
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("reasonHisAndCurrent")
            rData = self.aggregate(reasonAggregate)
            self.closeMongoConncetion()
            returnData = rData
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

    def _getEFA_impReason(self):
        try:
            tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
            tmpSITE = self.jsonData["SITE"]
            tmpFACTORY_ID = self.jsonData["FACTORY_ID"] 
            returnData = []
            sString = f"select code as REASON_CODE from INTMP_DB.codefilter where type = 'REASON'  \
                        and COMPANYCODE = '{tmpCOMPANY_CODE}' and SITE = '{tmpSITE}' and factoryid = '{tmpFACTORY_ID}' "
            description , data = self.pSelectAndDescription(sString)            
            deftData = self._zipDescriptionAndData(description, data)            
            for x in deftData:                  
                returnData.append(x["REASON_CODE"])
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

    def _calEFAData(self, EFAData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["EFA"]["limit"] if tmpSITE == "TN" else {
        }

        yellowList = self._getEFA_impReason()

        PRODList = []
        for x in EFAData:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})

        GREEN_VALUE = 0
        YELLOW_VALUE = 0
        RED_VALUE = 0

        GREENL = []
        YELLOWL = []
        REDL = []

        for prod in PRODList:
            d1 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], EFAData))
            targrt = 0.003
            targrtQTY = 3000
            if prod["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[prod["APPLICATION"]]["target"]
                targrtQTY = getLimitData[prod["APPLICATION"]]["qytlim"]

            rCheck = False
            yCheck = False
            checkTargrt = list(
                filter(lambda d: d["DEFECT_RATE"] >= targrt, d1))
            if len(checkTargrt) > 0:
                oper1600 = list(
                    filter(lambda d: d["MAIN_WC"] in ["1600", "CKEN"], d1))
                if len(oper1600) > 0:
                    checkRed = list(
                        filter(lambda d: d["DEFECT_RATE"] >= targrt, oper1600))
                    checkQTY = list(
                        filter(lambda d: d["PASS_QTY"] >= targrtQTY, checkRed))
                    if len(checkRed) != 0:
                        if len(checkQTY) != 0:
                            RED_VALUE += 1
                            REDL.append(prod)
                            rCheck = False
            else:
                rCheck = True

            _ReasonData = self._getProdReasonData(
                prod["PROD_NBR"], yellowList, "CKEN")
            checkYellow = []
            for x in _ReasonData:
                checkYellow.append(x)
            if len(checkYellow) != 0:
                YELLOW_VALUE += 1
                YELLOWL.append(prod)
                yCheck = False
            else:
                yCheck = True

            if rCheck == True and yCheck == True:
                GREEN_VALUE += 1
                GREENL.append(prod)

        returnData = {
            "CLASS_TYPE": "EFA",
            "GREEN_VALUE": GREEN_VALUE,
            "YELLOW_VALUE": YELLOW_VALUE,
            "RED_VALUE": RED_VALUE
        }

        return returnData

    def _calPRODEFAData(self, EFAData, OPER):
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["EFA"]["limit"] if tmpSITE == "TN" else {
        }

        yellowList = self._getEFA_impReason()

        PRODList = []
        for x in EFAData:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})

        DATASERIES = []
        targrt = 0.003
        targrtQTY = 3000
        for prod in PRODList:
            d1 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], EFAData))
            if prod["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[prod["APPLICATION"]]["target"]
                targrtQTY = getLimitData[prod["APPLICATION"]]["qytlim"]

            DEFECT_RATE = 0
            sumPASSQTY = 0
            sumDEFTQTY = 0
            if len(d1) > 0:
                sdCheck = False  # 單項不良率超標
                checkTargrtAndQyt = list(
                    filter(lambda d: d["PASS_QTY"] >= targrtQTY and d["DEFECT_RATE"] >= targrt, d1))
                sdCheck = False if len(checkTargrtAndQyt) > 0 else True

                tdCheck = False  # 總不良率超標
                sumPASSQTY = d1[0]["PASS_QTY"]
                for x in d1:
                    sumDEFTQTY += x["DEFT_QTY"]
                DEFECT_RATE = round(
                    sumDEFTQTY / sumPASSQTY, 4) if sumPASSQTY != 0 and sumDEFTQTY != 0 else 0
                tdCheck = False if sumPASSQTY >= targrtQTY and DEFECT_RATE >= targrt else True

                rCheck = False  # 潛在不良(REASON CODE)
                _ReasonData = self._getProdReasonData(
                    prod["PROD_NBR"], yellowList, OPER)
                checkYellow = []
                for x in _ReasonData:
                    checkYellow.append(x)
                rCheck = False if len(checkYellow) > 0 else True

                # SYMBOL
                SYMBOL = ""
                if sdCheck == False and rCheck == False:
                    SYMBOL = "diamond"
                elif sdCheck == True and rCheck == False:
                    SYMBOL = "triangle-down"
                elif sdCheck == False and rCheck == True:
                    SYMBOL = "triangle"
                else:
                    SYMBOL = "circle"
                # COLOR
                COLOR = ""
                if tdCheck == True and sdCheck == True and rCheck == True:
                    COLOR = "#2f7ed8"
                elif tdCheck == True and sdCheck == True and rCheck == False:
                    COLOR = "#ffd166"
                else:
                    COLOR = "#ef476f"

                DATASERIES.append({
                    "APPLICATION": prod["APPLICATION"],
                    "PROD_NBR": prod["PROD_NBR"],
                    "YIELD": DEFECT_RATE,
                    "DEFECT_RATE": DEFECT_RATE,
                    "COLOR": COLOR,
                    "SYMBOL": SYMBOL,
                    "QTY": sumPASSQTY,
                    "DEFTQTY": sumDEFTQTY,
                    "SHOWLABEL": True if COLOR != "#2f7ed8" else False
                })

        # red ef476f
        # yellow ffd166
        # green 06d6a0
        # blue #2f7ed8
        # midGreen 073b4c

        checkRankType = 0
        for x in DATASERIES:
            if x["QTY"] >= 3000 and  x["YIELD"] >= 0.003:
                x["QUADRANT"] = 1
                checkRankType = 1
            else:
                x["QUADRANT"] = 0
        
        # 因為使用 operator.itemgetter 方法 排序順序要反過來執行
        # 不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        if checkRankType == 0:
            DATASERIES.sort(key=operator.itemgetter("DEFTQTY"), reverse=True)   
        elif checkRankType == 1:
            DATASERIES.sort(key=operator.itemgetter("DEFTQTY"), reverse=True)  
            DATASERIES.sort(key=operator.itemgetter("YIELD"), reverse=True)        
            DATASERIES.sort(key=operator.itemgetter("QUADRANT"), reverse=True)

        rank = 1        
        length = len(DATASERIES)
        for x in range(length):
            DATASERIES[x]["RANK"] = rank
            rank += 1

        returnData = {
            "XLIMIT": targrtQTY,
            "YLIMIT": targrt,
            "OPER": OPER,
            "ACCT_DATE": tmpACCT_DATE,
            "DATASERIES": DATASERIES
        }

        return returnData

    def _calPRODEFAListData(self, EFAData, OPER):
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpSITE = self.jsonData["SITE"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["EFA"]["limit"] if tmpSITE == "TN" else {
        }

        yellowList = self._getEFA_impReason()

        PRODList = []
        for x in EFAData:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})

        DATASERIES = []
        targrt = 0.003
        targrtQTY = 3000
        for prod in PRODList:
            d1 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], EFAData))
            if prod["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[prod["APPLICATION"]]["target"]
                targrtQTY = getLimitData[prod["APPLICATION"]]["qytlim"]

            DEFECT_RATE = 0
            sumPASSQTY = 0
            sumDEFTQTY = 0
            if len(d1) > 0:
                sdCheck = False  # 單項不良率超標
                checkTargrtAndQyt = list(
                    filter(lambda d: d["PASS_QTY"] >= targrtQTY and d["DEFECT_RATE"] >= targrt, d1))
                sdCheck = False if len(checkTargrtAndQyt) > 0 else True

                tdCheck = False  # 總不良率超標
                sumPASSQTY = d1[0]["PASS_QTY"]
                for x in d1:
                    sumDEFTQTY += x["DEFT_QTY"]
                DEFECT_RATE = round(
                    sumDEFTQTY / sumPASSQTY, 4) if sumPASSQTY != 0 and sumDEFTQTY != 0 else 0
                tdCheck = False if sumPASSQTY >= targrtQTY and DEFECT_RATE >= targrt else True

                rCheck = False  # 潛在不良(REASON CODE)
                _ReasonData = self._getProdReasonData(
                    prod["PROD_NBR"], yellowList, OPER)
                checkYellow = []
                for x in _ReasonData:
                    checkYellow.append(x)
                rCheck = False if len(checkYellow) > 0 else True

                # SYMBOL
                SYMBOL = ""
                if sdCheck == False and rCheck == False:
                    SYMBOL = "diamond"
                elif sdCheck == True and rCheck == False:
                    SYMBOL = "triangle-down"
                elif sdCheck == False and rCheck == True:
                    SYMBOL = "triangle"
                else:
                    SYMBOL = "circle"
                # COLOR
                COLOR = ""
                if tdCheck == True and sdCheck == True and rCheck == True:
                    COLOR = "#06d6a0"
                elif tdCheck == True and sdCheck == True and rCheck == False:
                    COLOR = "#ffd166"
                else:
                    COLOR = "#ef476f"

                DATASERIES.append({
                    "APPLICATION": prod["APPLICATION"],
                    "PROD_NBR": prod["PROD_NBR"],
                    "YIELD": DEFECT_RATE,
                    "DEFECT_RATE": DEFECT_RATE,
                    "COLOR": COLOR,
                    "SYMBOL": SYMBOL,
                    "QTY": sumPASSQTY
                })

        # red ef476f
        # yellow ffd166
        # green 06d6a0
        # blue 118AB2
        # midGreen 073b4c
        # 因為使用 operator.itemgetter 方法 排序順序要反過來執行
        # 不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key=operator.itemgetter("QTY"), reverse=True)
        DATASERIES.sort(key=operator.itemgetter("YIELD"), reverse=True)

        length = len(DATASERIES)
        rank = 1
        for x in range(length):
            DATASERIES[x]["RANK"] = rank
            rank += 1

        selectlistData = []
        selectlistData.append({"value": "", "text": "ALL"})
        for x in DATASERIES:
            _pass = f'{round(int(x["QTY"])/1000,1)}k' if int(x["QTY"]
                                                             ) > 99 else f'{int(x["QTY"])}'
            selectlistData.append(
                {
                    "value": x["PROD_NBR"],
                    "text": f'({x["RANK"]}){x["PROD_NBR"]}-DEFT RATE:{round(decimal.Decimal(x["YIELD"])*100,2)}%-'
                    f'Pass:{_pass}'
                }
            )

        returnData = {
            "TITLE": "快選機種",
            "SELECTLIST": selectlistData
        }

        return returnData

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
