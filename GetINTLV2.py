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
from decimal import Decimal, ROUND_HALF_UP

class INTLV2(BaseType):
    def __init__(self, jsonData, _db_pool):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        #M011 => MOD1
        #J001 => MOD2
        #J003 => MOD3
        self.EFAOPERDATA = {
                "BONDING":{"OPER": [1300,1301]},
                "LAM":{"OPER": [1340,1370]},
                "AAFC":{"OPER": [1419,1420]},
                "TPI":{"OPER": [1510]},
                "OTPC":{"OPER": [1590]},
                "CKEN":{"OPER": [1600]}   
            }  
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
            tmpPROD_NBR = self.jsonData["PROD_NBR"]
            tmpOPER = self.jsonData["OPER"] if "OPER" in self.jsonData else "CKEN"              
            tmpCHECKCODE = self.jsonData["CHECKCODE"] if "CHECKCODE" in self.jsonData else ""
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
            
            if tmpKPITYPE == "FPYLV2PIE":
                expirTimeKey = tmpFACTORY_ID + '_PASS'

                PCBIData = self._getFPYLV2PIEData("PCBI", tmpPROD_NBR)
                PCBIResult = self._groupFPYLV2PIEOPER(PCBIData["dData"])
                LAMData = self._getFPYLV2PIEData("LAM", tmpPROD_NBR)
                LAMResult = self._groupFPYLV2PIEOPER(LAMData["dData"])
                AAFCData = self._getFPYLV2PIEData("AAFC", tmpPROD_NBR)
                AAFCResult = self._groupFPYLV2PIEOPER(AAFCData["dData"])
                CKENData = self._getFPYLV2PIEData("CKEN", tmpPROD_NBR)
                CKENResult = self._groupFPYLV2PIEOPER(CKENData["dData"])
                DKENData = self._getFPYLV2PIEData("DKEN", tmpPROD_NBR)
                DKENResult = self._groupFPYLV2PIEOPER(DKENData["dData"])
                
                returnData = self._calFPYLV2PIEData(PCBIResult,LAMResult,AAFCResult,CKENResult,DKENResult)
                
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "FPYLV2HISTO":
                expirTimeKey = tmpFACTORY_ID + '_PASS'

                _PCBIData = self._getFPYLV2PIEData("PCBI", tmpPROD_NBR)                
                PCBIResult = self._groupPassDeftByPRODandOPER(
                    _PCBIData["dData"], _PCBIData["pData"])
                PCBIData = self._calFPYLV2HISTObyOPER(PCBIResult)
                _LAMData = self._getFPYLV2PIEData("LAM", tmpPROD_NBR) 
                LAMResult = self._groupPassDeftByPRODandOPER(
                    _LAMData["dData"], _LAMData["pData"])
                LAMData = self._calFPYLV2HISTObyOPER(LAMResult)
                _AAFCData = self._getFPYLV2PIEData("AAFC", tmpPROD_NBR)
                AAFCResult = self._groupPassDeftByPRODandOPER(
                    _AAFCData["dData"], _AAFCData["pData"])
                AAFCData = self._calFPYLV2HISTObyOPER(AAFCResult)
                _CKENData = self._getFPYLV2PIEData("CKEN", tmpPROD_NBR)
                CKENResult = self._groupPassDeftByPRODandOPER(
                    _CKENData["dData"], _CKENData["pData"])
                CKENData = self._calFPYLV2HISTObyOPER(CKENResult)
                _DKENData = self._getFPYLV2PIEData("DKEN", tmpPROD_NBR)
                DKENResult = self._groupPassDeftByPRODandOPER(
                    _DKENData["dData"], _DKENData["pData"])
                DKENData = self._calFPYLV2HISTObyOPER(DKENResult)

                tempData = self._groupFPYLV2HISTOOPER(PCBIData, LAMData, AAFCData, CKENData, DKENData)

                returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "DATASERIES": tempData
                }
                
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}
            
            #二階 MSHIP PIE API
            elif tmpKPITYPE == "MSHIPLV2PIE":
                expirTimeKey = tmpFACTORY_ID + '_SCRP'

                formerfabData = self._getMSHIPSCRAPData("formerfab",tmpPROD_NBR)
                formerfabResult = self._groupMSHIPLV2PIE(formerfabData["scrapData"])
                fabData = self._getMSHIPSCRAPData("fab",tmpPROD_NBR)
                fabResult = self._groupMSHIPLV2PIE(fabData["scrapData"])
                incomingData = self._getMSHIPSCRAPData("incoming",tmpPROD_NBR)
                fincomingResult = self._groupMSHIPLV2PIE(incomingData["scrapData"])

                returnData = self._calMSHIPLV2PIE(formerfabResult,fabResult,fincomingResult)
                
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            #二階 MSHIP PIE API
            elif tmpKPITYPE == "EFALV2_3":    
                OPERList = []
                if tmpOPER == "ALL":
                    for key, value in self.EFAOPERDATA.items():
                        OPERList.extend(value.get("OPER"))
                else:
                    OPERList.extend(self.EFAOPERDATA[tmpOPER]["OPER"])

                data = self._getEFALV2_3_Data(tmpOPER, tmpPROD_NBR, tmpCHECKCODE)

                DATASERIES = self._calEFALV2_3_Data(data["rData"], data["pData"])

                returnData = returnData = {                    
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
                
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            #二階 MSHIP PIE API
            elif tmpKPITYPE == "EFALV2_21":    
                expirTimeKey = tmpFACTORY_ID + '_DEFT'

                OPERList = []
                if tmpOPER == "ALL":
                    for key, value in self.EFAOPERDATA.items():
                        OPERList.extend(value.get("OPER"))
                else:
                    OPERList.extend(self.EFAOPERDATA[tmpOPER]["OPER"])

                data = self._getEFALV2_21_Data(OPERList)

                DATASERIES = self._calEFALV2_21_Data(data["dData"], data["pData"])

                returnData = returnData = {                    
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
                
                """
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), self.getKeyExpirTime(expirTimeKey))
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), 60)
                """
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "EFALV2_WIP_1":    
                expirTimeKey = tmpFACTORY_ID + '_WIP'

                data = self._getEFALV2_WIP_1_Data()

                DATASERIES = self._calEFALV2_WIP_1_Data(data["faWip"], data["prodNbrWip"])

                returnData = returnData = {                    
                    "KPITYPE": tmpKPITYPE,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "FADATA": DATASERIES["faData"],
                    "PRODDATA1": DATASERIES["prodData1"],
                    "PRODDATA2": DATASERIES["prodData2"]
                }
                
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpKPITYPE == "EFALV2_WIP_2":    
                expirTimeKey = tmpFACTORY_ID + '_WIP'

                data = self._getEFALV2_WIP_2_Data()

                DATASERIES = self._calEFALV2_WIP_2_Data(data)

                returnData = returnData = {                    
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

    def _getFPYLV2PIEDataFromOracle(self, OPER, PROD_NBR):
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
            description , data = self.pSelectAndDescription(passString)            
            pData = self._zipDescriptionAndData(description, data)  
            deftString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            fdf.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            dop.name           AS OPER, \
                            ddf.DEFTCODE as DFCT_CODE, \
                            ddf.DEFTCODE_DESC as ERRC_DESCR, \
                            SUM(fdf.sumqty) AS DEFTSUMQTY \
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
                            AND fdf.mfgdate = '{tmpACCT_DATE}' \
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
            description , data = self.pSelectAndDescription(deftString)            
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


    def _getFPYLV2PIEDataFromMongo(self, OPER, PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]

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
                "ACCT_DATE": tmpACCT_DATE,
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
                "ACCT_DATE": 1,
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
                "ACCT_DATE": tmpACCT_DATE,
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
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": {"$toInt": "$MAIN_WC"},                 
                    "DFCT_CODE" : "$DFCT_CODE",
                    "ERRC_DESCR" : "$ERRC_DESCR"
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
                "DFCT_CODE" : "$_id.DFCT_CODE",                
                "ERRC_DESCR" : "$_id.ERRC_DESCR",
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
                    "APPLICATION": "$APPLICATION",
                    "DFCT_CODE" : "$DFCT_CODE",
                    "ERRC_DESCR" : "$ERRC_DESCR"
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
                "DFCT_CODE" : "$_id.DFCT_CODE",
                "ERRC_DESCR" : "$_id.ERRC_DESCR",
                "DEFTSUMQTY": "$DEFTSUMQTY"
            }
        }
        deftAdd = {
                "$addFields": {
                    "OPER": OPER
                }
            }
        deftSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
                "APPLICATION": 1,
                "DFCT_CODE" : 1,
                "ERRC_DESCR" : 1
            }
        }
        
        passAggregate.extend([passMatch1, passGroup1, passProject1, passGroup2, passProject2, passAdd, passSort])        
        deftAggregate.extend([deftMatch1, deftGroup1, deftProject1,deftGroup2, deftProject2, deftAdd, deftSort])
        
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

    def _getFPYLV2PIEData(self, OPER, PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getFPYLV2PIEDataFromMongo(OPER,PROD_NBR)
            else:
                data = self._getFPYLV2PIEDataFromOracle(OPER,PROD_NBR)
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

    def _groupFPYLV2PIEOPER(self, dData): 
        deftData = []            
        for d in dData:       
            deftData.append(d) 

        if deftData != []:  
            deftSUM = 0
            for p in deftData:            
                deftSUM += p["DEFTSUMQTY"]
            p = deftData[0]
            data = {
                "COMPANY_CODE": p["COMPANY_CODE"],
                "SITE": p["SITE"],
                "FACTORY_ID": p["FACTORY_ID"],
                "PROD_NBR": p["PROD_NBR"],
                "ACCT_DATE": datetime.datetime.strptime(p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d'),
                "APPLICATION": p["APPLICATION"] if "APPLICATION" in p.keys() else None,
                "OPER": p["OPER"],
                "DEFTSUMQTY" : deftSUM
            }
            return data
        else: 
            return None

    def _calFPYLV2PIEData(self, PCBI, LAM, AAFC, CKEN, DKEN):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]        
        tmpAPPLICATION =self.jsonData["APPLICATION"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        TotalDEFTSUMQTY = 0
        tempData = []
        tempData.extend([PCBI,LAM,AAFC,CKEN,DKEN])
        for x in tempData:
            if x != None:
                tmpAPPLICATION = x["APPLICATION"]
                TotalDEFTSUMQTY += x["DEFTSUMQTY"]
        
        colorMap = {
            "PCBI": {"colorName": "red", "HEX":"#ef476f"},
            "LAM": {"colorName": "yellow", "HEX":"#ffd166"},
            "AAFC": {"colorName": "green", "HEX":"#06d6a0"},
            "CKEN": {"colorName": "blue", "HEX":"#118AB2"},
            "DKEN": {"colorName": "midGreen", "HEX":"#073b4c"}
        }

        DATASERIES = []
        for x in tempData:
            if x != None:
                DATASERIES.append({
                    "OPER": x["OPER"],
                    "VALUE": round(x["DEFTSUMQTY"] / TotalDEFTSUMQTY , 2) if TotalDEFTSUMQTY !=0 else 0,
                    "COLOR": colorMap[x["OPER"]]["HEX"] if x["OPER"] in colorMap.keys() else None,
                    "SELECT": None,
                    "SLICED": None,
                    "DEFTSUMQTY": x["DEFTSUMQTY"],
                    "PROD_NBR": tmpPROD_NBR
                })

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

        return returnData

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
                oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                oData["PASSSUMQTY"] = copy.deepcopy(p["PASSSUMQTY"])
                if d == []:
                    oData["DEFTSUMQTY"] = 0
                else:
                    oData["DEFTSUMQTY"] = copy.deepcopy(d["DEFTSUMQTY"])
                if oData["DEFTSUMQTY"] == 0:
                    oData["DEFECT_RATE"] = 0
                else:
                    if(oData["PASSSUMQTY"] != 0):
                        ds = Decimal(oData["DEFTSUMQTY"])
                        ps = Decimal(oData["PASSSUMQTY"])
                        oData["DEFECT_RATE"] =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    else:
                        oData["DEFECT_RATE"] = 1
                oData["FPY_RATE"] = round(1 - oData["DEFECT_RATE"], 4)
                if oData["DEFTSUMQTY"] < oData["PASSSUMQTY"] and oData["FPY_RATE"] > 0:
                    data.append(copy.deepcopy(oData))
                oData = {}
        return data

    def _calFPYLV2HISTObyOPER(self, tempData):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        allDFCTCount = {}
        for x in tempData:     
            if x["DFCT_CODE"] in allDFCTCount.keys():
                allDFCTCount[x["DFCT_CODE"]] += x["DEFECT_RATE"]
            else:
                allDFCTCount[x["DFCT_CODE"]] = x["DEFECT_RATE"]
        top10 = dict(sorted(allDFCTCount.items(),key=lambda item:item[1],reverse=True) [:10])

        operMap = {"PCBI":0,"LAM":1,"AAFC":2,"CKEN":3,"DKEN":4}

        DATASERIES = []
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

            d = list(filter(lambda d: d["DFCT_CODE"] == cDFct and d["OPER"] == x["OPER"], DATASERIES))
            if d == []:
                DATASERIES.append({
                        "OPER": x["OPER"],
                        "XVALUE": operMap.get(x["OPER"], None),
                        "YVALUE": x["DEFECT_RATE"]*100,
                        "RANK": rank,
                        "DFCT_CODE" : cDFct,
                        "ERRC_DESCR" : cERRC,                        
                        "PROD_NBR": tmpPROD_NBR,
                        "DeftSUM": x["DEFTSUMQTY"],
                        "PassSUM": x["PASSSUMQTY"],
                        "DEFECT_RATE": x["DEFECT_RATE"]*100
                    })
            else:
                for cx in DATASERIES:
                    if cx["OPER"] == x["OPER"] and cx["DFCT_CODE"] == cDFct :                       
                       cx["DeftSUM"] += x["DEFTSUMQTY"]
                       ds = Decimal(cx["DeftSUM"])
                       ps = Decimal(cx["PassSUM"])
                       dr =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                       cx["DEFECT_RATE"] = dr*100
                       cx["YVALUE"] =  dr*100
                       
        #因為使用 operator.itemgetter 方法 排序順序要反過來執行
        #不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key = operator.itemgetter("RANK"), reverse = True)      
        
        returnData = DATASERIES

        return returnData

    def _groupFPYLV2HISTOOPER(self, PCBI, LAM, AAFC, CKEN, DKEN): 
        deftData = []            
        for d in PCBI:       
            deftData.append(d) 
        for d in LAM:       
            deftData.append(d) 
        for d in AAFC:       
            deftData.append(d) 
        for d in CKEN:       
            deftData.append(d) 
        for d in DKEN:       
            deftData.append(d) 

        deftData.sort(key = operator.itemgetter("XVALUE", "XVALUE"), reverse = True)
        deftData.sort(key = operator.itemgetter("RANK", "RANK"), reverse = True)

        return deftData

    def _getMSHIPSCRAPData(self,type,PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        """
        (1)  前廠責：USL、TX LCD、FABX
        (2)  廠責：MFG、INT、EQP、ER 
        (3)  來料責：SQE
        """       

        try:
            data = {}
            if tmpSITE == "TN":
                data =  self._getMSHIPSCRAPDataFromMongoDB(type,PROD_NBR)
            else:
                data = self._getMSHIPSCRAPDataFromOracle(type,PROD_NBR)

            returnData = {
                "scrapData": data["scrapData"]
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

    def _getMSHIPSCRAPDataFromOracle(self,type,PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        mshipDATA = {
            "formerfab":{"in":"usl|lcd|fab","name": "前廠責"},
            "fab":{"in":"mfg|int|eqp|er","name": "廠責"},
            "incoming":{"in":"sqe","name": "SQE來料責"},
        }
        getFabData = mshipDATA[type]["name"]

        applicatiionWhere = ""
        if tmpAPPLICATION != "ALL":
            applicatiionWhere = f"AND dmo.application = '{tmpAPPLICATION}' "        
        try:
            scrapString = f"SELECT \
                            dlo.company_code   AS company_code, \
                            dlo.site_code      AS site, \
                            dlo.factory_code   AS factory_id, \
                            dmo.code           AS prod_nbr, \
                            '{getFabData}'           AS RESP_OWNER, \
                            '{type}'          AS RESP_OWNER_E, \
                            msc.mfgdate        AS acct_date, \
                            dmo.application    AS APPLICATION, \
                            SUM(msc.sumqty) AS TOBESCRAP_SUMQTY \
                        FROM \
                            INTMP_DB.fact_mship_scrap_sum msc \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = msc.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = msc.model_id \
                            LEFT JOIN INTMP_DB.dime_respcode drc ON drc.respcode = msc.respcode \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND msc.mfgdate = '{tmpACCT_DATE}' \
                            and dmo.code = '{PROD_NBR}' \
                            and drc.respcode = '{getFabData}' \
                            {applicatiionWhere} \
                        GROUP BY \
                            dlo.company_code, \
                            dlo.site_code, \
                            dlo.factory_code, \
                            dmo.code, \
                            msc.mfgdate, \
                            dmo.application \
                        HAVING SUM(msc.sumqty) >= 0 "
            description , data = self.pSelectAndDescription(scrapString)            
            scrapData = self._zipDescriptionAndData(description, data)

            returnData = {
                "scrapData": scrapData
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

    def _getMSHIPSCRAPDataFromMongoDB(self,type,PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
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
                "ACCT_DATE": tmpACCT_DATE,
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
        scrapAdd = {
            "$addFields": {
                "RESP_OWNER": getFabData["name"],
                "RESP_OWNER_E": type
            }
        }
        scrapSort = {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "ACCT_DATE": 1,
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

    def _groupMSHIPLV2PIE(self, dData): 
        deftData = []            
        for d in dData:       
            deftData.append(d) 

        if deftData != []:  
            TOBESCRAP_SUMQTY = 0
            for p in deftData:            
                TOBESCRAP_SUMQTY += p["TOBESCRAP_SUMQTY"]
            p = deftData[0]
            data = {
                "COMPANY_CODE": p["COMPANY_CODE"],
                "SITE": p["SITE"],
                "FACTORY_ID": p["FACTORY_ID"],
                "PROD_NBR": p["PROD_NBR"],
                "ACCT_DATE": datetime.datetime.strptime(p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d'),
                "APPLICATION": p["APPLICATION"] if "APPLICATION" in p.keys() else None,                
                "TOBESCRAP_SUMQTY" : TOBESCRAP_SUMQTY,
                "RESP_OWNER_E" : p["RESP_OWNER_E"],
                "RESP_OWNER": p["RESP_OWNER"]
            }
            return data
        else: 
            return None

    def _calMSHIPLV2PIE(self, formerfab, fab, incoming):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]        
        tmpAPPLICATION =self.jsonData["APPLICATION"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        TotalSCRAPSUMQty = 0
        tempData = []
        tempData.extend([formerfab,fab,incoming])
        for x in tempData:
            if x != None:
                tmpAPPLICATION = x["APPLICATION"]
                TotalSCRAPSUMQty += x["TOBESCRAP_SUMQTY"]
        
        colorMap = {
            "formerfab": {"colorName": "red", "HEX":"#ef476f"},
            "fab": {"colorName": "yellow", "HEX":"#ffd166"},
            "incoming": {"colorName": "green", "HEX":"#06d6a0"}
        }

        DATASERIES = []
        for x in tempData:
            if x != None:
                DATASERIES.append({
                    "RESP_OWNER": x["RESP_OWNER"],
                    "RESP_OWNER_E": x["RESP_OWNER_E"],
                    "VALUE": round(x["TOBESCRAP_SUMQTY"] / TotalSCRAPSUMQty , 2) if TotalSCRAPSUMQty !=0 else 0,
                    "COLOR": colorMap[x["RESP_OWNER_E"]]["HEX"] if x["RESP_OWNER_E"] in colorMap.keys() else None,
                    "SELECT": None,
                    "SLICED": None,
                    "TOBESCRAP_SUMQTY": x["TOBESCRAP_SUMQTY"],
                    "PROD_NBR": tmpPROD_NBR
                })

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

        return returnData

    def _DecimaltoFloat(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)

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

    def _getEFALV2_3_Data(self, tmpOPER, PROD_NBR, CHECKCODE):
        tmpSITE = self.jsonData["SITE"] 
        OPERCODEList = []   
        OPERNAMEList = ""  
        if tmpOPER == "ALL":
            for key, value in self.EFAOPERDATA.items():
                OPERCODEList.extend(value.get("OPER"))
            for key, value in self.EFAOPERDATA.items():
                OPERNAMEList = OPERNAMEList + f"'{key}',"
        else:
            OPERCODEList.extend(self.EFAOPERDATA[tmpOPER]["OPER"])
            OPERNAMEList = f"'{tmpOPER}',"        
        if OPERNAMEList != "":
            OPERNAMEList = OPERNAMEList[:-1]

        try:
            data = {}
            if tmpSITE == "TN":
                data = self._getEFALV2_3_DataFromMongoDB(OPERCODEList, PROD_NBR, CHECKCODE)
            else:
                data = self._getEFALV2_3_DataFromOracle(OPERNAMEList, PROD_NBR, CHECKCODE)
            returnData = {
                "pData": data["pData"],
                "rData": data["rData"]
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
    
    def _getEFALV2_3_DataFromOracle(self, OPERList, PROD_NBR, CHECKCODE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        whereString = ""
        if tmpAPPLICATION != "ALL":
            whereString += f" AND dmo.application = '{tmpAPPLICATION}' "
        if PROD_NBR != '':
            whereString += f" AND dmo.code = '{PROD_NBR}' "
        
        try:
            passString = f"SELECT \
                            dmo.code        AS PROD_NBR, \
                            SUM(epa.sumqty) AS PASSQTY \
                        FROM \
                            INTMP_DB.fact_efa_pass_sum epa \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = epa.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = epa.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = epa.oper_id \
                        WHERE \
                            dlo.company_code = '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList}) \
                            AND epa.mfgdate = '{tmpACCT_DATE}' \
                            {whereString} \
                        GROUP BY \
                            dmo.code \
                        HAVING SUM(epa.sumqty) > 0 "
            description , data = self.pSelectAndDescription(passString)            
            pData = self._zipDescriptionAndData(description, data)

            if CHECKCODE != '':
                whereString += f" AND ers.deftcode = '{CHECKCODE}' "
            reasonString = f"SELECT \
                            ers.reasoncode     AS DFCT_REASON, \
                            drc.reasoncode_desc   AS REASON_DESC, \
                            SUM(ers.sumqty) AS REASONQTY \
                        FROM \
                            INTMP_DB.fact_efa_reason_sum ers \
                            LEFT JOIN INTMP_DB.dime_local dlo ON dlo.local_id = ers.local_id \
                            LEFT JOIN INTMP_DB.dime_model dmo ON dmo.model_id = ers.model_id \
                            LEFT JOIN INTMP_DB.dime_oper dop ON dop.oper_id = ers.oper_id \
                            LEFT JOIN INTMP_DB.dime_reasoncode drc ON drc.reasoncode = ers.reasoncode \
                        WHERE \
                            dlo.company_code =  '{tmpCOMPANY_CODE}' \
                            AND dlo.site_code = '{tmpSITE}' \
                            AND dlo.factory_code = '{tmpFACTORY_ID}' \
                            AND dop.name in ({OPERList}) \
                            AND ers.mfgdate = '{tmpACCT_DATE}' \
                            {whereString} \
                        GROUP BY \
                            ers.reasoncode, \
                            drc.reasoncode_desc \
                        HAVING SUM(ers.sumqty) > 0 "
            description , data = self.pSelectAndDescription(reasonString)            
            rData = self._zipDescriptionAndData(description, data)  

            returnData = {
                "pData": pData,
                "rData": rData
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

    def _getEFALV2_3_DataFromMongoDB(self, OPERList, PROD_NBR, CHECKCODE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        passAggregate =[
                  {
                    "$match": {
                      "COMPANY_CODE":  tmpCOMPANY_CODE,
                      "SITE": tmpSITE,
                      "FACTORY_ID": tmpFACTORY_ID,
                      "ACCT_DATE": tmpACCT_DATE,
                      "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList]}
                    }
                  },
                  {
                    "$group": {
                      "_id": {
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
                      "PROD_NBR": "$_id.PROD_NBR",
                      "PASSQTY": "$PASSQTY"
                    }
                  },
                  {
                    "$project": {
                      "_id": 0
                    }
                  },
                  {
                    "$sort": {
                      "PASSQTY": -1
                    }
                  }
                ]

        reasonAggregate = [
                  {
                    "$match": {
                      "COMPANY_CODE": tmpCOMPANY_CODE,
                      "SITE": tmpSITE,
                      "FACTORY_ID": tmpFACTORY_ID,
                      "ACCT_DATE": tmpACCT_DATE,
                      "WORK_CTR": "2110",
                      "TRANS_TYPE": "RWMO",
                      "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList]},
                      "DFCT_REASON": {
                        "$nin": [
                          "FA260-0"
                        ]
                      }
                    }
                  },
                  {
                    "$lookup": {
                      "from": "deftCodeView",
                      "as": "deftCodeList",
                      "let": {
                        "dfctCode": "$DFCT_CODE"
                      },
                      "pipeline": [
                        {
                          "$match": {
                            "$expr": {
                              "$and": [
                                {
                                  "$eq": [
                                    "$$dfctCode",
                                    "$DEFECT_CODE"
                                  ]
                                }
                              ]
                            }
                          }
                        },
                        {
                          "$project": {
                            "DEFECT_CODE": 1
                          }
                        }
                      ]
                    }
                  },
                  {
                    "$unwind": "$deftCodeList"
                  },
                  {
                    "$group": {
                      "_id": {
                        "DFCT_REASON": "$DFCT_REASON",
                        "REASON_DESC": "$REASON_DESC"
                      },
                      "REASONQTY": {
                        "$sum": "$QTY"
                      }
                    }
                  },
                  {
                    "$addFields": {
                      "DFCT_REASON": "$_id.DFCT_REASON",
                      "REASON_DESC": "$_id.REASON_DESC",
                      "REASONQTY": "$REASONQTY"
                    }
                  },
                   {
                    "$project": {
                      "_id": 0,
                    }
                  },
                  {
                    "$sort": {
                      "REASONQTY": -1
                    }
                  }
                ]
        
        if PROD_NBR != '':
            reasonAggregate[0]["$match"]["PROD_NBR"] = PROD_NBR
            passAggregate[0]["$match"]["PROD_NBR"] = PROD_NBR
        if tmpAPPLICATION != 'ALL':
            reasonAggregate[0]["$match"]["APPLICATION"] = tmpAPPLICATION
            passAggregate[0]["$match"]["APPLICATION"] = tmpAPPLICATION
        if CHECKCODE != '':
            reasonAggregate[0]["$match"]["DFCT_CODE"] = CHECKCODE

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("reasonHisAndCurrent")
            rData = self.aggregate(reasonAggregate)
            self.setMongoCollection("passHisAndCurrent")
            pData = self.aggregate(passAggregate)
            self.closeMongoConncetion()

            returnData = {
                "rData": rData,
                "pData": pData
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


    def _calEFALV2_3_Data(self, rdata, pdata):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        tmpOPER = self.jsonData["OPER"] if "OPER" in self.jsonData else "CKEN"
        _rData = []            
        for e in rdata:       
            _rData.append(e) 
        _pData = []            
        for p in pdata:       
            _pData.append(p) 
        
        yellowList = self._getEFA_impReason()

        prodNbrPASSQTY = 0
        for p in _pData:
            prodNbrPASSQTY += p["PASSQTY"]
        
        returnData = []
        for r in _rData:
            COLOR = "#ffd166" if r["DFCT_REASON"] in yellowList else "#0A8040"
            REASONYIELD = round(r["REASONQTY"] / prodNbrPASSQTY, 6) if r["REASONQTY"] != 0 and prodNbrPASSQTY != 0 else 0
            returnData.append({
                "PASSQTY": prodNbrPASSQTY,
                "DFCT_REASON": r["DFCT_REASON"],
                "REASON_DESC": r["REASON_DESC"],
                "REASONQTY": r["REASONQTY"],
                "REASONYIELD": REASONYIELD,
                "COLOR": COLOR,
                "PROD_NBR": tmpPROD_NBR,
                "OPER": tmpOPER
            })        
        return returnData

    def _getEFALV2_21_Data(self, OPERList):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]        
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        passAggregate = []
        deftAggregate = []        

        # pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,                
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList]}
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
                "$expr": {"$in": [{"$toInt": "$MAIN_WC"}, OPERList]}
            }
        }
        deftlookup1 = {
            "$lookup": {
                "from": "deftCodeView",
                "as": "deftCodeList",
                "let": {
                        "dfctCode": "$DFCT_CODE"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {
                                        "$eq": [
                                            "$$dfctCode",
                                            "$DEFECT_CODE"
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "DEFECT_CODE": 1
                        }
                    }
                ]
            }
        }
        deftunwind1 = {
            "$unwind": "$deftCodeList"
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
                    "MAIN_WC": "$MAIN_WC",
                    "DFCT_CODE": "$DFCT_CODE",
                    "ERRC_DESCR": "$ERRC_DESCR"
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
                "DFCT_CODE": "$_id.DFCT_CODE",
                "ERRC_DESCR": "$_id.ERRC_DESCR",
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
        if tmpPROD_NBR != '':
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

    def _calEFALV2_21_Data(self, dData, pData):
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        tmpOPER = self.jsonData["OPER"] if "OPER" in self.jsonData else "CKEN"
        _dData = []            
        for e in dData:       
            _dData.append(e) 
        _pData = []            
        for p in pData:       
            _pData.append(p) 
        
        prodNbrPASSQTY = 0
        for p in _pData:
            prodNbrPASSQTY += p["PASS_QTY"]
        returnData = []
        for r in _dData:
            DEFTYIELD = round(r["DEFT_QTY"] / prodNbrPASSQTY, 6) if r["DEFT_QTY"] != 0 and prodNbrPASSQTY != 0 else 0
            returnData.append({
                "PASSQTY": prodNbrPASSQTY,
                "DFCT_CODE": r["DFCT_CODE"],
                "ERRC_DESCR": r["ERRC_DESCR"],
                "PASSQTY": prodNbrPASSQTY,
                "DEFTQTY": r["DEFT_QTY"],
                "DEFTYIELD": DEFTYIELD,
                "PROD_NBR": tmpPROD_NBR,
                "OPER": tmpOPER
            })        

        returnData.sort(key=operator.itemgetter("DEFTQTY"), reverse=True)
        
        return returnData

    def _getEFA_impReason(self):
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("excelToJson")
            reqParm={
                "_id": "MOD2_DEFECT_DEV@J001-alarmReason"
            }
            projectionFields={
                "_id": False,
                "DATA": True
            }
            deftData = self.getMongoFind(reqParm,projectionFields)
            self.closeMongoConncetion()
            returnData = []
            for d in deftData:    
                for x in d["DATA"]:                  
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

    def _getEFALV2_WIP_1_Data(self):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"]
        tmpCHECKCODE = self.jsonData["CHECKCODE"] if "CHECKCODE" in self.jsonData else ""

        faWipAggregate =[
                  {
                    "$match": {
                      "COMPANY_CODE": tmpCOMPANY_CODE,
                      "SITE": tmpSITE,
                      "FACTORY_ID": tmpFACTORY_ID,
                      "ACCT_DATE": tmpACCT_DATE,
                      "WORK_CTR": "2110"
                    }
                  },
                  {
                    "$group": {
                      "_id": {
                        "COMPANY_CODE": "$COMPANY_CODE",
                        "SITE": "$SITE",
                        "FACTORY_ID": "$FACTORY_ID",
                        "WORK_CTR": "$WORK_CTR"
                      },
                      "TOTALQTY": {
                        "$sum": {
                          "$toInt": "$QTY"
                        }
                      }
                    }
                  }
                ]
        
        prodNbrWipAggregate = [
                  {
                    "$match": {
                      "COMPANY_CODE": tmpCOMPANY_CODE,
                      "SITE": tmpSITE,
                      "FACTORY_ID": tmpFACTORY_ID,
                      "ACCT_DATE": tmpACCT_DATE,
                      "PROD_NBR": tmpPROD_NBR,
                      "WORK_CTR": "2110"
                    }
                  },
                  {
                    "$group": {
                      "_id": {
                        "COMPANY_CODE": "$COMPANY_CODE",
                        "SITE": "$SITE",
                        "FACTORY_ID": "$FACTORY_ID",
                        "WORK_CTR": "$WORK_CTR"
                      },
                      "TOTALQTY": {
                        "$sum": {
                          "$toInt": "$QTY"
                        }
                      },
                      "QTY1": {
                        "$sum": {
                          "$toInt": "$QTY1"
                        }
                      },
                      "QTY2": {
                        "$sum": {
                          "$toInt": "$QTY2"
                        }
                      },
                      "QTY3": {
                        "$sum": {
                          "$toInt": "$QTY3"
                        }
                      },
                      "QTY5": {
                        "$sum": {
                          "$toInt": "$QTY5"
                        }
                      },
                      "QTY7": {
                        "$sum": {
                          "$toInt": "$QTY7"
                        }
                      },
                      "QTY15": {
                        "$sum": {
                          "$toInt": "$QTY15"
                        }
                      },
                      "QTY30": {
                        "$sum": {
                          "$toInt": "$QTY30"
                        }
                      },
                      "QTY31": {
                        "$sum": {
                          "$toInt": "$QTY31"
                        }
                      },
                      "QTY45": {
                        "$sum": {
                          "$toInt": "$QTY45"
                        }
                      }}
                  }
                ]
        
        if tmpAPPLICATION != 'ALL':
            faWipAggregate[0]["$match"]["APPLICATION"] = tmpAPPLICATION
            prodNbrWipAggregate[0]["$match"]["APPLICATION"] = tmpAPPLICATION
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("wipHisAndCurrent")
            faWip = self.aggregate(faWipAggregate)
            prodNbrWip = self.aggregate(prodNbrWipAggregate)
            self.closeMongoConncetion()

            returnData = {
                "faWip": faWip,
                "prodNbrWip": prodNbrWip
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

    def _calEFALV2_WIP_1_Data(self, faWip, prodNbrWip):
        _fData = []            
        for e in faWip:       
            _fData.append(e) 
        _pData = []            
        for p in prodNbrWip:       
            _pData.append(p) 
                           
        faData = {}
        for fa in _fData:       
            faData= {
                "y": fa["TOTALQTY"],
                "name": "FA 2110 WIP",
                "color": "#058DC7",
                "radius": '112%',
                "innerRadius": '88%'
            }

        prodData1 = {
            "y": 0,
            "name": "PROD_NBR",
            "color": "#656565",
            "radius": '87%',
            "innerRadius": '63%'
        }
        prodData2 = {
            "y": 0,
            "name":"< 24hrs",
            "color": "#8CE66A",
            "radius": '62%',
            "innerRadius": '38%',
            "underColor": "#D1F5C4"
        }
        for pn in _pData:       
            prodData1 = {
                "y": pn["TOTALQTY"],
                "name": "PROD_NBR",
                "color": "#656565",
                "radius": '87%',
                "innerRadius": '63%'
            } 
            _name = ""
            _y = ""
            _fontColor = ""
            _underColor = ""
            _OVERONEDAYQTY = pn["QTY2"] + pn["QTY3"]+ pn["QTY5"]+ pn["QTY7"]+ pn["QTY15"] \
                + pn["QTY30"]+ pn["QTY31"] + pn["QTY45"]
            if _OVERONEDAYQTY > 0 :
                _name = "> 24hrs"
                _y = _OVERONEDAYQTY
                _fontColor = "#F9CE24" #黃色
                _underColor = "#FAEDB9"  #淺黃色            
            else:
                _name = "< 24hrs"
                _y = pn["QTY1"]
                _fontColor = "#8CE66A"  #綠色
                _underColor = "#D1F5C4"  #淺綠色            
            prodData2 = {
                "y": _y,
                "name": _name,
                "color": _fontColor,
                "radius": '62%',
                "innerRadius": '38%',
                "underColor": _underColor
            }

        returnData = {
            "faData":faData,
            "prodData1":prodData1,
            "prodData2":prodData2
        }
        
        return returnData

    def _getEFALV2_WIP_2_Data(self):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        
        wipAggregate = [
                  {
                    "$match": {
                      "COMPANY_CODE": tmpCOMPANY_CODE,
                      "SITE": tmpSITE,
                      "FACTORY_ID": tmpFACTORY_ID,
                      "ACCT_DATE": tmpACCT_DATE,
                      "WORK_CTR": "2110"
                    }
                  },
                  {
                    "$group": {
                      "_id": {
                        "COMPANY_CODE": "$COMPANY_CODE",
                        "SITE": "$SITE",
                        "FACTORY_ID": "$FACTORY_ID",
                        "WORK_CTR": "$WORK_CTR",
                        "PROD_NBR" : "$PROD_NBR"
                      },
                      "TOTALQTY": {
                        "$sum": {
                          "$toInt": "$QTY"
                        }
                      },
                      "QTY1": {
                        "$sum": {
                          "$toInt": "$QTY1"
                        }
                      },
                      "QTY2": {
                        "$sum": {
                          "$toInt": "$QTY2"
                        }
                      },
                      "QTY3": {
                        "$sum": {
                          "$toInt": "$QTY3"
                        }
                      },
                      "QTY5": {
                        "$sum": {
                          "$toInt": "$QTY5"
                        }
                      },
                      "QTY7": {
                        "$sum": {
                          "$toInt": "$QTY7"
                        }
                      },
                      "QTY15": {
                        "$sum": {
                          "$toInt": "$QTY15"
                        }
                      },
                      "QTY30": {
                        "$sum": {
                          "$toInt": "$QTY30"
                        }
                      },
                      "QTY31": {
                        "$sum": {
                          "$toInt": "$QTY31"
                        }
                      },
                      "QTY45": {
                        "$sum": {
                          "$toInt": "$QTY45"
                        }
                      }}
                  },
                  {
                      "$project": {
                        "_id": 0,
                        "COMPANY_CODE": "$_id.COMPANY_CODE",
                        "SITE": "$_id.SITE",
                        "FACTORY_ID": "$_id.FACTORY_ID",
                        "WORK_CTR": "$_id.WORK_CTR",
                        "PROD_NBR" : "$_id.PROD_NBR",
                        "TOTALQTY": "$TOTALQTY",
                        "QTY1": "$QTY1",
                        "QTY2": "$QTY2",
                        "QTY3": "$QTY3",
                        "QTY5": "$QTY5",
                        "QTY7": "$QTY7",
                        "QTY15": "$QTY15",
                        "QTY30": "$QTY30",
                        "QTY31": "$QTY31",
                        "QTY45": "$QTY45"
                      }
                  }
                ]
        
        if tmpAPPLICATION != 'ALL':
            wipAggregate[0]["$match"]["APPLICATION"] = tmpAPPLICATION

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("wipHisAndCurrent")
            faWip = self.aggregate(wipAggregate)
            self.closeMongoConncetion()
            returnData = faWip
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

    def _calEFALV2_WIP_2_Data(self, prodNbrWip):
        _pData = []            
        for p in prodNbrWip:       
            _pData.append(p) 
            
        returnData = []
        for pn in _pData:  
            oneDay = 0
            oneThreeDay = 0
            threeDay = 0
            oneDay = pn["QTY1"]   
            oneThreeDay = pn["QTY2"] + pn["QTY3"]
            threeDay = pn["QTY5"] + pn["QTY7"] + pn["QTY15"] + pn["QTY30"]+ pn["QTY31"] + pn["QTY45"]
            if threeDay != 0:
                returnData.append({
                    "days":"3day~",
                    "qty":threeDay,
                    "color": "red",
                    "PROD_NBR" : pn["PROD_NBR"],
                    "XVALUE": 0
                })
            if oneThreeDay != 0:
                returnData.append({
                    "days":"1day~3day",
                    "qty":oneThreeDay,
                    "color": "yellow",
                    "PROD_NBR" : pn["PROD_NBR"],
                    "XVALUE": 1
                })
            if oneDay != 0:
                returnData.append({
                    "days":"~24hr",
                    "qty":oneDay,
                    "color": "green",
                    "PROD_NBR" : pn["PROD_NBR"],
                    "XVALUE": 2
                })
        return returnData

