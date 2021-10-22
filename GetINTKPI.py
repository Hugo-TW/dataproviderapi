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

            #redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpKPITYPE)
            tmp.append(tmpACCT_DATE)
            redisKey = bottomLine.join(tmp)
            expirTimeKey = tmpFACTORY_ID + '_PASS'

    
            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # Check Redis Data
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}
            
            #一階 FPY KPI API
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

            #二階 FPY 泡泡圖 API
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

            #二階 MSHIP 泡泡圖 API
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

                efaData = self._getEFAData(OPERDATA)
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

    def _getFPYData(self, OPER):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

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
                "ACCT_DATE": "$_id.ACCT_DATE",
                "APPLICATION": "$_id.APPLICATION",
                "OPER": OPER,
                "PassSUMQty": "$PassSUMQty"
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
                "DeftSUMQty": {
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
                "DeftSUMQty": "$DeftSUMQty"
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

        passAggregate.extend([passMatch1, passGroup1, passProject1, passGroup2, passProject2, passSort])
        deftAggregate.extend([deftMatch1, deftGroup1, deftProject1, deftGroup2, deftProject2, deftSort])

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
            oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
            oData["OPER"] = copy.deepcopy(p["OPER"])
            oData["PassSUMQty"] = copy.deepcopy(p["PassSUMQty"])
            if d == []:
                oData["DeftSUMQty"] = 0
            else:
                oData["DeftSUMQty"] = copy.deepcopy(d[0]["DeftSUMQty"])
            if oData["DeftSUMQty"] == 0:
                oData["DEFECT_RATE"] = 0
            else:
                if(oData["PassSUMQty"] != 0):
                    oData["DEFECT_RATE"] = round(
                        oData["DeftSUMQty"] / oData["PassSUMQty"], 4)
                else:
                    oData["DEFECT_RATE"] = 1
            oData["FPY_RATE"] = round(1 - oData["DEFECT_RATE"], 4)
            if oData["DeftSUMQty"] < oData["PassSUMQty"] and oData["FPY_RATE"] > 0:
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
        PASSOPER = 0
        for prod in PRODList:
            d1 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], PCBI))
            if d1 == []:
                PCBIFPY = 1
            else:
                PCBIFPY = copy.deepcopy(d1[0]["FPY_RATE"])
                PASSQTYSUM += d1[0]["PassSUMQty"]
                PASSOPER += 1

            d2 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"], LAM))
            if d2 == []:
                LAMFPY = 1
            else:
                LAMFPY = copy.deepcopy(d2[0]["FPY_RATE"])
                PASSQTYSUM += d2[0]["PassSUMQty"]
                PASSOPER += 1

            d3 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], AAFC))
            if d3 == []:
                AAFCFPY = 1
            else:
                AAFCFPY = copy.deepcopy(d3[0]["FPY_RATE"])
                PASSQTYSUM += d3[0]["PassSUMQty"]
                PASSOPER += 1

            d4 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], CKEN))
            if d4 == []:
                CKENFPY = 1
            else:
                CKENFPY = copy.deepcopy(d4[0]["FPY_RATE"])
                PASSQTYSUM += d4[0]["PassSUMQty"]
                PASSOPER += 1

            d5 = list(filter(lambda d: d["PROD_NBR"]
                      == prod["PROD_NBR"], DKEN))
            if d5 == []:
                DKENFPY = 1
            else:
                DKENFPY = copy.deepcopy(d5[0]["FPY_RATE"])
                PASSQTYSUM += d5[0]["PassSUMQty"]
                PASSOPER += 1

            FPY = round(PCBIFPY * LAMFPY * AAFCFPY * CKENFPY * DKENFPY, 4)

            PRODData.append({
                "PROD_NBR": prod['PROD_NBR'],
                "APPLICATION": prod["APPLICATION"],
                "PCBIFPY": PCBIFPY,
                "LAMFPY": LAMFPY,
                "AAFCFPY": AAFCFPY,
                "CKENFPY": CKENFPY,
                "DKENFPY": DKENFPY,
                "FPY": FPY,
                "AvegPASSQTY": round(PASSQTYSUM / PASSOPER, 0)
            })
            PASSQTYSUM = 0
            PASSOPER = 0

        return PRODData

    def _calFPYData(self, PRODFPYBaseData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"]

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

    def _calPRODFPYData(self, PRODFPYBaseData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["FPY"]["limit"]

        COLOR = "#118AB2"
        SYMBOL = "undefined"

        DATASERIES = []
        if tmpAPPLICATION == "ALL":
            d = PRODFPYBaseData
            xLimit = None
            yLimit = None
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
        #yellow ffd166
        #green 06d6a0
        #blue 118AB2
        #midGreen 073b4c

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
        #因為使用 operator.itemgetter 方法 排序順序要反過來執行
        #不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key = operator.itemgetter("QTY"), reverse = True) 
        DATASERIES.sort(key = operator.itemgetter("YIELD"), reverse = False)        
        DATASERIES.sort(key = operator.itemgetter("QUADRANT"), reverse = True)

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

        #PRODList
        PRODList = []
        for x in shipData:
            if {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]} not in PRODList:
                PRODList.append(
                    {"PROD_NBR": x["PROD_NBR"], "APPLICATION": x["APPLICATION"]})
        for x in gradeData:
            if "APPLICATION" not in x: x["APPLICATION"] = "ALL"
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
                if oData["TOBESCRAP_SUMQTY"] == 0:
                    oData["GRADW_YIELD"] = 1
                else:
                    oData["GRADW_YIELD"] = 1 - \
                        round(oData["TOBESCRAP_SUMQTY"] /
                              oData["SHIP_SUMQTY"], 4)
                oData["DOWNGRADE_SUMQTY"] = copy.deepcopy(
                    _gradeData[0]["DOWNGRADE_SUMQTY"])
                oData["TOTAL_SUMQTY"] = copy.deepcopy(
                    _gradeData[0]["TOTAL_SUMQTY"])

                if oData["DOWNGRADE_SUMQTY"] == 0:
                    oData["TOTAL_YIELD"] = 1
                else:
                    oData["TOTAL_YIELD"] = 1 - \
                        round(oData["DOWNGRADE_SUMQTY"] /
                              oData["TOTAL_SUMQTY"], 4)
                oData["MSHIP"] = round(oData["GRADW_YIELD"] * oData["TOTAL_YIELD"], 4) if oData["TOTAL_YIELD"] != 0 else 0
                if oData["MSHIP"] > 0:
                    mshipData.append(copy.deepcopy(oData))
                oData = {}
        return mshipData

    def _calMSHIPData(self, groupMSHIPData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["M-SHIP"]["limit"]

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
        getLimitData = self.operSetData[tmpFACTORY_ID]["M-SHIP"]["limit"]

        COLOR = "#118AB2"
        SYMBOL = "undefined"

        DATASERIES = []
        if tmpAPPLICATION == "ALL":
            d = PRODMSHIPBaseData
            xLimit = None
            yLimit = None
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
        #yellow ffd166
        #green 06d6a0
        #blue 118AB2
        #midGreen 073b4c

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
        #因為使用 operator.itemgetter 方法 排序順序要反過來執行
        #不同欄位key 排序方式不同時 需要 3 - 2 - 1  反順序去寫code
        DATASERIES.sort(key = operator.itemgetter("QTY"), reverse = True) 
        DATASERIES.sort(key = operator.itemgetter("YIELD"), reverse = False)        
        DATASERIES.sort(key = operator.itemgetter("QUADRANT"), reverse = True)

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

    def _getEFAData(self, OPERDATA):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpAPPLICATION = self.jsonData["APPLICATION"]

        OPERList = []
        for x in OPERDATA:
            OPERList.append(f'{x.get("OPER")}')

        passAggregate = []
        deftAggregate = []

        #pass
        passMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "MAIN_WC": {"$in": OPERList}
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

        #deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "MAIN_WC": {"$in": OPERList}
            }
        }
        deftlookup1 ={
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
                "MAIN_WC": "$_id.MAIN_WC",
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
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }

        if tmpAPPLICATION != "ALL":
            passMatch1["$match"]["APPLICATION"] = tmpAPPLICATION
            deftMatch1["$match"]["APPLICATION"] = tmpAPPLICATION

        passAggregate.extend([passMatch1, passGroup1, passProject1, passSort])
        deftAggregate.extend([deftMatch1, deftlookup1, deftunwind1, deftGroup1, deftProject1, deftSort])
       
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
            d = list(filter(lambda d: d["PROD_NBR"]== p["PROD_NBR"]
                and d["MAIN_WC"]== p["MAIN_WC"], deftData))
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
                    oData["MAIN_WC"] = copy.deepcopy(p["MAIN_WC"])
                    oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
                    oData["DFCT_CODE"] = copy.deepcopy(dd["DFCT_CODE"])
                    oData["DEFT_QTY"] = copy.deepcopy(dd["DEFT_QTY"])
                    oData["DEFECT_RATE"] = round(
                        oData["DEFT_QTY"] / oData["PASS_QTY"], 4)
                    data.append(copy.deepcopy(oData))
                    oData = {}
        return data

    def _calEFAData(self, EFAData):
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        getLimitData = self.operSetData[tmpFACTORY_ID]["EFA"]["limit"]

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
            d1 = list(filter(lambda d: d["PROD_NBR"] == prod["PROD_NBR"], EFAData))
            targrt = 0.003
            targrtQTY = 3000
            if prod["APPLICATION"] in getLimitData.keys():
                targrt = getLimitData[prod["APPLICATION"]]["target"]
                targrtQTY = getLimitData[prod["APPLICATION"]]["qytlim"]           
            
            checkTargrt = list(filter(lambda d: d["DEFECT_RATE"] >= targrt, d1))
            checkQTY = list(filter(lambda d: d["PASS_QTY"] >= targrtQTY, checkTargrt))

            if len(checkTargrt) != 0:
                if len(checkQTY) != 0:
                    RED_VALUE += 1
                    REDL.append(prod)
                else:
                    YELLOW_VALUE += 1
                    YELLOWL.append(prod)
            else:
                GREEN_VALUE += 1  
                GREENL.append(prod)
                

        returnData = {
            "CLASS_TYPE": "EFA",
            "GREEN_VALUE": GREEN_VALUE,
            "YELLOW_VALUE": YELLOW_VALUE,
            "RED_VALUE": RED_VALUE
        }

        return returnData


