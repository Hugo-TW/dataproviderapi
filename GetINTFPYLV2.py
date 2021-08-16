# -*- coding: utf-8 -*-
import json
from re import X
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType

class INTFPYLV2(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        #M011 => MOD1
        #J001 => MOD2
        #J003 => MOD3
        self.operSetData = {
            "J001": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 3000, "FPY": 0.95},
                        "TABLET": {"qytlim": 3000, "FPY": 0.90},
                        "NB": {"qytlim": 3000, "FPY": 0.94},
                        "TV": {"qytlim": 3000, "FPY": 0.90},
                        "AA": {"qytlim": 3000, "FPY": 0.95}
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
                        "CE": {"qytlim": 3000, "target": 0.97},
                        "TABLET": {"qytlim": 3000, "target": 0.955},
                        "NB": {"qytlim": 3000, "target": 0.96},
                        "TV": {"qytlim": 3000, "target": 0.90},
                        "AA": {"qytlim": 3000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
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
                        "CE": {"qytlim": 3000, "FPY": 0.95},
                        "TABLET": {"qytlim": 3000, "FPY": 0.90},
                        "NB": {"qytlim": 3000, "FPY": 0.94},
                        "TV": {"qytlim": 3000, "FPY": 0.90},
                        "AA": {"qytlim": 3000, "FPY": 0.95}
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
                        "CE": {"qytlim": 3000, "target": 0.97},
                        "TABLET": {"qytlim": 3000, "target": 0.955},
                        "NB": {"qytlim": 3000, "target": 0.96},
                        "TV": {"qytlim": 3000, "target": 0.90},
                        "AA": {"qytlim": 3000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
                        "TV": {"qytlim": 3000, "target": 0.003},
                        "AA": {"qytlim": 3000, "target": 0.003}
                    },
                    "numerator": {},
                    "denominator": {}
                }
            },
            "M011": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 3000, "FPY": 0.95},
                        "TABLET": {"qytlim": 3000, "FPY": 0.90},
                        "NB": {"qytlim": 3000, "FPY": 0.94},
                        "TV": {"qytlim": 3000, "FPY": 0.90},
                        "AA": {"qytlim": 3000, "FPY": 0.95}
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
                        "CE": {"qytlim": 3000, "target": 0.97},
                        "TABLET": {"qytlim": 3000, "target": 0.955},
                        "NB": {"qytlim": 3000, "target": 0.96},
                        "TV": {"qytlim": 3000, "target": 0.90},
                        "AA": {"qytlim": 3000, "target": 0.95}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "target": 0.003},
                        "TABLET": {"qytlim": 1000, "target": 0.003},
                        "NB": {"qytlim": 1000, "target": 0.003},
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

            #redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpKPITYPE)
            tmp.append(tmpACCT_DATE)
            redisKey = bottomLine.join(tmp)
            expirTimeKey = tmpFACTORY_ID + '_DEFT'

            if tmpFACTORY_ID not in self.operSetData.keys():
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            # Check Redis Data
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}
           
            if tmpKPITYPE == "FPYLV2PIE":
                PCBIData = self._getFPYLV2PIEData("PCBI", tmpPROD_NBR)
                PCBIResult = self._groupFPYLV2PIEOPER(PCBIData)
                LAMData = self._getFPYLV2PIEData("LAM", tmpPROD_NBR)
                LAMResult = self._groupFPYLV2PIEOPER(LAMData)
                AAFCData = self._getFPYLV2PIEData("AAFC", tmpPROD_NBR)
                AAFCResult = self._groupFPYLV2PIEOPER(AAFCData)
                CKENData = self._getFPYLV2PIEData("CKEN", tmpPROD_NBR)
                CKENResult = self._groupFPYLV2PIEOPER(CKENData)
                DKENData = self._getFPYLV2PIEData("DKEN", tmpPROD_NBR)
                DKENResult = self._groupFPYLV2PIEOPER(DKENData)
                
                returnData = self._calPRODFPYBaseData(PCBIResult,LAMResult,AAFCResult,CKENResult,DKENResult)
                
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

    def _getFPYLV2PIEData(self, OPER, PROD_NBR):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]

        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER]

        deftAggregate = []

        #deft
        deftMatch1 = {
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
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
                    "ACCT_DATE": "$ACCT_DATE",
                    "APPLICATION": "$APPLICATION",
                    "MAIN_WC": {"$toInt": "$MAIN_WC"},                 
                    "DFCT_CODE" : "$DFCT_CODE"
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
                "DEFT_QTY": "$DEFT_QTY"
            }
        }
        deftMatch2 = {
            "$match": {"MAIN_WC": {'$gt': numeratorData["fromt"], '$lt': numeratorData["tot"]}}
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
                    "DFCT_CODE" : "$DFCT_CODE"
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
                "DFCT_CODE" : "$_id.DFCT_CODE",
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
                "APPLICATION": 1,
                "DFCT_CODE" : 1
            }
        }

        deftAggregate.extend([deftMatch1, deftGroup1, deftProject1,
                             deftMatch2, deftGroup2, deftProject2, deftSort])
        
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

    def _groupFPYLV2PIEOPER(self, dData): 
        deftData = []            
        for d in dData:       
            deftData.append(d) 

        if deftData != []:  
            deftSUM = 0
            for p in deftData:            
                deftSUM += d["DeftSUMQty"]
            p = deftData[0]
            data = {
                "COMPANY_CODE": p["COMPANY_CODE"],
                "SITE": p["SITE"],
                "FACTORY_ID": p["FACTORY_ID"],
                "PROD_NBR": p["PROD_NBR"],
                "ACCT_DATE": datetime.datetime.strptime(p["ACCT_DATE"], '%Y%m%d').strftime('%Y-%m-%d'),
                "APPLICATION": p["APPLICATION"] if "APPLICATION" in p.keys() else None,
                "PROD_NBR": p["PROD_NBR"],
                "OPER": p["OPER"],
                "DeftSUMQty" : deftSUM
            }
            return data
        else: 
            return None

    def _calPRODFPYBaseData(self, PCBI, LAM, AAFC, CKEN, DKEN):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]        
        tmpAPPLICATION =self.jsonData["APPLICATION"]
        tmpKPITYPE = self.jsonData["KPITYPE"]
        tmpACCT_DATE = self.jsonData["ACCT_DATE"]
        tmpPROD_NBR = self.jsonData["PROD_NBR"]

        TotalDeftSUMQty = 0
        tempData = []
        tempData.extend([PCBI,LAM,AAFC,CKEN,DKEN])
        for x in tempData:
            if x != None:
                tmpAPPLICATION = x["APPLICATION"]
                TotalDeftSUMQty += x["DeftSUMQty"]
        
        DATASERIES = []
        for x in tempData:
            if x != None:
                DATASERIES.append({
                    "OPER": x["OPER"],
                    "VALUE": round(x["DeftSUMQty"] / TotalDeftSUMQty , 2) if TotalDeftSUMQty !=0 else 0,
                    "COLOR": None,
                    "SELECT": None,
                    "SLICED": None,
                    "DeftSUMQty": x["DeftSUMQty"]
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

