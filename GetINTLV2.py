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

            #redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpKPITYPE)
            tmp.append(tmpACCT_DATE)
            tmp.append(tmpPROD_NBR)
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
                "RW_COUNT": "0" 
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
                "DFCT_CODE" : "$_id.DFCT_CODE",
                "ERRC_DESCR" : "$_id.ERRC_DESCR",
                "DeftSUMQty": "$DeftSUMQty"
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

    def _groupFPYLV2PIEOPER(self, dData): 
        deftData = []            
        for d in dData:       
            deftData.append(d) 

        if deftData != []:  
            deftSUM = 0
            for p in deftData:            
                deftSUM += p["DeftSUMQty"]
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

    def _calFPYLV2PIEData(self, PCBI, LAM, AAFC, CKEN, DKEN):
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
                    "VALUE": round(x["DeftSUMQty"] / TotalDeftSUMQty , 2) if TotalDeftSUMQty !=0 else 0,
                    "COLOR": colorMap[x["OPER"]]["HEX"] if x["OPER"] in colorMap.keys() else None,
                    "SELECT": None,
                    "SLICED": None,
                    "DeftSUMQty": x["DeftSUMQty"],
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
                oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
                oData["OPER"] = copy.deepcopy(p["OPER"])
                oData["DFCT_CODE"] = copy.deepcopy(d["DFCT_CODE"])
                oData["ERRC_DESCR"] = copy.deepcopy(d["ERRC_DESCR"])
                oData["PassSUMQty"] = copy.deepcopy(p["PassSUMQty"])
                if d == []:
                    oData["DeftSUMQty"] = 0
                else:
                    oData["DeftSUMQty"] = copy.deepcopy(d["DeftSUMQty"])
                if oData["DeftSUMQty"] == 0:
                    oData["DEFECT_RATE"] = 0
                else:
                    if(oData["PassSUMQty"] != 0):
                        ds = Decimal(oData["DeftSUMQty"])
                        ps = Decimal(oData["PassSUMQty"])
                        oData["DEFECT_RATE"] =  self._DecimaltoFloat((ds / ps).quantize(Decimal('.00000000'), ROUND_HALF_UP))
                    else:
                        oData["DEFECT_RATE"] = 1
                oData["FPY_RATE"] = round(1 - oData["DEFECT_RATE"], 4)
                if oData["DeftSUMQty"] < oData["PassSUMQty"] and oData["FPY_RATE"] > 0:
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
                        "DeftSUM": x["DeftSUMQty"],
                        "PassSUM": x["PassSUMQty"],
                        "DEFECT_RATE": x["DEFECT_RATE"]*100
                    })
            else:
                for cx in DATASERIES:
                    if cx["OPER"] == x["OPER"] and cx["DFCT_CODE"] == cDFct :
                       cx["YVALUE"] += x["DEFECT_RATE"]*100
                       cx["DEFECT_RATE"] += x["DEFECT_RATE"]*100
                       
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

        return deftData
    
    def _DecimaltoFloat(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)

