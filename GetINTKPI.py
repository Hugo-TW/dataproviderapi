# -*- coding: utf-8 -*-
import json
from re import X
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType


class INTKPI(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        self.operSetData = {
            "J001": {
                "FPY": {
                    "limit": {
                        "CE": {"qytlim": 3000, "FPY": 0.95},
                        "TABLET": {"qytlim": 3000, "FPY": 0.90},
                        "NB": {"qytlim": 3000, "FPY": 0.94}
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
                        "CE": {"qytlim": 3000, "FPT": 0.97},
                        "TABLET": {"qytlim": 3000, "FPT": 0.955},
                        "NB": {"qytlim": 3000, "FPT": 0.96}
                    },
                    "numerator": {},
                    "denominator": {}
                },
                "EFA": {
                    "limit": {
                        "CE": {"qytlim": 1000, "FPT": 0.003},
                        "TABLET": {"qytlim": 1000, "FPT": 0.003},
                        "NB": {"qytlim": 1000, "FPT": 0.003}
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
            

            if tmpKPITYPE == "FPY": 
                expirTimeKey = tmpFACTORY_ID + '_PASS'               
                # Check Redis Data
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.writeLog(f"Cache Data From Redis")
                    return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

                data = []
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

                PRODFPYBaseData = self._calPRODFPYBaseData(
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
            
            if tmpKPITYPE == "PRODFPY":  
                expirTimeKey = tmpFACTORY_ID + '_PASS'
                # Check Redis Data
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):
                    self.writeLog(f"Cache Data From Redis")
                    return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=self.getKeyExpirTime(expirTimeKey))).timetuple()), "DataSource": "Redis"}

                data = []
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

                PRODFPYBaseData = self._calPRODFPYBaseData(
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

        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER]
        denominatorValue = getFabData["FPY"]["denominator"][OPER]

        passAggregate = [{
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]}
            }
        },
            {
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
        },
            {
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
        },
            {
            "$match": {"MAIN_WC": {"$in": denominatorValue}}
        },
            {
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
        },

            {
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
        },
            {
            "$sort": {
                "COMPANY_CODE": 1,
                "SITE": 1,
                "FACTORY_ID": 1,
                "PROD_NBR": 1,
                "ACCT_DATE": 1,
                "MAIN_WC": 1,
                "APPLICATION": 1
            }
        }]
        #self.writeLog(f'passAggregate:{passAggregate}')

        deftAggregate = [{
            "$match": {
                "COMPANY_CODE": tmpCOMPANY_CODE,
                "SITE": tmpSITE,
                "FACTORY_ID": tmpFACTORY_ID,
                "ACCT_DATE": tmpACCT_DATE,
                "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]}
            }
        },
            {
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
        },
            {
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
        },
            {
            "$match": {"MAIN_WC": {'$gt': numeratorData["fromt"], '$lt': numeratorData["tot"]}}
        },
            {
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
        },
            {
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
        },
            {
                "$sort": {
                    "COMPANY_CODE": 1,
                    "SITE": 1,
                    "FACTORY_ID": 1,
                    "PROD_NBR": 1,
                    "ACCT_DATE": 1,
                    "MAIN_WC": 1,
                    "APPLICATION": 1
                }
        }]
        #self.writeLog(f'deftAggregate:{deftAggregate}')

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
            oData["APPLICATION"] = copy.deepcopy(p["APPLICATION"])
            oData["PROD_NBR"] = copy.deepcopy(p["PROD_NBR"])
            oData["OPER"] = copy.deepcopy(p["OPER"])
            oData["PassSUMQty"] = copy.deepcopy(p["PassSUMQty"])
            if d == []:
                oData["DeftSUMQty"] = 0
            else:
                oData["DeftSUMQty"] = copy.deepcopy(d[0]["DeftSUMQty"])
            oData["DEFECT_RATE"] = round(
                oData["DeftSUMQty"] / oData["PassSUMQty"], 4)
            oData["FPY_RATE"] = round(1 - oData["DEFECT_RATE"], 4)

            if oData["DeftSUMQty"] < oData["PassSUMQty"] and oData["FPY_RATE"] > 0:
                data.append(copy.deepcopy(oData))
            oData = {}
        return data

    def _calPRODFPYBaseData(self, PCBI, LAM, AAFC, CKEN, DKEN):
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
            targrt = getLimitData[x["APPLICATION"]]["FPY"]
            if x["FPY"] >= targrt:
                GREEN_VALUE += 1
            elif targrt > x["FPY"] >= (targrt-(targrt*0.01)):
                YELLOW_VALUE += 1
            elif (targrt-(targrt*0.01)) > x["FPY"]:
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
            xLimit = ""
            yLimit = ""
        else:
            d = list(filter(lambda d: d["APPLICATION"]== tmpAPPLICATION, PRODFPYBaseData))  
            xLimit = getLimitData[tmpAPPLICATION]["qytlim"]
            yLimit = getLimitData[tmpAPPLICATION]["FPY"]  

        for x in d:
            QTYLimit = getLimitData[x["APPLICATION"]]["qytlim"]
            FPYLimit = getLimitData[x["APPLICATION"]]["FPY"]            
            if FPYLimit > x["FPY"] and x["AvegPASSQTY"] > QTYLimit :
                COLOR = "#EF476F"
                SYMBOL = "twinkle"
            else :
                COLOR = "#118AB2"
                SYMBOL = "undefined"     
            DATASERIES.append({
                "APPLICATION": x["APPLICATION"],
                "PROD_NBR": x["PROD_NBR"],
                "YIELD": x["FPY"],
                "QTY": x["AvegPASSQTY"],
                "COLOR": COLOR,
                "SYMBOL": SYMBOL
            })

        returnData = {
            "XLIMIT": xLimit,
            "YLIMIT": yLimit * 100,
            "DATASERIES": DATASERIES
        }

        return returnData