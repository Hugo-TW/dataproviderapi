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


class INTTALK(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
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
            tmpFUNCTYPE = self.jsonData["FUNCTYPE"]
            tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
            tmpSITE = self.jsonData["SITE"]
            tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
            tmpACCT_DATE = self.jsonData["ACCT_DATE"]
            tmpPROD_NBR = self.jsonData["PROD_NBR"]
            tmpOPER = self.jsonData["OPER"]
            tmpCODE = self.jsonData["CODE"]

            if tmpFUNCTYPE == "CREATE_FPY":
                tmpCONTENTTYPE = self.jsonData["CONTENTTYPE"] if "CONTENTTYPE" in self.jsonData else None
                tmpCONTENT = self.jsonData["CONTENT"] if "CONTENT" in self.jsonData else None
                tmpRATE = self.jsonData["RATE"] if "RATE" in self.jsonData else None
                tmpTOTAL = self.jsonData["TOTAL"] if "TOTAL" in self.jsonData else None

                Jmsg = {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": tmpACCT_DATE,
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "CODE": tmpCODE
                }
                self.getMongoConnection()
                self.setMongoDb("IAMP")
                self.setMongoCollection("intTalk")
                org = self.getMongoFindOne(Jmsg)
                self.closeMongoConncetion()
                if org != None:
                    Jmsg1 = copy.deepcopy(org)
                else:
                    Jmsg1 = copy.deepcopy(Jmsg)

                if tmpCONTENTTYPE == "AA":
                    Jmsg1["AA"] = tmpCONTENT
                elif tmpCONTENTTYPE == "COMM":
                    Jmsg1["COMM"] = tmpCONTENT
                
                DR_DATA = self._getDRDATA_FPY(tmpOPER, tmpPROD_NBR, tmpCODE, tmpACCT_DATE)

                if DR_DATA != None:
                    Jmsg1["RATE"] = DR_DATA["RATE"]
                    Jmsg1["TOTAL"] = DR_DATA["TOTAL"]

                """
                if tmpRATE != None:
                    Jmsg1["RATE"] = tmpRATE
                if tmpTOTAL != None:
                    Jmsg1["TOTAL"] = tmpTOTAL
                """ 
                              

                utc = datetime.datetime.strptime(Jmsg["ACCT_DATE"], '%Y%m%d')
                Jmsg1["UTC"] = int(time.mktime(utc.timetuple()))

                self.getMongoConnection()
                self.setMongoDb("IAMP")
                self.setMongoCollection("intTalk")
                count = self.deleteToMongo(Jmsg)
                self.inserOneToMongo(Jmsg1)
                self.closeMongoConncetion()

                tmpSTARTDT = self.jsonData["STARTDT"]
                tmpENDDT = self.jsonData["ENDDT"]
                dataArray = self._dataArray(tmpSTARTDT, tmpENDDT)
                tempData = self._groupTalkData(self._getTalkData(tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID,
                                                                 tmpPROD_NBR, tmpCODE, dataArray))
                returnData = {"draw": 1,
                              "recordsTotal": len(tempData),
                              "recordsFiltered": len(tempData),
                              "data": tempData
                              }

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpFUNCTYPE == "CREATE_EFA":
                tmpCONTENTTYPE = self.jsonData["CONTENTTYPE"] if "CONTENTTYPE" in self.jsonData else None
                tmpCONTENT = self.jsonData["CONTENT"] if "CONTENT" in self.jsonData else None
                tmpRATE = self.jsonData["RATE"] if "RATE" in self.jsonData else None
                tmpTOTAL = self.jsonData["TOTAL"] if "TOTAL" in self.jsonData else None

                Jmsg = {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": tmpACCT_DATE,
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "CODE": tmpCODE
                }
                self.getMongoConnection()
                self.setMongoDb("IAMP")
                self.setMongoCollection("intTalk")
                org = self.getMongoFindOne(Jmsg)
                self.closeMongoConncetion()
                if org != None:
                    Jmsg1 = copy.deepcopy(org)
                else:
                    Jmsg1 = copy.deepcopy(Jmsg)

                if tmpCONTENTTYPE == "AA":
                    Jmsg1["AA"] = tmpCONTENT
                elif tmpCONTENTTYPE == "COMM":
                    Jmsg1["COMM"] = tmpCONTENT
                
                DR_DATA = self._getDRDATA_EFA(tmpOPER, tmpPROD_NBR, tmpCODE, tmpACCT_DATE)

                if DR_DATA != None:
                    Jmsg1["RATE"] = DR_DATA["RATE"]
                    Jmsg1["TOTAL"] = DR_DATA["TOTAL"]

                """
                if tmpRATE != None:
                    Jmsg1["RATE"] = tmpRATE
                if tmpTOTAL != None:
                    Jmsg1["TOTAL"] = tmpTOTAL
                """ 
                              

                utc = datetime.datetime.strptime(Jmsg["ACCT_DATE"], '%Y%m%d')
                Jmsg1["UTC"] = int(time.mktime(utc.timetuple()))

                self.getMongoConnection()
                self.setMongoDb("IAMP")
                self.setMongoCollection("intTalk")
                count = self.deleteToMongo(Jmsg)
                self.inserOneToMongo(Jmsg1)
                self.closeMongoConncetion()

                tmpSTARTDT = self.jsonData["STARTDT"]
                tmpENDDT = self.jsonData["ENDDT"]
                dataArray = self._dataArray(tmpSTARTDT, tmpENDDT)
                tempData = self._groupTalkData(self._getTalkData(tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID,
                                                                 tmpPROD_NBR, tmpCODE, dataArray))
                returnData = {"draw": 1,
                              "recordsTotal": len(tempData),
                              "recordsFiltered": len(tempData),
                              "data": tempData
                              }

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}


            elif tmpFUNCTYPE == "SELECT":
                tmpSTARTDT = self.jsonData["STARTDT"]
                tmpENDDT = self.jsonData["ENDDT"]

                dataArray = self._dataArray(tmpSTARTDT, tmpENDDT)

                tempData = self._groupTalkData(self._getTalkData(tmpCOMPANY_CODE, tmpSITE, tmpFACTORY_ID,
                                                                 tmpPROD_NBR, tmpCODE, dataArray))

                returnData = {"draw": 1,
                              "recordsTotal": len(tempData),
                              "recordsFiltered": len(tempData),
                              "data": tempData
                              }

                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            else:
                return {'Result': 'Fail', 'Reason': 'Parametes[FUNCTYPE] not in Rule'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

        except Exception as e:
            self.closeMongoConncetion()
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


    def _getTalkData(self, COMPANY_CODE, SITE, FACTORY_ID, PROD_NBR, CODE, DATAARRAY):

        selectAggregate = []
        # select
        selectMatch = {
            "$match": {
                "COMPANY_CODE": COMPANY_CODE,
                "SITE": SITE,
                "FACTORY_ID": FACTORY_ID,
                "ACCT_DATE": {"$in": DATAARRAY},
                "PROD_NBR": PROD_NBR,
                "CODE": CODE
            }
        }
        selectProject = {
            "$project": {
                "_id": 0
            }
        }
        selectSort = {
            "$sort": {
                "ACCT_DATE": -1
            }
        }

        selectAggregate.extend([selectMatch, selectProject, selectSort])

        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("intTalk")
            rData = self.aggregate(selectAggregate)
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

    def _groupTalkData(self, Data):
        reData = []
        for d in Data:
            d["AA"] = d["AA"] if "AA" in d else ""
            d["COMM"] = d["COMM"] if "COMM" in d else ""
            d["RATE"] = d["RATE"] if "RATE" in d else ""
            d["TOTAL"] = d["TOTAL"] if "TOTAL" in d else ""
            reData.append(d)
        return reData

    def _dataArray(self, sd, ed):
        d = datetime.datetime
        _sd = d.strptime(sd, '%Y%m%d')
        _ed = d.strptime(ed, '%Y%m%d')

        dataArray = []
        _ed = _ed + datetime.timedelta(days=1)
        d = datetime.datetime
        for i in range(int((_ed - _sd).days)):
            x = _sd + datetime.timedelta(i)
            dataArray.append(d.strftime(x, '%Y%m%d'))
        return dataArray

    def _getDRDATA_FPY(self, OPER, PROD_NBR, DEFECTCODE, ACCT_DATE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        
        getFabData = self.operSetData[tmpFACTORY_ID]
        numeratorData = getFabData["FPY"]["numerator"][OPER] 
        denominatorValue = getFabData["FPY"]["denominator"][OPER]

        DEFECT_Aggregate = [
            {
                "$match": {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": ACCT_DATE,
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
                                        "ACCT_DATE": ACCT_DATE,                                       
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
            DEFECT_Aggregate[0]["$match"]["DFCT_CODE"] = DEFECTCODE
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            rData = self.aggregate(DEFECT_Aggregate)
            self.closeMongoConncetion()

            magerData = [] 
            for d in rData:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)

            if len(magerData) == 0:
                return None
            else:
                returnData = {
                    "RATE" : str(round(magerData[0]["DEFECT_YIELD"]*100,2)),
                    "TOTAL" : str(magerData[0]["passQty"])
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

    def _getDRDATA_EFA(self, OPER, PROD_NBR, DEFECTCODE, ACCT_DATE):
        tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
        tmpSITE = self.jsonData["SITE"]
        tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
        
        numeratorData = OPER
        denominatorValue = OPER

        DEFECT_Aggregate = [
            {
                "$match": {
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "ACCT_DATE": ACCT_DATE,
                    "PROD_NBR": PROD_NBR,
                    "LCM_OWNER": {"$in": ["LCM0", "LCME", "PROD", "QTAP", "RES0"]},
                    "MAIN_WC": numeratorData
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
                                        "ACCT_DATE": ACCT_DATE,                                       
                                        "PROD_NBR": PROD_NBR,
                                        "MAIN_WC": denominatorValue
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
            DEFECT_Aggregate[0]["$match"]["DFCT_CODE"] = DEFECTCODE
        try:
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("deftHisAndCurrent")
            rData = self.aggregate(DEFECT_Aggregate)
            self.closeMongoConncetion()

            magerData = [] 
            for d in rData:   
                d["DEFECT_YIELD"] = round(d["DEFECT_YIELD"], 4) if "DEFECT_YIELD" in d else 0    
                magerData.append(d)

            if len(magerData) == 0:
                return None
            else:
                returnData = {
                    "RATE" : str(round(magerData[0]["DEFECT_YIELD"]*100,2)),
                    "TOTAL" : str(magerData[0]["passQty"])
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


