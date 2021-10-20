# -*- coding: utf-8 -*-
import json
import operator
import sys
import traceback
import time
import datetime
import copy
import gc
from re import X
from BaseType import BaseType
from Dao import DaoHelper, ReadConfig
from decimal import Decimal, ROUND_HALF_UP


class INTRelation(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(
            f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
        #INT_ORACLEDB_PROD / INT_ORACLEDB_TEST
        self.DBconfig = "INT_ORACLEDB_TEST"
        self.BASE_GROUPList = []

    def getData(self):
        try:
            self.writeLog(
                f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Input Json:{self.jsonData}')
            bottomLine = "_"
            redisKey = ""
            tmp = []

            className = f"{self.__class__.__name__}"
            tmpFuncType = self.jsonData["FUNCTYPE"]
            tmpCOMPANY_CODE = self.jsonData["COMPANY_CODE"]
            tmpSITE = self.jsonData["SITE"]
            tmpFACTORY_ID = self.jsonData["FACTORY_ID"]
            tmpAPPLICATION = self.jsonData["APPLICATION"]
            tmpACCT_DATE = self.jsonData["ACCT_DATE"]
            tmpPROD_NBR = self.jsonData["PROD_NBR"]
            tmpOPER = self.jsonData["OPER"]  # or RESPTYPE
            # Defect or Reason Code
            tmpCHECKCODE = self.jsonData["CHECKCODE"]
            expirSecond = 3600

            # redisKey
            tmp.append(className)
            tmp.append(tmpCOMPANY_CODE)
            tmp.append(tmpSITE)
            tmp.append(tmpFACTORY_ID)
            tmp.append(tmpAPPLICATION)
            tmp.append(tmpFuncType)
            tmp.append(tmpACCT_DATE)
            tmp.append(tmpPROD_NBR)
            tmp.append(tmpOPER)
            tmp.append(tmpCHECKCODE)
            redisKey = bottomLine.join(tmp)

            if tmpFACTORY_ID not in ["J001"]:
                return {'Result': 'NG', 'Reason': f'{tmpFACTORY_ID} not in FactoryID MAP'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            """
            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type', "Access-Control-Expose-Headers": "Expires,DataSource", "Expires": time.mktime((datetime.datetime.now() + datetime.timedelta(seconds=expirSecond)).timetuple()), "DataSource": "Redis"}
            """
            if tmpFuncType == "FPY_TEST":
                nodes = [
                    {"id": "0", "name": "TA_20001704", "symbolSize": 22.5,
                        "symbol": "circle", "value": 3, "category": 0},
                    {"id": "1", "name": "TA_20007668", "symbolSize": 13.75,
                        "symbol": "circle", "value": 4, "category": 0},
                    {"id": "2", "name": "09/07_21時1415", "symbolSize": 25,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "3", "name": "09/07_22時1415", "symbolSize": 25,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "4", "name": "09/07_23時1415", "symbolSize": 25,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "5", "name": "09/08_02時1415", "symbolSize": 25,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "6", "name": "09/08_02時1420", "symbolSize": 25,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "7", "name": "09/08_03時1420", "symbolSize": 12.5,
                        "symbol": "triangle", "value": 0, "category": 1},
                    {"id": "9", "name": "ASSY3606", "symbolSize": 22.5,
                        "symbol": "rect", "value": 3, "category": 2},
                    {"id": "10", "name": "AAFC3144", "symbolSize": 13.75,
                        "symbol": "rect", "value": 4, "category": 2},
                    {"id": "11", "name": "1415_Assy(B/L外購)", "symbolSize": 45,
                     "symbol": "pin", "value": 4, "category": 3},
                    {"id": "12", "name": "1420_AAFC", "symbolSize": 27.5,
                        "symbol": "pin", "value": 4, "category": 3}
                ]

                links = [
                    {"source": "0", "target": "9", "value": 3},
                    {"source": "1", "target": "10", "value": 4},
                    {"source": "2", "target": "0", "value": 1},
                    {"source": "2", "target": "9", "value": 1},
                    {"source": "3", "target": "0", "value": 1},
                    {"source": "3", "target": "9", "value": 1},
                    {"source": "4", "target": "0", "value": 1},
                    {"source": "4", "target": "9", "value": 1},
                    {"source": "6", "target": "1", "value": 2},
                    {"source": "6", "target": "10", "value": 2},
                    {"source": "7", "target": "1", "value": 1},
                    {"source": "8", "target": "1", "value": 1},
                    {"source": "8", "target": "10", "value": 1},
                    {"source": "9", "target": "11", "value": 3},
                    {"source": "10", "target": "12", "value": 4},
                    {"source": "11", "target": "0", "value": 3},
                    {"source": "12", "target": "1", "value": 4}
                ]
                categories = [
                    {
                        "name": "人員",
                        "itemStyle": {
                            "color": "#3F70BF"
                        }
                    },
                    {
                        "name": "分時",
                        "itemStyle": {
                            "color": "#F75356"
                        }
                    },
                    {
                        "name": "機台",
                        "itemStyle": {
                            "color": "#70BF3F"
                        }
                    },
                    {
                        "name": "站點",
                        "itemStyle": {
                            "color": "#F9CE24"
                        }
                    },
                    {
                        "name": "物料",
                        "itemStyle": {
                            "color": "#E561C6"
                        }
                    }
                ]

                returnData = {
                    "RELATIONTYPE": tmpFuncType,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": "0908",
                    "PROD_NBR": "GZJ133IA0010S",
                    "OPER": "AAFC",
                    "C_CODE": "PCU16",
                    "C_DESCR": "周邊框線",
                    "nodes": nodes,
                    "links": links,
                    "categories": categories
                }
                """
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                """
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            elif tmpFuncType == "REASON_PROD":
                # region 準備數據
                # comm data: 權種數據
                whereString = f" REASONCODE = '{tmpCHECKCODE}' "
                sql = "select REASONCODE, COMPARECODE, WEIGHT from INTMP_DB.REASON_WEIGHT " \
                      f"where {whereString} " \
                      "order by REASONCODE, COMPARECODE "
                self.getConnection(self.DBconfig)
                commData = self.Select(sql)
                self.closeConnection()
                weightData = {}
                if(len(commData) != 0):
                    for da in commData:
                        weightData[da[1]] = float(da[2])
                del commData
                gc.collect()

                # step0: 取得 與 defect / Reason 相關的 panel id
                whereString = f"where PROD_NBR = '{tmpPROD_NBR}' and  DEFT_REASON = '{tmpCHECKCODE}' "\
                    f" and MAIN_OPER = '{tmpOPER}'  and MFGDATE = '{tmpACCT_DATE}' "
                sql = "select PROD_NBR, DEFT_REASON, MFGDATE, MAIN_OPER, PANELID, RW_COUNT "\
                      " from INTMP_DB.PANELHISDAILY_REASON " \
                      f"{whereString} " \
                      "group by PROD_NBR, DEFT_REASON, MFGDATE, MAIN_OPER, PANELID, RW_COUNT " \
                      "order by PANELID "
                self.getConnection(self.DBconfig)
                idData = self.Select(sql)
                self.closeConnection()
                panelData = []
                if(len(idData) != 0):
                    for da in idData:
                        datadict = {
                            "PROD_NBR": da[0],
                            "DEFT_REASON": da[1],
                            "MFGDATE": da[2],
                            "MAIN_OPER": da[3],
                            "PANELID": da[4],
                            "RW_COUNT": da[5]
                        }
                        panelData.append(datadict)
                del idData
                gc.collect()

                PANELID_Group = self._Group_PANELID_ListbyReason(panelData)

                PANELID_Group_SQL_LIST = ""
                for x in PANELID_Group:
                    PANELID_Group_SQL_LIST = PANELID_Group_SQL_LIST + f"'{x}',"
                if PANELID_Group_SQL_LIST != "":
                    PANELID_Group_SQL_LIST = PANELID_Group_SQL_LIST[:-1]

                # step1: 取得 panel his
                whereString = f"where PROD_NBR = '{tmpPROD_NBR}' and MFGDATE = '{tmpACCT_DATE}' and PANELID in ({PANELID_Group_SQL_LIST}) "
                sql = f"with panel_his_daily as (select * from INTMP_DB.PANELHISDAILY {whereString} ) " \
                      "select PROD_NBR, MFGDATE, PANELID, OPER, TRANSDT, OPERATOR, EQPID, RW_COUNT, " \
                      "OUTPUT_FG from panel_his_daily order by PANELID, TRANSDT asc"

                self.getConnection(self.DBconfig)
                data1 = self.Select(sql)
                self.closeConnection()
                hisData = []
                if(len(data1) != 0):
                    for da in data1:
                        d = datetime.datetime
                        TIMECLUST_d = d.strptime(da[4], '%Y%m%d%H%M%S')
                        TIMECLUST = d.strftime(TIMECLUST_d, '%Y%m%d%H')
                        datadict = {
                            "PROD_NBR": da[0],
                            "MFGDATE": da[1],
                            "PANELID": da[2],
                            "OPER": da[3],
                            "TRANSDT": da[4],
                            "OPERATOR": da[5],
                            "EQPID": da[6],
                            "RW_COUNT": da[7],
                            "OUTPUT_FG": da[8],
                            "TIMECLUST": TIMECLUST
                        }
                        hisData.append(datadict)
                del data1
                gc.collect()

                # step2: 取得panel use mat
                whereString = f" PROD_NBR = '{tmpPROD_NBR}' and MFGDATE = '{tmpACCT_DATE}' and OPER = '1050' and PANELID in ({PANELID_Group_SQL_LIST}) "
                sql = f"with panel_his_mat as (select * from INTMP_DB.PANELHISDAILY_MAT where {whereString}) " \
                      "select PROD_NBR, MFGDATE, PANELID, OPER, MAT_ID, MAT_LOTID from panel_his_mat " \
                      "order by MAT_ID, MAT_LOTID asc"

                self.getConnection(self.DBconfig)
                data2 = self.Select(sql)
                self.closeConnection()
                matData = []
                if(len(data2) != 0):
                    for da in data2:
                        datadict = {
                            "PROD_NBR": da[0],
                            "MFGDATE": da[1],
                            "PANELID": da[2],
                            "OPER": da[3],
                            "MAT_ID": da[4],
                            "MAT_LOTID": da[5]
                        }
                        matData.append(datadict)
                del data2
                gc.collect()
                # endregion

                # temp list
                # 分群
                self.BASE_GROUPList = self._Group_OPERATOR_OPER_EQPID_TIMECLUST_PANELID_List(
                    hisData)
                PANEL_TOTAL_COUNT = len(PANELID_Group)

                # 人
                node_cal_OPERATOR_OPER = []
                link_cal_OPERATOR_OPER = []
                OPERATOR_OPER_PANELID_Group = self._Group_OPERATOR_OPER_PANELID_List()
                OPERATOR_OPER_EQPID_PANELID_Group = self._Group_OPERATOR_OPER_EQPID_PANELID_List()
                notInOPER1 = ["1050", "1100", "1200", "2110"]
                OPERATOR_OPER_Count = self._Count_OPERATOR_OPER_List(
                    notInOPER1, OPERATOR_OPER_PANELID_Group)
                OPERATOR_OPER_EQPID_Count = self._Count_OPERATOR_OPER_EQPID_List(
                    notInOPER1, OPERATOR_OPER_EQPID_PANELID_Group)
                OPER_Count = self._Count_OPER_List(
                    notInOPER1, OPERATOR_OPER_Count)
                o_A_Limit = self._OPER_Limit(
                    OPER_Count, PANEL_TOTAL_COUNT)
                o_T_Limit = 0.3
                node_cal_OPERATOR_OPER = self._calNode_OPERATOR_OPER(
                    OPERATOR_OPER_Count, PANEL_TOTAL_COUNT, o_A_Limit, o_T_Limit, weightData)
                link_cal_OPERATOR_OPER = self._calLink_OPERATOR_OPER(
                    node_cal_OPERATOR_OPER, OPERATOR_OPER_EQPID_Count)

                node_cal_OPERATOR_TIMECLUST = []
                link_cal_OPERATOR_TIMECLUST = []
                node_cal_EQPID_TIMECLUST = []
                link_cal_EQPID_TIMECLUST = []
                if PANEL_TOTAL_COUNT > 10:  # 沒大於10片 不計算分時
                    # 人時
                    OPERATOR_OPER_TIMECLUST_PANELID_Group = self._Group_OPERATOR_OPER_TIMECLUST_PANELID_List()
                    notInOPER2 = ["1050", "1100", "1200", "2110"]
                    OPERATOR_OPER_TIMECLUST_Count = self._Count_OPERATOR_OPER_TIMECLUST_List(
                        notInOPER2, OPERATOR_OPER_TIMECLUST_PANELID_Group)
                    node_cal_OPERATOR_TIMECLUST = self._calNode_OPERATOR_TIMECLUSTR(
                        OPERATOR_OPER_TIMECLUST_Count, PANEL_TOTAL_COUNT)
                    link_cal_OPERATOR_TIMECLUST = self._calLink_OPERATOR_TIMECLUSTR(
                        node_cal_OPERATOR_TIMECLUST)
                # 機時
                    EQPID_OPER_TIMECLUST_PANELID_Group = self._Group_EQPID_OPER_TIMECLUST_PANELID_List()
                    notInOPER3 = ["1050", "1100", "1200", "2110"]
                    EQPID_OPER_TIMECLUST_Count = self._Count_EQPID_OPER_TIMECLUST_List(
                        notInOPER3, EQPID_OPER_TIMECLUST_PANELID_Group)
                    node_cal_EQPID_TIMECLUST = self._calNode_EQPID_TIMECLUSTR(
                        EQPID_OPER_TIMECLUST_Count, PANEL_TOTAL_COUNT)
                    link_cal_EQPID_TIMECLUST = self._calLink_EQPID_TIMECLUSTR(
                        node_cal_EQPID_TIMECLUST)

                # 機
                node_cal_EQPID_OPER = []
                link_cal_EQPID_OPER = []
                EQPID_OPER_PANELID_Group = self._Group_EQPID_OPER_PANELID_List()
                notInOPER4 = ["1050", "1100", "2110"]
                EQPID_OPER_Count = self._Count_EQPID_OPER_List(
                    notInOPER4, EQPID_OPER_PANELID_Group)
                OPER_Count = self._Count_OPER_List(
                    notInOPER4, EQPID_OPER_Count)
                g_A_Limit = self._OPER_Limit(
                    OPER_Count, PANEL_TOTAL_COUNT)
                g_T_Limit = 0.3
                node_cal_EQPID_OPER = self._calNode_EQPID_OPER(
                    EQPID_OPER_Count, PANEL_TOTAL_COUNT, g_A_Limit, g_T_Limit, weightData)
                link_cal_EQPID_OPER = self._calLink_EQPID_OPER(
                    node_cal_EQPID_OPER)

                # 站
                node_cal_OPER_OPERATOR = []
                link_cal_OPER_OPERATOR = []
                notInOPER5 = ["1050", "1100", "2110"]
                OPER_OPERATOR_Count = self._Count_OPERATOR_OPER_List(
                    notInOPER5, OPERATOR_OPER_PANELID_Group)
                OPER_Count = self._Count_OPER_List(
                    notInOPER5, OPERATOR_OPER_Count)
                node_cal_OPER_OPERATOR = self._calNode_OPER_OPERATOR(
                    OPER_OPERATOR_Count, PANEL_TOTAL_COUNT, o_A_Limit, o_T_Limit, weightData)
                link_cal_OPER_OPERATOR = self._calLink_OPER_OPERATOR(
                    node_cal_OPER_OPERATOR)

                # 料
                node_cal_MAT_OPER = []
                link_cal_MAT_OPER = []
                MAT_OPER_PANELID_Group = self._Group_MAT_OPER_PANELID_List(
                    matData)
                MAT_OPER_Count = self._Count_MAT_OPER_List(
                    MAT_OPER_PANELID_Group)
                m_A_Limit = 0.6
                m_T_Limit = 0.6
                node_cal_MAT_OPER = self._calNode_MAT_OPER(
                    MAT_OPER_Count, PANEL_TOTAL_COUNT, m_A_Limit, m_T_Limit, weightData)
                link_cal_MAT_OPER = self._calLink_MAT_OPER(node_cal_MAT_OPER)

                # 資料聚合
                nodes = self._grouptNodes(
                    PANEL_TOTAL_COUNT,
                    node_cal_OPERATOR_OPER,
                    node_cal_OPERATOR_TIMECLUST,
                    node_cal_EQPID_TIMECLUST,
                    node_cal_EQPID_OPER,
                    node_cal_OPER_OPERATOR,
                    node_cal_MAT_OPER
                )

                links = self._grouptLinks(
                    nodes,
                    link_cal_OPERATOR_OPER,
                    link_cal_OPERATOR_TIMECLUST,
                    link_cal_EQPID_TIMECLUST,
                    link_cal_EQPID_OPER,
                    link_cal_OPER_OPERATOR,
                    link_cal_MAT_OPER
                )

                categories = self._categories()

                C_DESC = self._code2Desc("REASONCODE",tmpCHECKCODE)
                returnData = {
                    "RELATIONTYPE": tmpFuncType,
                    "COMPANY_CODE": tmpCOMPANY_CODE,
                    "SITE": tmpSITE,
                    "FACTORY_ID": tmpFACTORY_ID,
                    "APPLICATION": tmpAPPLICATION,
                    "ACCT_DATE": datetime.datetime.strptime(tmpACCT_DATE, '%Y%m%d').strftime('%Y-%m-%d'),
                    "PROD_NBR": tmpPROD_NBR,
                    "OPER": tmpOPER,
                    "C_CODE": tmpCHECKCODE,
                    "C_DESCR": C_DESC if C_DESC != None else tmpCHECKCODE,
                    "nodes": nodes,
                    "links": links,
                    "categories": categories
                }
                """
                self.getRedisConnection()
                if self.searchRedisKeys(redisKey):     
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                else:
                    self.setRedisData(redisKey, json.dumps(
                        returnData, sort_keys=True, indent=2), expirSecond)
                """
                return returnData, 200, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

            else:
                self.closeConnection()
                return {'Result': 'Fail', 'Reason': 'Parametes[KPITYPE] not in Rule'}, 400, {"Content-Type": "application/json", 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'x-requested-with,content-type'}

        except Exception as e:
            self.closeConnection()
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

    def _filterListbyOPER(self, LIST, OPER):
        d = list(filter(lambda d: d["OPER"] != OPER, LIST))
        return d

    def _code2Desc(self, TYPE, C_CODE):
        returnString = C_CODE
        if TYPE == "REASONCODE":
            sql = f"select REASONCODE_DESC from INTMP_DB.REASONCODE where REASONCODE = '{C_CODE}'"
            # INT_ORACLEDB_PROD
            self.getConnection(self.DBconfig)
            data = self.Select(sql)
            self.closeConnection()
            returnString = None
            if(len(data) != 0):
                returnString = data[0][0]
        elif TYPE == "MAT4":
            sql = f"select MAT_DESC from INTMP_DB.MAT where MAT4 = '{C_CODE}'"
            # INT_ORACLEDB_PROD
            self.getConnection(self.DBconfig)
            data = self.Select(sql)
            self.closeConnection()
            returnString = None
            if(len(data) != 0):
                returnString = data[0][0]        
        return returnString

    def _Group_PANELID_List(self):
        PANELIDList = []
        for x in self.BASE_GROUPList:
            if x["PANELID"] not in PANELIDList:
                PANELIDList.append(x["PANELID"])
        return PANELIDList

    def _Group_PANELID_ListbyReason(self, PANEL_List):
        PANELIDList = []
        for x in PANEL_List:
            if x["PANELID"] not in PANELIDList:
                PANELIDList.append(x["PANELID"])
        return PANELIDList

    def _Group_OPERATOR_OPER_EQPID_TIMECLUST_PANELID_List(self, DATA):
        List = []
        for x in DATA:
            if {"OPERATOR": x["OPERATOR"], "OPER": x["OPER"],
                    "EQPID": x["EQPID"], "TIMECLUST": x["TIMECLUST"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "OPERATOR": x["OPERATOR"],
                    "OPER": x["OPER"],
                    "EQPID": x["EQPID"],
                    "TIMECLUST": x["TIMECLUST"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_MAT_OPER_PANELID_List(self, DATA):
        List = []
        for x in DATA:
            if {"OPER": x["OPER"], "MAT_ID": x["MAT_ID"], "MAT_LOTID": x["MAT_LOTID"],
                    "PANELID": x["PANELID"]} not in List:
                data = {
                    "OPER": x["OPER"],
                    "MAT_ID": x["MAT_ID"],
                    "MAT_LOTID": x["MAT_LOTID"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_OPERATOR_OPER_TIMECLUST_PANELID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"OPERATOR": x["OPERATOR"], "OPER": x["OPER"],
                    "TIMECLUST": x["TIMECLUST"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "OPERATOR": x["OPERATOR"],
                    "OPER": x["OPER"],
                    "TIMECLUST": x["TIMECLUST"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_EQPID_OPER_TIMECLUST_PANELID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"EQPID": x["EQPID"], "OPER": x["OPER"],
                    "TIMECLUST": x["TIMECLUST"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "EQPID": x["EQPID"],
                    "OPER": x["OPER"],
                    "TIMECLUST": x["TIMECLUST"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_OPERATOR_OPER_EQPID_PANELID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"OPERATOR": x["OPERATOR"], "OPER": x["OPER"],
                    "EQPID": x["EQPID"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "OPERATOR": x["OPERATOR"],
                    "OPER": x["OPER"],
                    "EQPID": x["EQPID"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_OPERATOR_OPER_PANELID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"OPERATOR": x["OPERATOR"], "OPER": x["OPER"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "OPERATOR": x["OPERATOR"],
                    "OPER": x["OPER"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_EQPID_OPER_PANELID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"EQPID": x["EQPID"], "OPER": x["OPER"], "PANELID": x["PANELID"]} not in List:
                data = {
                    "EQPID": x["EQPID"],
                    "OPER": x["OPER"],
                    "PANELID": x["PANELID"]
                }
                List.append(data)
        return List

    def _Group_OPERATOR_OPER_EQPID_List(self):
        List = []
        for x in self.BASE_GROUPList:
            if {"OPERATOR": x["OPERATOR"], "OPER": x["OPER"], "EQPID": x["EQPID"]} not in List:
                data = {
                    "OPERATOR": x["OPERATOR"],
                    "OPER": x["OPER"],
                    "EQPID": x["EQPID"]
                }
                List.append(data)
        return List

    def _Count_OPERATOR_OPER_TIMECLUST_List(self, notInOPER, DATA):
        List = []
        for x in DATA:
            if x["OPER"] not in notInOPER:
                d = list(filter(lambda d: d["OPERATOR"] == x["OPERATOR"] and
                                d["OPER"] == x["OPER"] and d["TIMECLUST"] == x["TIMECLUST"], List))
                if d == []:
                    data = {
                        "OPERATOR": x["OPERATOR"],
                        "OPER": x["OPER"],
                        "TIMECLUST": x["TIMECLUST"],
                        "PANELID_COUNT": 1
                    }
                    List.append(data)
                else:
                    for cx in List:
                        if (cx["OPERATOR"] == x["OPERATOR"] and cx["OPER"] == x["OPER"]
                                and cx["TIMECLUST"] == x["TIMECLUST"]):
                            cx["PANELID_COUNT"] += 1
        return List

    def _Count_EQPID_OPER_TIMECLUST_List(self, notInOPER, DATA):
        List = []
        for x in DATA:
            if x["OPER"] not in notInOPER:
                d = list(filter(lambda d: d["EQPID"] == x["EQPID"] and
                                d["OPER"] == x["OPER"] and d["TIMECLUST"] == x["TIMECLUST"], List))
                if d == []:
                    data = {
                        "EQPID": x["EQPID"],
                        "OPER": x["OPER"],
                        "TIMECLUST": x["TIMECLUST"],
                        "PANELID_COUNT": 1
                    }
                    List.append(data)
                else:
                    for cx in List:
                        if (cx["EQPID"] == x["EQPID"] and cx["OPER"] == x["OPER"]
                                and cx["TIMECLUST"] == x["TIMECLUST"]):
                            cx["PANELID_COUNT"] += 1
        return List

    def _Count_OPERATOR_OPER_EQPID_List(self, notInOPER, DATA):
        List = []
        for x in DATA:
            if x["OPER"] not in notInOPER:
                d = list(filter(lambda d: d["OPERATOR"] == x["OPERATOR"] and
                                d["OPER"] == x["OPER"] and d["EQPID"] == x["EQPID"], List))
                if d == []:
                    data = {
                        "OPERATOR": x["OPERATOR"],
                        "OPER": x["OPER"],
                        "EQPID": x["EQPID"],
                        "PANELID_COUNT": 1
                    }
                    List.append(data)
                else:
                    for cx in List:
                        if (cx["OPERATOR"] == x["OPERATOR"] and cx["OPER"] == x["OPER"]
                                and cx["EQPID"] == x["EQPID"]):
                            cx["PANELID_COUNT"] += 1
        return List

    def _Count_OPERATOR_OPER_List(self, notInOPER, DATA):
        List = []
        for x in DATA:
            if x["OPER"] not in notInOPER:
                d = list(filter(
                    lambda d: d["OPERATOR"] == x["OPERATOR"] and d["OPER"] == x["OPER"], List))
                if d == []:
                    data = {
                        "OPERATOR": x["OPERATOR"],
                        "OPER": x["OPER"],
                        "PANELID_COUNT": 1
                    }
                    List.append(data)
                else:
                    for cx in List:
                        if (cx["OPERATOR"] == x["OPERATOR"] and cx["OPER"] == x["OPER"]):
                            cx["PANELID_COUNT"] += 1
        return List

    def _Count_EQPID_OPER_List(self, notInOPER, DATA):
        List = []
        for x in DATA:
            if x["OPER"] not in notInOPER:
                d = list(
                    filter(lambda d: d["EQPID"] == x["EQPID"] and d["OPER"] == x["OPER"], List))
                if d == []:
                    data = {
                        "EQPID": x["EQPID"],
                        "OPER": x["OPER"],
                        "PANELID_COUNT": 1
                    }
                    List.append(data)
                else:
                    for cx in List:
                        if (cx["EQPID"] == x["EQPID"] and cx["OPER"] == x["OPER"]):
                            cx["PANELID_COUNT"] += 1
        return List

    def _Count_MAT_OPER_List(self, DATA):
        List = []
        for x in DATA:
            d = list(filter(lambda d: d["OPER"] == x["OPER"] and d["MAT_ID"] == x["MAT_ID"]
                            and d["MAT_LOTID"] == x["MAT_LOTID"], List))
            if d == []:
                data = {
                    "OPER": x["OPER"],
                    "MAT_ID": x["MAT_ID"],
                    "MAT_LOTID": x["MAT_LOTID"],
                    "PANELID_COUNT": 1
                }
                List.append(data)
            else:
                for cx in List:
                    if (cx["OPER"] == x["OPER"] and cx["MAT_ID"] == x["MAT_ID"]
                            and cx["MAT_LOTID"] == x["MAT_LOTID"]):
                        cx["PANELID_COUNT"] += 1
        return List

    # OPER 出現次數
    def _Count_OPER_List(self, notInOPER, DATA):
        List = {}
        for x in DATA:
            if x["OPER"] not in notInOPER:
                if x["OPER"] not in List:
                    List[x["OPER"]] = 1
                else:
                    List[x["OPER"]] += 1
        return List

    def _OPER_Limit(self, OPER_List, PANEL_TOTAL_COUNT):
        # (PANEL_TOTAL_COUNT*70%)/(MAX(B2:N2)*35%)/PANEL_TOTAL_COUNT
        OPER_List_MAX = max(OPER_List.values()) * 0.35
        cal = (PANEL_TOTAL_COUNT * 0.7) / OPER_List_MAX / PANEL_TOTAL_COUNT
        returnData = cal if cal < 0.5 else 0.5
        return returnData

    def _calNode_OPERATOR_OPER(self, OPERATOR_OPER, PANEL_TOTAL_COUNT, A_Limit, T_Limit, weightData):
        DATASERIES = []
        for oo in OPERATOR_OPER:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            # bRate=>RSC權重
            bRate = weightData.get(oo["OPER"], 0)
            # (A*B)權重計算
            tRate = round(aRate * bRate, 4)
            SymbolSize = round(tRate*oo["PANELID_COUNT"])
            if aRate >= A_Limit and tRate >= T_Limit:
                NAME = f'TA_{oo["OPERATOR"]}' 
                if oo["OPERATOR"] == "AUTO":
                    NAME = f'TA_{oo["OPERATOR"]}{oo["OPER"]}' 
                data = {
                    "NAME": NAME,
                    "OPERATOR": oo["OPERATOR"],
                    "OPER": oo["OPER"],
                    "PANELID_COUNT": oo["PANELID_COUNT"],
                    "A_Limit": A_Limit,
                    "T_Limit": T_Limit,
                    "aRate": aRate,
                    "bRate": bRate,
                    "tRate": tRate,
                    "SymbolSize": SymbolSize,
                    "value": oo["PANELID_COUNT"]
                }
                DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calLink_OPERATOR_OPER(self, node_cal_OPERATOR_OPER, OPERATOR_OPER_EQPID_Lis):
        DATASERIES = []
        for oo in node_cal_OPERATOR_OPER:
            d = list(filter(lambda d: d["OPERATOR"] == oo["OPERATOR"]
                     and d["OPER"] == oo["OPER"], OPERATOR_OPER_EQPID_Lis))
            if d != []:
                for dd in d:
                    data = {
                        "source": oo["NAME"],
                        "target": dd["EQPID"],
                        "value": dd["PANELID_COUNT"]
                    }
                    DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calNode_OPERATOR_TIMECLUSTR(self, OPERATOR_TIMECLUSTR, PANEL_TOTAL_COUNT):
        DATASERIES = []
        for oo in OPERATOR_TIMECLUSTR:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            SymbolSize = oo["PANELID_COUNT"]*2
            d = datetime.datetime
            TIMECLUST_d = d.strptime(oo["TIMECLUST"], '%Y%m%d%H')
            TIMECLUST = d.strftime(TIMECLUST_d, '%m/%d_%H')
            data = {
                "NAME": f'{TIMECLUST}時{oo["OPER"]}_人',
                "OPERATOR": f'{oo["OPERATOR"]}',
                "OPER": oo["OPER"],
                "TIMECLUST": TIMECLUST,
                "PANELID_COUNT": oo["PANELID_COUNT"],
                "PANEL_TOTAL_COUNT": PANEL_TOTAL_COUNT,
                "aRate": aRate,
                "SymbolSize": SymbolSize,
                "value": oo["PANELID_COUNT"]
            }
            DATASERIES.append(data)

        DATASERIES.sort(key=operator.itemgetter(
            "aRate", "aRate"), reverse=True)

        aRateList = []
        for x in DATASERIES:
            aRateList.append(f'{x.get("aRate")}')
        qq = sorted(aRateList, reverse=True)
        top3 = float(qq[2])
        top3Filter = list(filter(lambda d: d["aRate"] >= top3, DATASERIES))
        returnData = list(filter(lambda d: d["aRate"] >= 0.36, top3Filter))
        return returnData

    def _calLink_OPERATOR_TIMECLUSTR(self, node_cal_OPERATOR_TIMECLUSTR):
        DATASERIES = []
        for oo in node_cal_OPERATOR_TIMECLUSTR:
            target = f'TA_{oo["OPERATOR"]}' 
            if oo["OPERATOR"] == "AUTO":
                target = f'TA_{oo["OPERATOR"]}{oo["OPER"]}' 
            data = {
                "source": oo["NAME"],
                "target": target,
                "value": oo["value"]
            }
            DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calNode_EQPID_TIMECLUSTR(self, EQPID_TIMECLUSTR, PANEL_TOTAL_COUNT):
        DATASERIES = []
        for oo in EQPID_TIMECLUSTR:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            SymbolSize = oo["PANELID_COUNT"]*2
            d = datetime.datetime
            TIMECLUST_d = d.strptime(oo["TIMECLUST"], '%Y%m%d%H')
            TIMECLUST = d.strftime(TIMECLUST_d, '%m/%d_%H')
            data = {
                "NAME": f'{TIMECLUST}時{oo["OPER"]}',
                "EQPID": f'{oo["EQPID"]}',
                "OPER": oo["OPER"],
                "TIMECLUST": TIMECLUST,
                "PANELID_COUNT": oo["PANELID_COUNT"],
                "PANEL_TOTAL_COUNT": PANEL_TOTAL_COUNT,
                "aRate": aRate,
                "SymbolSize": SymbolSize,
                "value": oo["PANELID_COUNT"]
            }
            DATASERIES.append(data)

        DATASERIES.sort(key=operator.itemgetter(
            "aRate", "aRate"), reverse=True)

        aRateList = []
        for x in DATASERIES:
            aRateList.append(f'{x.get("aRate")}')
        qq = sorted(aRateList, reverse=True)
        top3 = float(qq[2])
        top3Filter = list(filter(lambda d: d["aRate"] >= top3, DATASERIES))
        returnData = list(filter(lambda d: d["aRate"] >= 0.36, top3Filter))
        return returnData

    def _calLink_EQPID_TIMECLUSTR(self, node_cal_EQPID_TIMECLUSTR):
        DATASERIES = []
        for oo in node_cal_EQPID_TIMECLUSTR:
            data = {
                "source": oo["NAME"],
                "target": oo["EQPID"],
                "value": oo["value"]
            }
            DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calNode_EQPID_OPER(self, EQPID_OPER, PANEL_TOTAL_COUNT, A_Limit, T_Limit, weightData):
        DATASERIES = []
        for oo in EQPID_OPER:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            # bRate=>RSC權重
            bRate = weightData.get(oo["OPER"], 0)
            # (A*B)權重計算
            tRate = round(aRate * bRate, 4)
            SymbolSize = round(tRate*oo["PANELID_COUNT"])
            if aRate >= A_Limit and tRate >= T_Limit:
                data = {
                    "NAME": f'{oo["EQPID"]}',
                    "EQPID": f'{oo["EQPID"]}',
                    "OPER": oo["OPER"],
                    "PANELID_COUNT": oo["PANELID_COUNT"],
                    "A_Limit": A_Limit,
                    "T_Limit": T_Limit,
                    "aRate": aRate,
                    "bRate": bRate,
                    "tRate": tRate,
                    "SymbolSize": SymbolSize,
                    "value": oo["PANELID_COUNT"]
                }
                DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calLink_EQPID_OPER(self, node_cal_EQPID_OPER):
        DATASERIES = []
        for oo in node_cal_EQPID_OPER:
            data = {
                "source": oo["NAME"],
                "target": oo["OPER"],
                "value": oo["value"]
            }
            DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calNode_OPER_OPERATOR(self, OPER_OPERATOR, PANEL_TOTAL_COUNT, A_Limit, T_Limit, weightData):
        DATASERIES = []
        for oo in OPER_OPERATOR:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            # bRate=>RSC權重
            bRate = weightData.get(oo["OPER"], 0)
            # (A*B)權重計算
            tRate = round(aRate * bRate, 4)
            SymbolSize = round(tRate*oo["PANELID_COUNT"])
            if aRate >= A_Limit and tRate >= T_Limit:
                data = {
                    "NAME": f'{oo["OPER"]}',
                    "OPERATOR": f'{oo["OPERATOR"]}',
                    "OPER": oo["OPER"],
                    "PANELID_COUNT": oo["PANELID_COUNT"],
                    "A_Limit": A_Limit,
                    "T_Limit": T_Limit,
                    "aRate": aRate,
                    "bRate": bRate,
                    "tRate": tRate,
                    "SymbolSize": SymbolSize,
                    "value": oo["PANELID_COUNT"]
                }
                DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calLink_OPER_OPERATOR(self, node_cal_OPER_OPERATOR):
        DATASERIES = []
        for oo in node_cal_OPER_OPERATOR:
            target = f'TA_{oo["OPERATOR"]}' 
            if oo["OPERATOR"] == "AUTO":
                target = f'TA_{oo["OPERATOR"]}{oo["OPER"]}' 
            data = {
                "source": oo["NAME"],
                "target":target,
                "value": oo["value"]
            }
            DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calNode_MAT_OPER(self, MAT_OPER, PANEL_TOTAL_COUNT, A_Limit, T_Limit, weightData):
        DATASERIES = []
        for oo in MAT_OPER:
            # aRate=>Pcs/All不良占%
            aRate = oo["PANELID_COUNT"] / PANEL_TOTAL_COUNT
            # bRate=>RSC權重
            mat4 = oo["MAT_ID"][0:4]
            mat4_DESC = self._code2Desc("MAT4",mat4)
            bRate = weightData.get(mat4, 0)
            # (A*B)權重計算
            tRate = round(aRate * bRate, 4)
            SymbolSize = round(tRate*oo["PANELID_COUNT"])
            if aRate >= A_Limit and tRate >= T_Limit:
                data = {
                    "NAME": f'{oo["MAT_LOTID"]}_{mat4_DESC}',
                    "OPER": oo["OPER"],
                    "MAT4": mat4,
                    "MAT4_DESC": mat4_DESC,
                    "MAT_ID": oo["MAT_ID"],
                    "MAT_LOTID": oo["MAT_LOTID"],
                    "PANELID_COUNT": oo["PANELID_COUNT"],
                    "A_Limit": A_Limit,
                    "T_Limit": T_Limit,
                    "aRate": aRate,
                    "bRate": bRate,
                    "tRate": tRate,
                    "SymbolSize": SymbolSize,
                    "value": oo["PANELID_COUNT"]
                }
                DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _calLink_MAT_OPER(self, node_cal_MAT_OPER):
        DATASERIES = []
        for oo in node_cal_MAT_OPER:
            data = {
                "source": oo["NAME"],
                "target": oo["OPER"],
                "value": oo["value"]
            }
            DATASERIES.append(data)
        returnData = DATASERIES
        return returnData

    def _grouptNodes(self, PANEL_TOTAL_COUNT, n1d, n2d, n3d, n4d, n5d, n6d):
        #{"id": "1", "name": "TA_AUTO_1301", "symbolSize": 10.4, "symbol": "circle", "value": 13, "category": 0 },
        magerData = []
        for d in n1d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "circle",
                "value": d["value"],
                "category": 0
            }
            magerData.append(oData)
        for d in n2d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "triangle",
                "value": d["value"],
                "category": 1
            }
            magerData.append(oData)
        for d in n3d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "triangle",
                "value": d["value"],
                "category": 1
            }
            magerData.append(oData)
        for d in n4d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "rect",
                "value": d["value"],
                "category": 2
            }
            magerData.append(oData)
        for d in n5d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "pin",
                "value": d["value"],
                "category": 3
            }
            magerData.append(oData)
        for d in n6d:
            oData = {
                "id": "0",
                "name": d["NAME"],
                "symbolSize": d["SymbolSize"],
                "symbol": "roundRect",
                "value": d["value"],
                "category": 4
            }
            magerData.append(oData)

        # symbolSize resize
        if(PANEL_TOTAL_COUNT >= 25 and magerData != []):
            magerData.sort(key=operator.itemgetter(
                "symbolSize", "symbolSize"), reverse=True)
            maxSymbolSize = magerData[0]["symbolSize"]
            for x in magerData:
                weight = 25/maxSymbolSize
                x["symbolSize"] = round(x["symbolSize"]*weight, 4)        
        else:
            multiple = round(40 / PANEL_TOTAL_COUNT, 4)
            for x in magerData:
                x["symbolSize"] = round(x["symbolSize"]*multiple, 4)

        magerData.sort(key=operator.itemgetter("name", "name"))
        magerData.sort(key=operator.itemgetter("category", "category"))

        idCount = 1
        for x in magerData:
            x["id"] = f"{idCount}"
            idCount += 1

        return magerData

    def _grouptLinks(self, nodes, n1d, n2d, n3d, n4d, n5d, n6d):
        #{ "source": "2", "target": "9", "value":21 },
        magerData = []
        for d in n1d:
            magerData.append(self._getLinkData(nodes, d))
        for d in n2d:
            magerData.append(self._getLinkData(nodes, d))
        for d in n3d:
            magerData.append(self._getLinkData(nodes, d))
        for d in n4d:
            magerData.append(self._getLinkData(nodes, d))
        for d in n5d:
            magerData.append(self._getLinkData(nodes, d))
        for d in n6d:
            magerData.append(self._getLinkData(nodes, d))

        returnData = d = list(
            filter(lambda d: d["source"] != "0" and d["target"] != "0", magerData))

        return returnData

    def _getLinkData(self, nodes, data):
        oData = {
            "source": self._getIDbyNodeName(nodes, data["source"]),
            "target": self._getIDbyNodeName(nodes, data["target"]),
            "value": data["value"]
        }
        return oData

    def _getIDbyNodeName(self, nodes, NodeName):
        d = list(filter(lambda d: d["name"] == NodeName, nodes))
        returnData = 0
        if d != []:
            returnData = d[0]["id"]
        return f'{returnData}'

    def _categories(self):
        returnData = [
            {
                "name": "人員",
                "itemStyle": {
                        "color": "#3F70BF"
                }
            },
            {
                "name": "分時",
                "itemStyle": {
                        "color": "#F75356"
                }
            },
            {
                "name": "機台",
                "itemStyle": {
                        "color": "#70BF3F"
                }
            },
            {
                "name": "站點",
                "itemStyle": {
                        "color": "#F9CE24"
                }
            },
            {
                "name": "物料",
                "itemStyle": {
                        "color": "#E561C6"
                }
            }
        ]
        return returnData
