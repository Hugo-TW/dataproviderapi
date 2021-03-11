# -*- coding: utf-8 -*-
import json
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
from datetime import timedelta
class fpyLogFunc(BaseType):
    def __init__(self, jsonData):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            #self.writeLog(f'Input Json:{self.jsonData}')
            # STARTIME to TimeStamp
            st = datetime.datetime.strptime(self.jsonData['STARTIME'],'%Y-%m-%d')
            STARTIME = int(time.mktime(st.timetuple()))
            # ENDTIME to TimeTtamp
            et = datetime.datetime.strptime(self.jsonData['ENDTIME'],'%Y-%m-%d')
            ENDTIME =  int(time.mktime(et.timetuple()))
            self.writeLog(f'TimeStamp:{STARTIME} ~ {ENDTIME}')
            bottomLine = "_"
            redisKey = ""
            tmp = []
            tmp.append(f"{self.__class__.__name__}")
            tmpCodition = copy.deepcopy(self.jsonData["CONDITION"])
            tmpGroupby = copy.deepcopy(self.jsonData["GROUPBY"])
            tmpConceal = copy.deepcopy(self.jsonData["CONCEAL"])
            tmpCONDITION = {}
            for k,v in tmpCodition.items():
                if len(v) != 0:
                    if isinstance(v,(dict,list)):
                        for kk,vv in v.items():
                            tmp.append(f"{kk}-{vv}")
                    else:
                        tmp.append(v)
                else:
                    del self.jsonData["CONDITION"][k]
            
            GROUPBY = {}
            GROUPBY["_id"] = {}
            for c in tmpGroupby:
                GROUPBY["_id"][c] = f"${c}" 
            passGROUPBY = copy.deepcopy(GROUPBY)
            deftGROUPBY = copy.deepcopy(GROUPBY)
            passGROUPBY["PASS_QTY"] = {
                "$sum":{
                    "$toInt":"$QTY"
                }
            }
            deftGROUPBY["DFCT_QTY"] = {
                "$sum":{
                    "$toInt":"$QTY"
                }
            }

            tmp.extend(tmpGroupby)
            for k,v in tmpConceal.items():
                tmp.append(f"{k}-{v}")
            tmp.append(str(STARTIME))
            tmp.append(str(ENDTIME))
            redisKey = bottomLine.join(tmp)

            self.getRedisConnection()
            if self.searchRedisKeys(redisKey):
                #self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(redisKey)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(self.jsonData["CONDITION"]["FACTORY_ID"] + '_PASS'))).timetuple()),"DataSource":"Redis"}
            CONCEAL = {}
            for k,v in self.jsonData["CONCEAL"].items():
                CONCEAL[k] = {"$ne":v}
            tmpCONDITION = self.jsonData["CONDITION"]
            tmpCONDITION["UTC"] = {
                                  "$gte" : STARTIME, "$lte" : ENDTIME  
                            }
            CONDITION = [
                            {
                                "$match":tmpCONDITION
                            },
                            {
                                "$project":{
                                                "_id":0,
                                                "FAC_ID":0,
                                                "JobFinishTime":0,
                                            }
                            }
                        ]
            passCONDITION = copy.deepcopy(CONDITION)
            passCONDITION.append({
                                 "$group" : passGROUPBY
                            })
            passCONDITION.append({
                                "$project":{
                                                "_id":1,
                                                "PASS_QTY":1
                                           }
                            })
            passCONDITION.append({
                                    "$sort":{
                                                "_id.ACCT_DATE":1
                                            }
                                })

            self.writeLog(f'passCONDITION:{passCONDITION}')
            deftCONTION = copy.deepcopy(CONDITION)
            deftCONTION.append({
                                 "$group" : deftGROUPBY
                            })
            deftCONTION.append({
                                    "$project":{
                                                    "_id":1,
                                                    "DFCT_QTY":1
                                               }
                                })
            deftCONTION.append(
                                {
                                    "$sort":{
                                                "_id.ACCT_DATE":1
                                            }
                                })
            self.writeLog(f'deftCONTION:{deftCONTION}')
            self.getMongoConnection()
            self.setMongoDb("IAMP")
            self.setMongoCollection("passHisAndCurrent")              
            pData = self.aggregate(passCONDITION)
            
            self.setMongoCollection("deftHisAndCurrent")
            dData = self.aggregate(deftCONTION)
            self.closeMongoConncetion()
            deftData = []
            for d in dData:
                deftData.append(d)
            passData = []
            for p in pData:
                passData.append(p)     
            data = []
            maxPass = {}
            sumDeft = {}
            for p in passData:
                if p['_id']['ACCT_DATE'] not in maxPass:
                    maxPass[p['_id']['ACCT_DATE']] = p['PASS_QTY']
                elif maxPass.get(p['_id']['ACCT_DATE'], 0) < p['PASS_QTY']:
                    maxPass[p['_id']['ACCT_DATE']] = p['PASS_QTY']
            self.writeLog(f"maxPass:\n {json.dumps(maxPass, sort_keys=True, indent=2)}")
            for d in deftData:
                if d['_id']['ACCT_DATE'] not in sumDeft:
                    sumDeft[d['_id']['ACCT_DATE']] = d['DFCT_QTY']
                elif d['_id']['ACCT_DATE'] in sumDeft:
                    sumDeft[d['_id']['ACCT_DATE']] = sumDeft.get(d['_id']['ACCT_DATE'],0) + d['DFCT_QTY']
            #self.writeLog(f"sumDeft:\n {json.dumps(sumDeft, sort_keys=True, indent=2)}")
            #for p in passData: 
            #    for d in deftData:
            #        flag = False                 
            #        for k,v in p['_id'].items():                                          
            #            if p['_id'].get(k,'') != d['_id'].get(k,''):
            #                flag = True
            #                break
            #        if flag:
            #            continue
            #        oData = copy.deepcopy(p["_id"])
            #        oData["ACCT_DATE"] = datetime.datetime.strptime(oData["ACCT_DATE"],'%Y%m%d').strftime('%Y-%m-%d')
            #        oData["PASS_QTY"] = copy.deepcopy(p["PASS_QTY"])
            #        oData["DFCT_QTY"] = copy.deepcopy(d["DFCT_QTY"])
            #        oData["FPY"] =  round((oData["PASS_QTY"] - oData["DFCT_QTY"]) / oData["PASS_QTY"], 4)
            #        #float('{:.2f}'.format( )
            #        data.append(copy.deepcopy(oData))
            #        oData = {}
            for k,v in maxPass.items():
                acctData = {
                    "COMPANY_CODE":self.jsonData["CONDITION"].get('COMPANY_CODE','INX'),
                    "SITE":self.jsonData["CONDITION"].get('SITE','TN'),
                    "FACTORY_ID":self.jsonData["CONDITION"].get('FACTORY_ID','J005'),
                    "ACCT_DATE":datetime.datetime.strptime(k,'%Y%m%d').strftime('%Y-%m-%d'),
                    "DEFECT":sumDeft.get(k,'0'),
                    "PASS":v,
                    "DEFECT_RATE":round(sumDeft.get(k,'0')/v,4),
                    "FPY":1 - round(sumDeft.get(k,'0')/v,4),
                }
                data.append(acctData)
            self.getRedisConnection()
            if self.searchRedisKeys(self.jsonData["CONDITION"]["FACTORY_ID"] + '_PASS'):
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), self.getKeyExpirTime(self.jsonData["CONDITION"]["FACTORY_ID"] + '_PASS'))
            else:
                self.setRedisData(redisKey, json.dumps(data, sort_keys=True, indent=2), 60)
            
            return data, 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
        except Exception as e:
            error_class = e.__class__.__name__ #取得錯誤類型
            detail = e.args[0] #取得詳細內容
            cl, exc, tb = sys.exc_info() #取得Call Stack
            lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
            fileName = lastCallStack[0] #取得發生的檔案名稱
            lineNum = lastCallStack[1] #取得發生的行號
            funcName = lastCallStack[2] #取得發生的函數名稱
            self.writeError(f"File:[{fileName}] , Line:{lineNum} , in {funcName} : [{error_class}] {detail}")
            return {'Result': 'NG','Reason':f'{funcName} erro'},400 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}