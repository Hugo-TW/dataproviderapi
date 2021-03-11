# -*- coding: utf-8 -*-
import json
import kafka_replayer
import sys
import traceback
import time
import datetime
import copy
from BaseType import BaseType
def dictSortTimestamp(elm):
    return elm['timestamp']

class kafkaReplayFunc ( BaseType ):
    def __init__( self, jsonData ):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__jsonData = jsonData
    def getData( self ):
        try:           
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            self.writeLog(f'Input Json:{self.__jsonData}')
            des_fn = lambda x: json.loads(x) if x else None
            replayer = kafka_replayer.KafkaReplayer(self.__jsonData["TOPIC"],
                                                    bootstrap_servers = [ 'idts-kafka1.cminl.oa:9092' ],
                                                    key_deserializer = des_fn,
                                                    auto_offset_reset = "latest",
                                                    api_version = (0, 10, 1),
                                                    consumer_timeout_ms = 500,
                                                    value_deserializer = des_fn)
            
            CONDITION = json.dumps(self.__jsonData['CONDITION'], sort_keys=True)
            #轉timestamp
            #STARTIME = time.mktime(datetime.datetime.strptime(self.__jsonData['STARTIME'],'%Y-%m-%d %H:%M:%S').timetuple())
            st = datetime.datetime.strptime(self.__jsonData['STARTIME'],'%Y-%m-%d %H:%M:%S.%f')
            STARTIME = int(time.mktime(st.timetuple()) * 1000.0 + st.microsecond / 1000.0)
            #轉timestamp
            #ENDTIME = time.mktime(datetime.datetime.strptime(self.__jsonData['ENDTIME'],'%Y-%m-%d %H:%M:%S').timetuple())
            et = datetime.datetime.strptime(self.__jsonData['ENDTIME'],'%Y-%m-%d %H:%M:%S.%f')
            ENDTIME =  int(time.mktime(et.timetuple()) * 1000.0 + et.microsecond / 1000.0)

            self.writeLog(f"STARTIME:{self.__jsonData['STARTIME']} Transfer timestamp:{STARTIME}")
            self.writeLog(f"ENDTIME:{self.__jsonData['ENDTIME']} Transfer timestamp:{ENDTIME}")
            key = f"{self.__jsonData['TOPIC']}_{STARTIME}_{ENDTIME}"
            for k,v in self.__jsonData['CONDITION'].items():
                 key += f"_{k}-{v}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}
            datajson = []

            conditionKey = list(self.__jsonData['CONDITION'].keys())
            conditionValue = list(self.__jsonData['CONDITION'].values())
            self.writeLog(f"conditionKey:{conditionKey}，conditionValue:{conditionValue}")
            try:
                tStart = time.time()#計時開始
                #Kafka Timestamp is long type
                for record in replayer.replay(STARTIME, ENDTIME):
                #for record in replayer.replay(STARTIME, ENDTIME):
                    if type(record.value) is dict:
                        recordKey = list(record.value.keys())               
                        recordValue = list(record.value.values())             
                        #self.writeLog(f"recordKey:{recordKey}，recordValue:{recordValue}")
                
                        resultKey = [False for c in conditionKey if c not in recordKey]              
                        #有包含則[]為空，沒有包含["False"]不為空
                        if resultKey :
                            continue
                        resultValue = [False for c in conditionKey if record.value.get(c) != self.__jsonData['CONDITION'].get(c)]
                        #有包含則[]為空，沒有包含["False"]不為空
                        if resultValue:
                            continue
                        datajson.append(record.value) 
                   
                    elif type(record.value) is list:
                        rtCode = copy.deepcopy(record.value)
                        #using enumerate()，a loop over a copy of the list referred as [:]
                        for r in rtCode[:]:
                            recordKey = list(r.keys())               
                            recordValue = list(r.values())             
                            #self.writeLog(f"recordKey:{recordKey}，recordValue:{recordValue}")
                
                            resultKey = [False for c in conditionKey if c not in recordKey]              
                            #有包含則[]為空，沒有包含["False"]不為空
                            if resultKey :
                                rtCode.remove(r)
                                continue
                            resultValue = [False for c in conditionKey if r.get(c) != self.__jsonData['CONDITION'].get(c)]
                            #有包含則[]為空，沒有包含["False"]不為空
                            if resultValue:
                                rtCode.remove(r)
                                continue
                        datajson.extend(rtCode)
            except json.JSONDecodeError as jex:
                    
                 return {'Result': 'NG','Reason':f'DataSouce is not Json Format'},400 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
            datajson.sort(key=dictSortTimestamp)
            data = json.dumps(datajson, indent=2, ensure_ascii = False)
            tEnd = time.time()#計時結束          
            self.writeLog(f"Json:\n{data}")
            self.writeLog(f"Time cost {round((tEnd - tStart),1)} sec from Kafka , Message count:{len(datajson)}")#會自動做近位
            self.getRedisConnection()
            self.setRedisData(key, data, 60) 
            return json.loads(data),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(minutes = 10)).timetuple()),"DataSource":"Kafka"} 
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
        