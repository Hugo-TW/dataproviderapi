#!flask/bin/python
from flask import Flask, jsonify
from flask import request
import threading,logging,time
import multiprocessing
import json
import redis
from redis.sentinel import Sentinel
from Dao import DaoHelper 
app = Flask(__name__)

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

#BOOTSTRAP_SERVERS="localhost:9092";
class Hourly:
    def __init__(self,FACTORY_ID,EQP_ID,EQP_STATUS,HOUR_PERIOD,HOUR_RATIO):
        self.FACTORY_ID=FACTORY_ID
        self.EQP_ID=EQP_ID
        self.EQP_STATUS=EQP_STATUS
        self.HOUR_PERIOD=HOUR_PERIOD
        self.HOUR_RATIO=HOUR_RATIO
    def __repr__(self):
        return repr(self.FACTORY_ID,self.EQP_ID,self.EQP_STATUS,self.HOUR_PERIOD,self.HOUR_RATIO)
class Daily:
    def __init__(self,FACTORY_ID,EQP_ID,EQP_STATUS,DAY_PERIOD,HOUR_RATIO):
        self.FACTORY_ID=FACTORY_ID
        self.EQP_ID=EQP_ID
        self.EQP_STATUS=EQP_STATUS
        self.DAY_PERIOD=DAY_PERIOD
        self.HOUR_RATIO=HOUR_RATIO
    def __repr__(self):
        return repr(self.FACTORY_ID,self.EQP_ID,self.EQP_STATUS,self.DAY_PERIOD,self.HOUR_RATIO)
class TwentyFour:
    def __init__(self,EQP_ID,HOUR_RATIO):
        self.EQP_ID=EQP_ID
        self.HOUR_RATIO=HOUR_RATIO
    def __repr__(self):
        return repr(self.EQP_ID,self.HOUR_RATIO)
#選擇使用SQL
def SwitchSQL(dataType,factoryId,eqpType,eqpId):
    switcher={
        "Hourly":"SELECT factory_id,eqp_id,eqp_status,hour_period,hour_ratio FROM DCS_EQP_STATUS_HOUR_RATIO h WHERE     h.factory_id = '{0}' AND h.eqp_id = '{1}' AND h.hour_period IN (SELECT period FROM dcs_hours_period_v)ORDER BY HOUR_PERIOD ASC".format(factoryId,eqpId),
        "Daily": "SELECT factory_id,eqp_id,SUBSTR (hour_period, 1, 8) AS day_period,eqp_status,ROUND (AVG (hour_ratio), 0) hour_ratio FROM DCS_EQP_STATUS_HOUR_RATIO h WHERE     h.factory_id = '{0}' AND h.eqp_id = '{1}' AND h.hour_period IN (SELECT period FROM DCS_HOURSBYWEEK_PERIOD_V) GROUP BY factory_id,eqp_id,eqp_status, SUBSTR (hour_period, 1, 8)ORDER BY DAY_PERIOD ASC".format(factoryId,eqpId),
        "TwentyFour":" SELECT eqp_id, ROUND (AVG (r.hour_ratio), 0) HOUR_RATIO FROM DCS_EQP_STATUS_HOUR_RATIO r WHERE     r.eqp_id IN (SELECT eqp_id FROM dcs_eqp WHERE eqp_type = '{0}') AND EQP_STATUS = 'RUN' AND r.hour_period IN (SELECT period FROM dcs_hours_period_v) GROUP BY r.factory_id, r.eqp_id, r.eqp_status".format(eqpType)
    }
    return switcher.get(dataType,None)
#選擇json的Class
def SeletClass(dataType,tuples):
    swichter={
        "Hourly":Hourly(tuples[0],tuples[1],tuples[2],tuples[3],tuples[4]),
        "Daily":Daily(tuples[0],tuples[1],tuples[2],tuples[3],tuples[4]),
        "TwentyFour":TwentyFour(tuples[0],tuples[1])
    }
    return swichter.get(dataType,None)
#讀取DB config檔
def ReadConfig(identity):
    with open('config.json') as f:
        data = json.load(f)
        dbAccount=None
        dbPassword=None
        SERVICE_NAME=None
        try:  
            for config in data["config_list"]:
                iden=config["identity"]
                if identity == iden:
                    dbAccount=config["dbAccount"]
                    dbPassword=config["dbPassword"]
                    SERVICE_NAME=config["SERVICE_NAME"]
                    break
            return dbAccount,dbPassword,SERVICE_NAME
        except:
            return None,None,None
@app.route('/mb/api/DataSendMessage', methods=['POST'])
def create_task():
    global daoHelper,sql
    if not request:
        abort(400)
    data=None
    datajosn=[]
    Rcheck=False
    Ocheck=False
    jsonData =request.json[0]
    #print(jsonData)
    if "companyCode" not in jsonData or "site" not in jsonData or "mapId" not in jsonData or "messageType" not in jsonData or 'dataType' not in jsonData or "authKey" not in jsonData or "eqpId" not in jsonData or "eqpType" not in jsonData: 
        return jsonify({'Result': 'NG','Reason':''}), 401   
    #Redis Key
    mbtopic = jsonData["companyCode"] + "-" + jsonData["site"] + "-" + jsonData["mapId"] + "-" + jsonData["messageType"]
    #Hourly、Daily、TwentyFour型態
    dataType=jsonData["dataType"]
    #預留
    authkey=jsonData["authKey"] 
    mapId=jsonData["mapId"]
    #
    eqpId=jsonData["eqpId"]
    #
    factory=mapId.split('-')[0]
    #設備型態
    eqpType=jsonData["eqpType"]
    #抓取config.json資料庫帳密
    identity=jsonData["companyCode"] + "-" + jsonData["site"]+"-"+factory
    sql=SwitchSQL(dataType,factory,eqpType,eqpId)
    if sql is None:
        return jsonify({'Result': 'NG', 'Reason': 'DATATYPE IS NOT DEFINE'}),401
    #Redis哨兵
    sential=Sentinel([('10.55.8.62',26379)],socket_timeout=0.1,retry_on_timeout=0.1)  
    master1=sential.master_for('master1',socket_timeout=0.1)
    keys=master1.keys()#取得所有key name
    for key in keys:
        ke=str(key,encoding = "utf-8")
        if mbtopic ==ke:#比對 key name是否存在    
            data=master1.get(mbtopic).decode('utf-8')
            Rcheck=True
            break
        else:
            Rcheck=False

    if data is None and Rcheck is False:
        #連接資料庫
        dbAccount,dbPassword,SERVICE_NAME=ReadConfig(identity)
        if dbAccount is not None and dbPassword is not None and SERVICE_NAME is not None:
            daoHelper=DaoHelper(dbAccount,dbPassword,SERVICE_NAME)
            daoHelper.Connect()
            #取得資料值組
            data=daoHelper.Select(sql)
            daoHelper.Close()
            if data is not None:
                for da in data:
                    #轉Json
                    datajosn.append(SeletClass(dataType,da))
                data = json.dumps(datajosn, default=lambda o: o.__dict__, sort_keys=True, indent=4)
                Ocheck=True
            else:
                Ocheck=False 
        else:
            return jsonify({'Result': 'NG', 'Reason': 'CONFIG IS NOT DEFINE'}), 401
    if Ocheck is True and Rcheck is False:
        master1.set(mbtopic,data)
    if Ocheck is False and Rcheck is False:
        return jsonify({'Result': 'NG', 'Reason': 'DATA IS NOT EXIST'}),401
    return data, 200,{"Content-Type": "application/json"}

            
if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0', debug=False)
    
