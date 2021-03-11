# -*- coding: utf-8 -*-
import json
import sys
import traceback
import datetime
import time
from Dao import DaoHelper,ReadConfig
from BaseType import BaseType
class Menu(BaseType):
    def __init__(self,COMPANY_CODE,SITE,FACTORY_ID,SUPPLY_LINE,TYPE,indentity):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
        self.COMPANY_CODE = COMPANY_CODE
        self.SITE = SITE
        self.FACTORY_ID = FACTORY_ID
        self.SUPPLY_LINE = SUPPLY_LINE
        self.TYPE = TYPE
        self.indentity = indentity
        self.dbAccount,self.dbPassword,self.SERVICE_NAME = ReadConfig('config.json',self.indentity).READ()
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} ')
            
            #if len(self.SUPPLY_LINE.strip())>0:   
            #    sql="select distinct (entity_id), type, supply_line,(select distinct display_id from DMB_ALINE_MONITOR_MAP where company_code = t.company_code and site = t.site and factory_id = t.factory_id and item = t.supply_line) as display_supply_line from dmb_dc_entity_state t where t.company_code = '{0}' and t.site = '{1}' and t.factory_id = '{2}' and t.type != 'EQ' and t.supply_line = '{3}'group by company_code, site, factory_id, supply_line, type, entity_id order by supply_line, type, entity_id".format(self.COMPANY_CODE,self.SITE,self.FACTORY_ID,self.SUPPLY_LINE)
            #else:
            #    sql="select distinct (entity_id), type, supply_line,(select distinct display_id from DMB_ALINE_MONITOR_MAP where company_code = t.company_code and site = t.site and factory_id = t.factory_id and item = t.supply_line) as display_supply_line from dmb_dc_entity_state t where t.company_code = '{0}' and t.site = '{1}' and t.factory_id = '{2}' and t.type != 'EQ' group by company_code, site, factory_id, supply_line, type, entity_id order by supply_line, type, entity_id".format(self.COMPANY_CODE,self.SITE,self.FACTORY_ID)
            key = f"{self.__class__.__name__}_{self.COMPANY_CODE}_{self.SITE}_{self.FACTORY_ID}_{self.SUPPLY_LINE}_{self.TYPE}"
            self.writeLog(f"Redis Key:{key}")
            self.getRedisConnection()
            if self.searchRedisKeys(key):
                self.writeLog(f"Cache Data From Redis")
                return json.loads(self.getRedisData(key)), 200 ,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(seconds = self.getKeyExpirTime(key))).timetuple()),"DataSource":"Redis"}
            sql = f"""
                        SELECT 
                            DISTINCT company_code,
                            site,
                            factory_id,
                            supply_line,
                            display_supply_line,
                            TYPE,
                            entity_id,
                            factory_id||'_'||entity_id as IDENTITY 
                        FROM DMB_ALINE_ALL_EQP_V t
                        WHERE  t.company_code = nvl('{self.COMPANY_CODE}',t.company_code)
                        AND t.site = nvl('{self.SITE}',t.site)
                        AND t.factory_id = nvl('{self.FACTORY_ID}',t.factory_id) 
                        AND t.supply_line = nvl('{self.SUPPLY_LINE}',t.supply_line) 
                        AND t.TYPE = nvl('{self.TYPE}',t.TYPE)
                        GROUP BY 
                            company_code,
                            site,
                            factory_id,
                            supply_line,
                            display_supply_line,
                            TYPE,
                            entity_id
                        ORDER BY 
                            company_code,
                            site,
                            factory_id,
                            supply_line,
                            TYPE,
                            entity_id
                  """
            self.writeLog(f'SQL:{sql}')
            #daoHelper= DaoHelper(self.dbAccount,self.dbPassword,self.SERVICE_NAME)
            self.getConnection(self.indentity)
            #daoHelper.Connect()
            Description,data = self.SelectAndDescription(sql)
            #daoHelper.Close()
            self.closeConnection()
            col_names = [row[0] for row in Description]
            datajson = [dict(zip(col_names,da)) for da in data]
            #datajson=[]
            #for da in data:
            #    datajson.append(Pack(da[0],da[1],da[2]))
            data = json.dumps(datajson,sort_keys=True, indent=2,ensure_ascii = False)
            self.writeLog(f"Json:\n {data}")

            self.getRedisConnection()
            self.setRedisData(key, data, 3600)      
            return json.loads(data), 200, {"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST','Access-Control-Allow-Headers':'x-requested-with,content-type',"Access-Control-Expose-Headers":"Expires,DataSource","Expires":time.mktime((datetime.datetime.now() + datetime.timedelta(minutes = 10)).timetuple()),"DataSource":"Oracle"} 
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

