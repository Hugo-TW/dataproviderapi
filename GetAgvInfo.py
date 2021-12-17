from flask import Flask, jsonify
from flask import request
import threading,logging
import time
import multiprocessing
import json
import redis
import os
import sys
import traceback
from redis.sentinel import Sentinel
from Dao import DaoHelper,ReadConfig
from flask_cors import CORS
from BaseType import BaseType
from datetime import date, datetime
os.environ['NLS_LANG'] = 'TRADITIONAL CHINESE_TAIWAN.UTF8'
class AgvInfo(BaseType):
    def __init__(self,indentity):
        super().__init__()
        self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name}')
        self.__indentity = indentity
        
    def getData(self):
        try:
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} Start')
            #sql =  "with position_mapping as (select t.item5 as Line, t.item6 as MM, t.item7 as SS, t.item8 as S, t.item9 as STK_POSITION,t.item10 as LINE_POSITION from DCSMOD3_DB.DCS_EQ_PORT_LIST t)select t.agv_id ,case when t.create_date + (t1.s/86400) < sysdate then null else t1.Line||' ('||t.localtion_to||')' end as location,case when t.create_date + (t1.s/86400) < sysdate then null else to_date(to_char(t.create_date,'yyyy/mm/dd hh24miss'),'yyyy/mm/dd hh24miss') end as start_time,case when t.create_date + (t1.s/86400) < sysdate then null else t.create_date + (t1.s/86400) end as end_time  from ( select t.event_code as description,t.agv_id,t.localtion_from,t.localtion_to, t.current_position, max(t.create_date) over (partition by t.line_id,t.agv_id) as max_create_date,t.create_date from mcs_dispatch_command_log t where t.event_code = 3 and t.create_date is not null  and t.localtion_from in (select t.LINE_POSITION from position_mapping t ) )t, position_mapping t1   where t.max_create_date = t.create_date and t.localtion_to = t1.LINE_POSITION(+) and t.create_date + (t1.s/86400) > sysdate order by 1 asc  "
            #sql =  "with position_mapping as (select t.item5 as Line, t.item6 as MM, t.item7 as SS, t.item8 as S, t.item9 as STK_POSITION,t.item10 as LINE_POSITION from DCSMOD3_DB.DCS_EQ_PORT_LIST t)select t.agv_id ,case when t.create_date + (t1.s/86400) < sysdate then null else t1.Line||' ('||t.localtion_to||')' end as location,case when t.create_date + (t1.s/86400) < sysdate then null else to_date(to_char(t.create_date,'yyyy/mm/dd hh24miss'),'yyyy/mm/dd hh24miss') end as start_time,case when t.create_date + (t1.s/86400) < sysdate then null else t.create_date + (t1.s/86400) end as end_time from ( select t.event_code as description,t.agv_id,t.localtion_from,t.localtion_to, t.current_position, max(t.create_date) over (partition by t.line_id,t.agv_id) as max_create_date,t.create_date from mcs_dispatch_command_log t where t.event_code = 3 and t.create_date is not null and t.localtion_from in (select t.LINE_POSITION from position_mapping t ) and t.localtion_from <> t.current_position )t, position_mapping t1,(select t.event_code as description,t.agv_id,t.localtion_from,t.localtion_to, t.current_position, max(t.create_date) over (partition by t.line_id,t.agv_id) as max_create_date,t.create_date from mcs_dispatch_command_log t where t.event_code in (4,7) and t.create_date is not null )t2 where t.max_create_date = t.create_date and t2.max_create_date = t2.create_date and t.localtion_to = t1.LINE_POSITION(+) and t.agv_id = t2.agv_id(+) and t.create_date + (t1.s/86400) > sysdate and t2.create_date < t.create_date order by 1 asc  "
            sql = """with runtime_mapping as 
                    (
                    select t.ccomment04 as Line, t.data_value as S, t.ccomment01 as agv_oper_id_from,t.ccomment02 as agv_oper_id_to 
                    from dcs_parameter t
                    where t.data_type = 'MAP_AGV_INFO'
                    )
                    ,TICKET_MAPPING_AGV_INFO as (
                    select distinct substr(t.ticket_id,0,instr(t.ticket_id,'-')-1) as trx_job_id,t.agv_id 
                    from mcs_dispatch_command_log t
                    where t.agv_id is not null
                    and substr(t.ticket_id,0,instr(t.ticket_id,'-')-1) is not null
                    )
                    ,MAX_AGV_TICKET_ID as (
                    select substr(max(t.ticket_id),0,instr(max(t.ticket_id),'-')-1) as ticket_id,t.agv_id from mcs_dispatch_command_log t
                    where t.agv_id like 'SB-%'
                    and t.create_date is not null
                    and t.create_user <> 'AGVC'
                    group by t.agv_id
                    )
                    ,TRX_WBS as (
                    select t.trx_job_id,t.trx_sub_job_id,t.from_identity,t.to_identity,t1.agv_oper_id 
                    from dcs_trx_job_wbs t, mcs_agv_oper_map t1 
                    where t.from_identity = t1.identity 
                    and t.agv_position = t1.agv_position
                    and t.trx_job_id in (select t.ticket_id from MAX_AGV_TICKET_ID t)
                    )
                    ,DISPATCH_INFO_START as (
                    select * from 
                    (
                        select t2.agv_id,t.trx_job_id,t.trx_sub_job_id,t.from_identity,t.to_identity,
                        lag(t.agv_oper_id,1) over (partition by t.trx_job_id order by t.trx_job_id,t.trx_sub_job_id asc) as agv_oper_id_from,
                        t.agv_oper_id as agv_oper_id_to,
                        to_date(to_char(t1.create_date,'yyyy/mm/dd hh24miss'),'yyyy/mm/dd hh24miss') as create_date
                        from TRX_WBS t, (select * from mcs_dispatch_command_log t where t.event_code = 3) t1, TICKET_MAPPING_AGV_INFO t2
                        where t.trx_sub_job_id = t1.ticket_id(+)
                        and t.trx_job_id = t2.trx_job_id (+) 
                        and t.from_identity not like '%_02'
                    ) t
                    where t.from_identity not like '%STK%' 
                    and t.from_identity not like '%_02'
                    )
                    ,DISPATCH_INFO_END as (
                    select distinct agv_id,trx_sub_job_id,create_date from
                    (
                        select t2.agv_id,t.trx_job_id,t.trx_sub_job_id,t.from_identity,t.to_identity,
                        lag(t.agv_oper_id,1) over (partition by t.trx_job_id order by t.trx_job_id,t.trx_sub_job_id asc) as agv_oper_id_from,
                        t.agv_oper_id as agv_oper_id_to,
                        to_date(to_char(t1.create_date,'yyyy/mm/dd hh24miss'),'yyyy/mm/dd hh24miss') as create_date
                        from TRX_WBS t, (select * from  mcs_dispatch_command_log t where t.create_date||t.ticket_id in (select min(create_date)||ticket_id as ticket_date from mcs_dispatch_command_log t where t.event_code in (4,7) group by ticket_id)) t1, TICKET_MAPPING_AGV_INFO t2
                        where t.trx_sub_job_id = t1.ticket_id(+)
                        and t.trx_job_id = t2.trx_job_id (+)  
                        and t1.create_date is not null
                        and t.from_identity not like '%_02'
                    ) t
                    where t.from_identity not like '%STK%' 
                    and t.from_identity not like '%_02'
                    )
                    ,Result_Data as (
                    select t.agv_oper_id_from,t.agv_oper_id_to,t.create_date,t.trx_sub_job_id,
                    t.agv_id,'#'||t1.line||' ('||t.agv_oper_id_to||')' as to_identity, 
                    nvl(lag(t.create_date,1) over (partition by t.agv_id order by t.trx_sub_job_id asc),t.create_date) as Statr_Time ,
                    nvl(lag(t.create_date,1) over (partition by t.agv_id order by t.trx_sub_job_id asc),t.create_date) + ((t1.s + nvl(lag(t1.s,1) over (partition by t.agv_id order by t.trx_sub_job_id asc),0))/86400) as to_step_time,
                    t1.s + nvl(lag(t1.s,1) over (partition by t.agv_id order by t.trx_sub_job_id asc),0) as s,t2.create_date as end_date
                    from DISPATCH_INFO_START t, runtime_mapping t1, DISPATCH_INFO_END t2
                    where t.agv_oper_id_from = t1.agv_oper_id_from
                    and t.agv_oper_id_to = t1.agv_oper_id_to
                    and t.agv_id = t2.agv_id(+)
                    and t.trx_sub_job_id = t2.trx_sub_job_id(+)
                    )
                    select distinct 
                    t.agv_id,t.to_identity,t.Statr_Time,t.to_step_time
                    from Result_Data t
                    where 1=1
                    and t.Statr_Time + (t.s/86400) > sysdate 
                    and t.end_date is null
                    and t.Statr_Time is not null
                    order by t.agv_id,t.to_step_time"""

            self.writeLog(f'SQL:\n {sql}')
            self.getConnection(self.__indentity)
            data = self.Select(sql)
            self.closeConnection()
            
            datajson=[]
            if(len(data) != 0):
                for da in data:
                    datadict={
                        "車號" : da[0],
                        "目的地" : da[1],
                        "出發時間" : da[2],
                        "預估到站時間" : da[3]
                    }
                    datajson.append(datadict)
            else:
                datadict={
                        "車號" : "",
                        "目的地" :"",
                        "出發時間" : "",
                        "預估到站時間" : ""
                    }
                datajson.append(datadict)
            data = json.dumps(datajson, sort_keys=False, indent=2,cls=ComplexEncoder)
            
            self.writeLog(f"Json:\n {data}")
            self.writeLog(f'{self.__class__.__name__} {sys._getframe().f_code.co_name} DONE')
            return json.loads(data),200,{"Content-Type": "application/json",'Connection':'close','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'x-requested-with,content-type'}
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
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)