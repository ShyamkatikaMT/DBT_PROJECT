/*
      This Macro is used for Insertint rows count for source and destination system
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft
      01/21/2022                      Pradeep                     Updated all Entity Name with Veriables

      Paramaters :
      Sname : System_name 
      DestTbl : Destination Table
      Sview :Source View 
*/



{% macro insert_audit(DestTbl,Sname,sview) %}
 {% set new_query_tag = model.name+"_tmp1" %}
 {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %}
 {% set LoggingTbl = "Logging_"+model.name+"_tmp" %}
 
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ dbtview) }}

        CREATE OR REPLACE TABLE {{var('db_srcviewdb')}}.{{LoggingTbl}}
        AS SELECT * FROM CONSOLIDATED.LOGGING WHERE  1=2;
                                              
        INSERT INTO {{var('db_srcviewdb')}}.{{LoggingTbl}}(LID, MODEL_NAME,MODEL_TYPE,SESSION_ID,SRCTBLNAME,SRCSCHEMA,DSTTBLNAME,DSTSCHEMA,JOB_TIME_START,LOADSTATUS,
        ROWREAD,CRETEDDATETIME)
        VALUES
        ({{sbd_edw_main.Get_DIT(Sname,DestTbl,sview)}},'{{model.name}}','DATA LOAD',{{sbd_edw_main.get_CurrentSessionID()}},'{{new_query_tag}}',{{dbtview}},'{{DestTbl}}','CONSOLIDATED',current_timestamp,
        'START',0,CURRENT_DATE);
                                    
{% endmacro %}

