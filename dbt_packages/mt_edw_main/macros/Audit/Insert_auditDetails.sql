/*
      This Macro is used to insert AUDIT details into table_logging table for destination system
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      Paramaters :
      Sname : System_name 
      DestTbl : Destination Table
      Sview :Source View
      srcsystname :Source system Name
      
*/

{% macro insert_auditDetails() %}
{% set LoggingTbl = "Logging_"+model.name+"_tmp" %}
      BEGIN;

        INSERT INTO {{var('db_destschema')}}.{{var('table_logging')}} select * from {{var('db_srcviewdb')}}.{{LoggingTbl}};

        INSERT INTO  {{var('db_destschema')}}.{{var('table_logdetails')}}
        select SESSION_ID,Query_type,Query_text,DataBase_Name,Schema_name,Execution_status,Error_Code,Error_Message,Start_time,End_time,
        TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED
        from  table(Information_schema.query_history()) where query_tag='dbt_special' AND SESSION_ID={{sbd_edw_main.get_CurrentSessionID()}} AND Query_type in ('INSERT','UPDATE','DROP','CREATE_TABLE_AS_SELECT','MERGE') order by start_time desc;

        drop table {{var('db_srcviewdb')}}.{{LoggingTbl}};
      COMMIT;

{% endmacro %}