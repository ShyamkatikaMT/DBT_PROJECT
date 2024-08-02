/*
      This Macro is used to troubleshoot the errors for destination system
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



{% macro Insert_auditIntermediate(errcode,errormessage) %}

        {{ log("Running  error code test: " ~ errcode ) }}
        {% set insert_intermediate_query %}
        {% set LoggingTbl = "Logging_"+model.name+"_tmp" %}

                BEGIN;
                INSERT INTO {{var('db_destschema')}}.{{var('table_logdetails')}}(SESSION_ID,QUERY_TYPE,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,EXECUTION_STATUS,ERROR_CODE,
                                                ERROR_MESSAGE,START_TIME,END_TIME)
                                                VALUES({{sbd_edw_main.get_CurrentSessionID()}},
                                                'INSERT','N/A','{{ this.database }}','{{ this.schema }}','ERROR',{{errcode}},
                                                '{{errormessage}}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
                                                
                UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET LOADSTATUS='ERROR' , ERROR_DESCRIPTION='{{errormessage}}'
                 WHERE SESSION_ID={{sbd_edw_main.get_CurrentSessionID()}};
                INSERT INTO {{var('db_destschema')}}.{{var('table_logging')}} select * from {{var('db_srcviewdb')}}.{{LoggingTbl}};
                 COMMIT;
        {% endset %}

        {% set results = run_query(insert_intermediate_query) %}

{% endmacro %}