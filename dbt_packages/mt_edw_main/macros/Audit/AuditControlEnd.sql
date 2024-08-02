/*
      This Macro is used for Insertint rows count for source and destination system
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      Paramaters :
      DestTbl : Destination Table
      rwins : Rows Insert 
      rwread : Rows read from source system
      rwupdte : Rows Update into Destination system
      srcsystname :Source system Name
*/


{% macro Updte_audit(DestTbl,rwins,rwread,rwupdte,srcsystname) %}
{% set new_query_tag = model.name+"_tmp1" %}
{% set MetadataTbl = "Metadateload_"+model.name+"_tmp" %}
{% set LoggingTbl = "Logging_"+model.name+"_tmp" %}
        {% set table_query %}
          SELECT DISTINCT {{var('table_srcdrivingtbl')}} AS srtblname FROM {{var('db_srcviewdb')}}.{{MetadataTbl}}  WHERE  SYSTEM_NAME='{{srcsystname}}'  AND DSTTBLNAME='{{DestTbl}}'
        {% endset %}

        {% if execute %}
          {%- set srctblname = run_query(table_query).columns[0].values()[0] -%}
            {%- set  srctblname = srctblname|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
            {{ log("srctblname: " ~ rwins) }}
           
                                        
        {% else %}
            {{return('SELECT 1;')}}
        {% endif %} 
      BEGIN;
        UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET 
                ROWINSERTED={{sbd_edw_main.Get_RowRead()}}-{{rwins}},ROWUPDATED={{rwupdte}},ROWREAD={{sbd_edw_main.Get_update_src_count(srcsystname,srctblname,DestTbl)}}
                ,LOADSTATUS='END'
                ,JOB_TIME_END=current_timestamp
                WHERE DSTTBLNAME='{{DestTbl}}' AND SRCTBLNAME='{{new_query_tag}}' AND SESSION_ID={{sbd_edw_main.get_CurrentSessionID()}};                           
        
       COMMIT;
{% endmacro %}