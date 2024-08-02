/*
      This Macro is used to get the columns from information schema.
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft
      10/20/2023                      Pawan Mehta                 Updated Jda_qv logic

      Paramaters :
      Sname : System_name 
      DestTbl : Destination Table
*/ 

{% macro SoftDelete(DestTbl,Sname) %}
{% set new_query_tag = model.name+"_tmp1" %}
    {% set table_query %}
        SELECT DEST_CHANGE_KEY,SOURCE_CHANGE_TBL,
        CASE WHEN position('~',SOURCE_CHANGE_KEY,1)=0 THEN concat('S.',SOURCE_CHANGE_KEY) ELSE
        concat('concat(S.',REPLACE(REPLACE(SOURCE_CHANGE_KEY,'~',',''~'',S.'), 'S.''#''', '''#'''),')') END  {{var('column_srcchangekey')}} FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE DSTTBLNAME='{{DestTbl}}' AND SYSTEM_NAME='{{Sname}}';
           
     {% endset %}
    
         
                     {% if execute %}
                         {%- set resultqry = run_query(table_query) -%}
                         {%- set result    = resultqry.columns[0].values()[0] -%}
                         {%- set result1   = resultqry.columns[1].values()[0] -%}
                         {%- set result2   = resultqry.columns[2].values()[0] -%}
                       
                              {% if result==None %}
                               {{ log("Running  Softdelete: " ~ result ~ ", " ~ result1) }}
                                    {{return('select 4;' )}} 
                               {% else %}  
                                {{sbd_edw_main._Delete(DestTbl,Sname,result,result1,result2)}}
                              {% endif %}
 
                         {{return('select 3;' )}}
                     {% else %}
                            {{return( 'select 2 ;' ) }}
                     {% endif %}
                            
{% endmacro %}


{% macro _Delete(DestTbl,Sname,DEST_KEY,SOURCE_TBL,SOURCE_KEY) %}
    {% if target.database == 'DEV_EDW' %} 
        {% set devrw='DEV_RAW' %}
    {% elif target.database == 'TEST_EDW' -%}
        {% set devrw='TEST_RAW' %}
    {% elif target.database == 'PROD_EDW' %}
        {% set devrw='PROD_RAW' %}
    {% elif target.database == 'UAT_EDW' %}
        {% set devrw='UAT_RAW' %}
    {% else -%} 
        {% set devrw='INVALID_DATABASE' %}
    {% endif %}

    {% if Sname == 'JDEEF' or Sname=='SAPC11' or Sname=='SAPE03' or Sname=='SAPP10' or Sname=='OBIEE' or Sname=='SAPSHP' or Sname=='QADAR' or Sname=='QADPE' or Sname=='QADBR' or Sname=='QADDE' or Sname=='QADAR_BACK' or Sname=='QADPE_BACK' or Sname=='QADBR_BACK' or Sname=='QADDE_BACK' or Sname=='QADCH' or Sname=='QADCH_BACK' or Sname=='NAVISIONHY' or Sname=='NAVISIONHY_BACK' or Sname=='UFIDA' or Sname=='SAPBYD' or Sname=='SAPE03_BACK' or Sname=='SAPE03_BACK1' or Sname=='JDA' or Sname=='JDAEDW_BACK' or Sname=='JDAEDW' or Sname=='JDA_BACK' or Sname=='SAPP10_BACK' or Sname=='SAPC11_BACK' or Sname == 'PROWL' or Sname == 'STACKLINE' %} 
        {% set devrw='PROD_RAW' %}
    {% endif %}

               
    {% set update_query %}

        {% set check_query%}
            SELECT COUNT(1) FROM {{devrw}}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{SOURCE_TBL}}'
        {% endset %}
        {%- set resultqry = run_query(check_query) -%}
        {%- set result    = resultqry.columns[0].values()[0] -%}
         {{ log("Running  Updte_DELETE: " ~ DestTbl ~ ", " ~ result) }}
        
                        {% if Sname=='LAWSONMAC_HIST' %}
                            {% set Sname='LAWSONMAC' %}
                        {% endif %}

                        {% if Sname == 'JDAEDW' or Sname == 'JDAEDW_BACK' %}
                            {% set Sname = 'JDA_QV' %}
                        {% endif %}


                    {% set Sname=Sname|replace('_BACK1','') %}
                    {% set Sname=Sname|replace('_BACK','') %}

        {% if result!=0 and (Sname=='SAPC11' or Sname=='SAPE03' or Sname=='SAPP10' or Sname=='JDEEF') and (SOURCE_TBL!='VW_KONV_CHANGES' and SOURCE_TBL!='KONV_CHANGES')%}
                UPDATE {{var('db_destschema')}}.{{DestTbl}} D SET {{var('column_DEL_FROM_SRC_FLAG')}}='Y' , {{var('column_currrecflag')}} ='N' FROM 
                (SELECT {{SOURCE_KEY}} SRC_COL,* FROM {{devrw}}.{{Sname}}.{{SOURCE_TBL}} S QUALIFY row_number() 
                over( partition by SRC_COL order by header_change_seq desc )=1 ) S1
                WHERE S1.SRC_COL=D.{{DEST_KEY}} AND S1.CASPIAN_CHANGEINDICATOR_OPERATION='DELETE' AND D.{{var('column_srcsyskey')}}='{{Sname}}';
        {% elif result!=0 %}
                UPDATE {{var('db_destschema')}}.{{DestTbl}} D SET {{var('column_DEL_FROM_SRC_FLAG')}}='Y' , {{var('column_currrecflag')}} ='N' FROM 
                (SELECT {{SOURCE_KEY}} SRC_COL,* FROM {{devrw}}.{{Sname}}.{{SOURCE_TBL}} S QUALIFY row_number() 
                over( partition by SRC_COL order by EVENTDTS desc )=1 ) S1
                WHERE S1.SRC_COL=D.{{DEST_KEY}} AND S1.CASPIAN_CHANGEINDICATOR_OPERATION='DELETE' AND D.{{var('column_srcsyskey')}}='{{Sname}}';
        {% else %}
                {{ log("Running  Updte_DELETE111: " ~ DestTbl ~ ", " ~ result) }}

                 UPDATE {{var('db_destschema')}}.{{var('table_logging')}} SET 
                 LOADSTATUS='PARTIALLY END'
                ,JOB_TIME_END=current_timestamp
                ,ERROR_DESCRIPTION={{var('error_delete')}}
                 WHERE  SESSION_ID={{sbd_edw_main.get_CurrentSessionID()}};
         {% endif %}

    {% endset %}

    {% if execute %}
              {%- set result = run_query(update_query) %}
     {% else %} 
            {{return(false)}}
    {% endif %}
{% endmacro %}
