/*
      This Macro is used for reads the rows count for source system
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


{% macro Get_update_rows_read(DestTbl) %}
 {% set new_query_tag = model.name+"_tmp1" %}
 {% set LoggingTbl = "Logging_"+model.name+"_tmp" %}
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}


     {% set table_query %}
          select nvl(count(1),0) ctr  FROM    {{var('db_destschema')}}.{{DestTbl}} T , {{var('db_srcviewdb')}}.{{new_query_tag}} S 
                                WHERE T.{{var('column_rechashkey')}} = S.{{var('column_rechashkey')}} AND 
                                T.{{var('column_verexpirydt')}} ::DATE='9999-12-31' AND T.{{var('column_currrecflag')}} ='Y' 
                                AND T.{{var('column_orprecflag')}} ='N' AND T.{{var('column_srcsyskey')}}=S.{{var('column_srcsyskey')}};
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
          {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
            
                {{return(result)}}
            {% else %}
                {{return( false) }}
    {% endif %}
    {% endmacro %}




{% macro Get_update_src_count(sname,SrcTbl,dname) %}
 {% set new_query_tag = model.name+"_tmp1" %}
 {% set System_name = "'" ~ sname  ~ "'" %}
 {% set Dest_Table = "'" ~ dname  ~ "'" %}
 {% set MetadataTbl = "Metadateload_"+model.name+"_tmp" %}
 {% set LoggingTbl = "Logging_"+model.name+"_tmp" %}

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
    
    {% if sname == 'JDEEF' or sname=='SAPC11' or sname=='SAPE03' or sname=='SAPP10' or sname=='OBIEE' or sname=='SAPSHP' or sname=='QADAR' or sname=='QADPE' or sname=='QADBR' or sname=='QADDE' or sname=='QADAR_BACK' or sname=='QADPE_BACK' or sname=='QADBR_BACK' or sname=='QADDE_BACK' or sname=='QADCH' or sname=='QADCH_BACK' or sname=='NAVISIONHY' or sname=='NAVISIONHY_BACK' or sname=='UFIDA' or sname=='SAPBYD' or sname=='SAPE03_BACK' or sname=='SAPE03_BACK1' or sname=='JDA' or sname=='JDAEDW' or sname=='JDAEDW_BACK' or sname=='JDA_BACK' or sname=='SAPP10_BACK' or sname=='SAPC11_BACK' or sname == 'PROWL' or sname == 'STACKLINE' %} 
        {% set devrw='PROD_RAW' %}
    {% endif %}


 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}
       {% set check_query %}
            SELECT COUNT(1) FROM {{devrw}}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{{SrcTbl}}'
        {% endset %}
        {%- set resultqry = run_query(check_query) -%}
        {%- set result    = resultqry.columns[0].values()[0] -%}
         {{ log("Running  Update_count: " ~ DestTbl ~ ", " ~ result) }}

       
       
            {% set tbl_query1 %}
                  SELECT DISTINCT SRCTBLCONDITION FROM  {{var('db_srcviewdb')}}.{{MetadataTbl}}  WHERE  
                    SYSTEM_NAME={{System_name}}  AND DSTTBLNAME={{Dest_Table}};
            {% endset %}
           
            {% if execute %}
            {%- set result = run_query(tbl_query1).columns[0].values()[0] -%}
                {%- set resultcon    = result|replace("@@DBNAME",devrw) -%}    
                {{ log("Running  CheckResult: "  ~ resultcon ) }}
             

            {% endif %}
          
    
    {% if result!=0 %}
                {% set table_query %}

                        {% if sname=='LAWSONMAC_HIST' %}
                            {% set sname='LAWSONMAC' %}
                        {% endif %}

                        {% set sname=sname|replace('_BACK1','') %}
                        {% set sname=sname|replace('_BACK','') %}

                        {% if resultcon=='None' %}
                                    select nvl(count(1),0) ctr  FROM    {{devrw}}.{{sname}}.{{SrcTbl}} A  
                                                WHERE {{ sbd_edw_main.get_lastloaddate4src( System_name , Dest_Table)}} 
                        {% else %}
                                    select nvl(count(1),0) ctr  FROM  {{resultcon}} and A.{{ sbd_edw_main.get_lastloaddate4src( System_name , Dest_Table)}}
                        
                        {% endif %}

                {% endset %}

                {% if execute %}
                    {%- set result = run_query(table_query).columns[0].values()[0] -%}
                    {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                        
                            {{return(result)}}
                  {% else %}
                            {{return( false) }}
                {% endif %}
    {% else %}
                    {% set table_query %} 
                        UPDATE {{var('db_srcviewdb')}}.{{LoggingTbl}} SET 
                        LOADSTATUS='PARTIALLY END'
                        ,JOB_TIME_END=current_timestamp
                        ,ERROR_DESCRIPTION= {{var('error_sourcecount')}}
                        WHERE  SESSION_ID={{sbd_edw_main.get_CurrentSessionID()}};
                       
                    {% endset %}
    
                        {% if execute %}
                            {%- set result = run_query(table_query) -%}
                            {{return(0)}}
                        {% else %}
                            {{return(0) }}
                        {% endif %}
                    
    {% endif %}
{% endmacro %}



{% macro Get_update_rows_read_scd1(DestTbl) %}
 {% set new_query_tag = model.name+"_tmp1" %}
 
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}


     {% set table_query %}
          select nvl(count(1),0) ctr  FROM    
          {{var('db_destschema')}}.{{DestTbl}} DEST , {{var('db_srcviewdb')}}.{{new_query_tag}} SRC 
          WHERE
          SRC.{{var('column_rechashkey')}} = DEST.{{var('column_rechashkey')}} AND SRC.{{var('column_srcsyskey')}}=DEST.{{var('column_srcsyskey')}}
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
          {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
            
                {{return(result)}}
            {% else %}
                {{return( false) }}
    {% endif %}
    {% endmacro %}
