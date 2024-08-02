/*
      This Macro is used for determine SCD type 1 of the destination system 
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
      
*/{% macro get_columnsSCD1(table_name) %}
      {% set new_query_tag = model.name+"_TMP1" %}
      {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %}
      {% set etlInrt = "'" ~ var('column_ETL_INS_DTE')  ~ "'"  %}
      {% set etlUpdt = "'" ~ var('column_ETL_UPD_DTE')  ~ "'"  %}
    
    {% set table_query %}
          SELECT concat('DEST.',COLUMN_NAME, CASE WHEN COLUMN_NAME={{etlUpdt}} THEN '=CURRENT_TIMESTAMP::TIMESTAMP_NTZ' ELSE   CONCAT('=SRC.',COLUMN_NAME) END)  
          FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{{new_query_tag}}' AND  TABLE_SCHEMA={{dbtview}} 
          AND COLUMN_NAME NOT IN({{etlInrt}})  ORDER BY ORDINAL_POSITION 
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values() -%}
          {%- set  result = result|replace("'", '"') -%}
           {{return( result )}}
    {% else %}
        {{return( false ) }}
    {% endif %}
    
{% endmacro %}