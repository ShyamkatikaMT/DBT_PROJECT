/*
      This Macro is used for determine SCD type 2 of the destination system 
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
      
*/

{% macro get_columns(table_name) %}
{% set new_query_tag = model.name+"_TMP1" %}
{% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %}
    
    {% set table_query %}
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME='{{new_query_tag}}' AND  TABLE_SCHEMA={{dbtview}}   ORDER BY ORDINAL_POSITION
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values() -%}
          {%- set  result = result|replace("'", '"') -%}
           {{return( result )}}
    {% else %}
        {{return( false ) }}
    {% endif %}
    
{% endmacro %}
