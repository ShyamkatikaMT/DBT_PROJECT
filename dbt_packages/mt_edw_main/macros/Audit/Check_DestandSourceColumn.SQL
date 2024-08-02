/*
      This Macro is used to check duplicates for source and destination system
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft
      01/21/2022                      Pradeep                     Updated all Entity Name with Veriables

      Paramaters :
      DestTbl : Destination Table
*/


{% macro Get_DestandSourceColumn(DestTbl) %}
 {% set new_query_tag = model.name+"_TMP1" %}
 {% set dbtview = "'" ~ var('db_srcviewdb')  ~ "'"  %} 
 {% set db_destschema = "'" ~ var('db_destschema')  ~ "'"  %} 
  
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}


     {% set table_query %}
            SELECT COUNT(1) CTR FROM
            (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{{new_query_tag}}' AND TABLE_SCHEMA={{dbtview}}) A
            LEFT JOIN
            (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{{DestTbl}}' AND TABLE_SCHEMA={{db_destschema}}) B
            ON A.COLUMN_NAME=B.COLUMN_NAME WHERE B.COLUMN_NAME IS NULL
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
          {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{return(result)}}
            {% else %}
                {{return( false) }}
    {% endif %}
    {% endmacro %}
  