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


{% macro Get_Duplicate() %}
 {% set new_query_tag = model.name+"_tmp1" %}
 
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}


     {% set table_query %}
          SELECT CASE WHEN count(1)>1 THEN 1 ELSE 0 END  as Ctr11 FROM  {{var('db_srcviewdb')}}.{{new_query_tag}} GROUP BY {{var('column_rechashkey')}} HAVING COUNT(1)>1 LIMIT 1;
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
          {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
                {{return(result)}}
            {% else %}
                {{return( false) }}
    {% endif %}
    {% endmacro %}
  