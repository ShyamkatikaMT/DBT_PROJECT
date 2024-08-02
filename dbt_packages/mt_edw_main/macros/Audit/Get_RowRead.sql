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
      DestTbl : Destination Table
      
*/


{% macro Get_RowRead() %}
 {% set new_query_tag = model.name+"_tmp1" %}
 
 {{ log("Running  Updte_audit: " ~ DestTbl ~ ", " ~ DestTbl) }}


     {% set table_query %}
          SELECT nvl(count(1),0) FROM  {{var('db_srcviewdb')}}.{{new_query_tag}}
    {% endset %}

    {% if execute %}
          {%- set result = run_query(table_query).columns[0].values()[0] -%}
          {%- set  result = result|replace("'", '"')|replace("('", '')|replace("',)", '') -%}
            
                {{return(result)}}
            {% else %}
                {{return( false) }}
    {% endif %}
    {% endmacro %}
  