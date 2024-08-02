/*
      This Macro is used to perform full refresh, meaning truncate and load to cater for systems and tables like JDA that are full refresh in Zone3
      Author :
      Creation Date : 05/24/2022
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      
      Paramaters :
      srcsystname : System_name 
      DestTbl : Destination Table
*/ 

{% macro FullR_scd1Sql(DestTbl,srcsystname) %}
{% set new_query_tag = model.name+"_tmp1" %}
{% set table_query %}

   TRUNCATE TABLE {{var('db_destschema')}}.{{DestTbl}} ;

    INSERT INTO  {{var('db_destschema')}}.{{DestTbl}}
         {{sbd_edw_main.get_columns(DestTbl)}}
         SELECT 
         {{sbd_edw_main.get_columns(DestTbl)|replace('(', '')|replace(')','')}} 
         FROM {{var('db_srcviewdb')}}.{{new_query_tag}};
  
   
        {% set rowrd=sbd_edw_main.Get_update_rows_read(DestTbl) %}
        
        {{sbd_edw_main.Updte_audit(DestTbl,rowrd,100,rowrd,srcsystname)}}     
    
       /*DROP TABLE {{var('db_srcviewdb')}}.{{new_query_tag}};      */
    
    {% endset %}
    
          {%- set result = run_query(table_query)-%}
                        {% if execute %}
                            {{return( 'select 1 ;' )}}
                        {% else %}
                            {{return( 'select 2 ;' ) }}
          {% endif %}
                            
   
                              
{% endmacro %}