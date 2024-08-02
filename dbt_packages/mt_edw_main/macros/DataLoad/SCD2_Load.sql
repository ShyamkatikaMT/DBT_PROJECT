/*
      This Macro is used to create merge statements to do upserts or deletes of the records for SCD TYPE 2 destination systems.
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft
      02/28/2022                      Pradeep                     Removed delete to source flag condition

      Paramaters :
      srcsystname : System_name 
      DestTbl : Destination Table
*/ 

{% macro get_scdSql(DestTbl,srcsystname) %}
{% set new_query_tag = model.name+"_tmp1" %}
    {% set table_query %}
           
       {% set rowrd=sbd_edw_main.Get_update_rows_read(DestTbl) %}

        UPDATE   {{var('db_destschema')}}.{{DestTbl}} T    SET T.{{var('column_verexpirydt')}} = S.{{var('column_z3loddtm')}},
                                T.{{var('column_currrecflag')}} ='N' FROM {{var('db_srcviewdb')}}.{{new_query_tag}} S 
                                WHERE T.{{var('column_rechashkey')}} = S.{{var('column_rechashkey')}} AND 
                                T.{{var('column_verexpirydt')}}::DATE ='9999-12-31' AND T.{{var('column_currrecflag')}} ='Y' 
                                 AND T.{{var('column_srcsyskey')}}=S.{{var('column_srcsyskey')}};
   
       

        {{sbd_edw_main.Updte_audit(DestTbl,0,100,rowrd,srcsystname)}}     
        {{sbd_edw_main.Insert_scdSql(DestTbl)}}

      /*  DROP TABLE {{var('db_srcviewdb')}}.{{new_query_tag}};      */
    {% endset %}
    
          {%- set result = run_query(table_query)-%}
                        {% if execute %}
                            {{return( 'select 1 ;' )}}
                        {% else %}
                            {{return( 'select 2 ;' ) }}
                        {% endif %}
                            
{% endmacro %}



{% macro Insert_scdSql(DestTbl) %}

{% set new_query_tag = model.name+"_tmp1" %}
   INSERT INTO  {{var('db_destschema')}}.{{DestTbl}}
         {{sbd_edw_main.get_columns(DestTbl)}}
         SELECT 
         {{sbd_edw_main.get_columns(DestTbl)|replace('(', '')|replace(')','')}} FROM {{var('db_srcviewdb')}}.{{new_query_tag}};
                              
{% endmacro %}