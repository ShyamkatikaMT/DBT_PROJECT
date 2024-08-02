/*
      This Macro is used to create merge statements to do upserts or deletes of the records for SCD TYPE 1 destination systems.
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      Paramaters :
      srcsystname : System_name 
      DestTbl : Destination Table
*/ 

{% macro Insert_scd1Sql(DestTbl,srcsystname) %}
{% set new_query_tag = model.name+"_tmp1" %}
{% set table_query %}
           
   MERGE INTO {{var('db_destschema')}}.{{DestTbl}} DEST
   USING {{var('db_srcviewdb')}}.{{new_query_tag}} SRC
   ON  
               SRC.{{var('column_rechashkey')}} = DEST.{{var('column_rechashkey')}} AND SRC.{{var('column_srcsyskey')}}=DEST.{{var('column_srcsyskey')}}
        
   WHEN MATCHED THEN UPDATE set
        {{sbd_edw_main.get_columnsSCD1(DestTbl)|replace('(', '')|replace(')','')|replace('"', '')}}

   WHEN NOT MATCHED THEN INSERT  
         {{sbd_edw_main.get_columns(DestTbl)|replace('"', '')}}
         VALUES 
         {{sbd_edw_main.get_columns(DestTbl)|replace('"', '')}};
   
        {% set rowrd=sbd_edw_main.Get_update_rows_read_scd1(DestTbl) %}
        
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