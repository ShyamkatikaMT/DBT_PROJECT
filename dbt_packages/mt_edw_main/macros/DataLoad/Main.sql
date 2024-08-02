/*
      This Macro is used for set incremental macros for source anmd destination systems, also defined models that needs to execute before and after model.
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      Paramaters :
      Systemname : System_name 
      DestTbl : Destination Table
      SourceView : Source_View
*/ 
 
 {% macro Main_call(DestTbl,Systemname,SourceView) %}
  {% set new_query_tag = model.name+"_tmp1" %}
  {% set Dest_Table = DestTbl %}
  {% set System_name =  Systemname %}
  {% set Source_view =  SourceView %}
  {% set System_name1 = "'" ~ Systemname  ~ "'" %}
  {% set Dest_Table1 = "'" ~ DestTbl  ~ "'" %}
{{ log("Running Main: " ~ DestTbl ~ ", " ~ System_name1) }}
{{ config(materialized='table' , schema='EDW_DBT_VIEWS',transient=true,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_special',
            pre_hook='{{sbd_edw_main.insert_audit ("' ~ Dest_Table ~ '","' ~ System_name ~ '","' ~ SourceView ~ '")}}',
            post_hook= '{{ sbd_edw_main.get_scd ("' ~ Dest_Table ~ '","' ~ System_name ~ '") }}
                        {{ sbd_edw_main.get_updateloaddate("' ~ System_name ~ '","' ~ Dest_Table ~ '") }}  
                        {{sbd_edw_main.SoftDelete("' ~ Dest_Table ~ '","' ~ System_name ~ '")}}
                        {{sbd_edw_main.insert_auditDetails()}}') }}

with source_data as (
    SELECT * FROM {{ref(SourceView)}}
     WHERE  {{ sbd_edw_main.get_lastloaddate( System_name1 , Dest_Table1 ) }}
    ) 
select *
from source_data
{% endmacro %}