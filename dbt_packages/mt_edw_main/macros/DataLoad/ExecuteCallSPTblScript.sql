/*
      This Macro will call and create stored procedure, also creates destination tables 
      Author :
      Creation Date : 12/15/2021
      Change History
      =========================================================================================================
      Updateion Date                  Updated By                  Comment
      ==========================================================================================================
      12/15/2021                      Pradeep                     Inital Draft

      
*/

 {% macro SP_call(spname) %}
  {% set new_query_tag = model.name+"_tmp1" %}
    {{ config(materialized='table',transient=true,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_special',
            pre_hook='{{ sbd_edw_main.runprocedure("' ~ spname ~ '")}}')}}
            

    with source_data as (
               Select '1' as number
        ) 
    
    select *
    from source_data
{% endmacro %}


{% macro SP_Create() %}
  {% set new_query_tag = model.name+"_tmp1" %}
    {{ config(materialized='table',transient=true,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_special',
            pre_hook='{{sbd_edw_main.Create_Procedure_objectrs()}}')}}
            

    with source_data as (
        Select '1' as number
        ) 
    select *
    from source_data
{% endmacro %}


{% macro Tbl_Create() %}
  {% set new_query_tag = model.name+"_tmp1" %}
    {{ config(materialized='table',transient=true,
            alias=new_query_tag,
            tags=["daily"],
            query_tag='dbt_special',
            pre_hook='{{sbd_edw_main.Create_Table_objectrs()}}')}}
            

    with source_data as (
        Select '1' as number
        ) 
    select *
    from source_data
{% endmacro %}



{% macro runprocedure(spname) %}

{% set execpro=spname|replace("@@", "'") %}
   
{{ log("Running  Updte_audit: " ~ execpro ~ ", " ~ spname) }}
          call CONSOLIDATED.{{execpro}}

{% endmacro %}