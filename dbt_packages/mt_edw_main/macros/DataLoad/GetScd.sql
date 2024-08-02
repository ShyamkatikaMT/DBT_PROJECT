/*
      This Macro is used for determine SCD type of the destination system in from the  table_metadateloaddt 
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



{% macro get_scd(DestTbl,Sname) %}
    
    {% set table_query %}
        SELECT SCDTYPE FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE SYSTEM_NAME='{{Sname}}' AND DSTTBLNAME='{{DestTbl}}'
    {% endset %}
    {{ log("Entered get_scd:" ~table_query) }}
    {% if execute %}

             {% set getduplicate=sbd_edw_main.Get_Duplicate()%}
             {% set getcolumnscount=sbd_edw_main.Get_DestandSourceColumn(DestTbl)%}
          

            {% if getduplicate=="1" %}
                {{sbd_edw_main.Insert_auditIntermediate('004',var('error_duplicate'))}}
                {{return( false ) }}
            {% elif getcolumnscount!="0" %}
                {{sbd_edw_main.Insert_auditIntermediate('005',var('error_columnmismatch'))}}
                {{return( false ) }}
            {% else %}
                        {%- set result = run_query(table_query).columns[0].values() -%}
                        {%- set  result = result|replace("('", '')|replace("',)", '') -%}
                                {{ log("Running GetScd: " ~ result ) }}
                                {% if result=='SCD2' %}
                                        {{ log("Entered SCD2") }}
                                        {{sbd_edw_main.get_scdSql(DestTbl,Sname)}}
                                        {{return( 'SELECT 1;' )}}
                                {% elif result=='SCD1' %}
                                        {{ log("Entered SCD1") }}
                                        {{sbd_edw_main.Insert_scd1Sql(DestTbl,Sname)}}
                                        {{return( 'SELECT 2; ' )}}
                                {% else %}
                                        {{ log("Entered SCD1_FR") }}
                                        {{sbd_edw_main.FullR_scd1Sql(DestTbl,Sname)}}
                                        {{return( 'SELECT 3; ' )}}
                                {% endif %}
            {% endif %}
                        
     {% else %}
                    {{return( false ) }}
     {% endif %}
    
{% endmacro %}
