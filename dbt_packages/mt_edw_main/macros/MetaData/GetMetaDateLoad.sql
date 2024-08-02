{% macro get_lastloaddate(Sname,DestTable) %}
   {% set tbl_query1 %}
            SELECT DISTINCT ISSQORDAY FROM  {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE  
            SYSTEM_NAME={{Sname}}  AND DSTTBLNAME={{DestTable}};
    {% endset %}

        {% if execute %}
            {%- set result = run_query(tbl_query1).columns[0].values()[0] -%}

            {% if result=='D'  %}
                        {{var('column_z3loddtm')}}::DATE =(SELECT {{var('column_loadmetadts')}}::DATE FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE SYSTEM_NAME={{Sname}} AND DSTTBLNAME={{DestTable}})
            {% else %}
                        {{var('column_z3loddtm')}} >(SELECT {{var('column_loadmetadts')}} FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE SYSTEM_NAME={{Sname}} AND DSTTBLNAME={{DestTable}})
                        
             {% endif %}
        {% endif %}
 {% endmacro %}

 {% macro get_lastloaddate4src(Sname,DestTable) %}
   {% set new_query_tag = model.name+"_tmp1" %}
   {% set tbl_query1 %}
            SELECT DISTINCT ISSQORDAY FROM  {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE  
             SYSTEM_NAME={{Sname}}  AND DSTTBLNAME={{DestTable}};
    {% endset %}

        {% if execute %}
            {%- set result = run_query(tbl_query1).columns[0].values()[0] -%}

            {% if result=='D'  %}
                        {{var('column_loaddts')}}::DATE =(SELECT {{var('column_loadmetadts')}}::DATE FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} WHERE SYSTEM_NAME={{Sname}} AND DSTTBLNAME={{DestTable}})
            {% else %}
                        {{var('column_loaddts')}} >(SELECT {{var('column_loadmetadts')}} FROM {{var('db_destschema')}}.{{var('table_metadateloaddt')}} 
                        WHERE SYSTEM_NAME={{Sname}} AND DSTTBLNAME={{DestTable}})
                        AND A.{{var('column_loaddts')}}<=(SELECT MAX(ZONE3_LOD_DTE) FROM  {{var('db_srcviewdb')}}.{{new_query_tag}})
                        
             {% endif %}
        {% endif %}
 {% endmacro %}