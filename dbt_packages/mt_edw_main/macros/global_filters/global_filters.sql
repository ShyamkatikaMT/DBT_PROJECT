{% macro get_otc_globalfilter_values( application_name, source_system, variable_name, mode) %}
        
        {% set global_filter_where %}
             1 = 1
                     {% if application_name is not none %}
                            and  FILTERCATEGORY in ('{{ application_name }}')
                     {% endif %}       
                     {% if source_system is not none %}
                                                 and SOURCESYS in ('{{ source_system }}')
                     {% endif %}
                     {% if source_system is not none %}
                            and FIELD in ('{{ variable_name }}')
                     {% endif %}
                     {% if mode is not none %}
                            and CRITERIA in ('{{ mode }}')                          
                     {% endif %}
        {% endset %}
       {{ log(" NEW get_otc_globalfilter_values : " ~  global_filter_where, info = false) }}

       {% if mode == 'I' or mode == 'E' %}
              {% set global_filter_values = dbt_utils.get_column_values(table=source('EDW_REFDATA','EDW_OTC_GLOBAL_FILTERS'), column='VALUE' , where=global_filter_where) | replace("[","(") | replace("]",")")    %}
       {% elif mode == 'LIKE' or mode == 'EQ' %}
              {% set global_filter_values = dbt_utils.get_column_values(table=source('EDW_REFDATA','EDW_OTC_GLOBAL_FILTERS'), column='VALUE' , where=global_filter_where) | replace("[","") | replace("]","")    %}
       {% elif mode == 'MAP' %}
              {% set global_filter_values = dbt_utils.get_column_values(table=source('EDW_REFDATA','EDW_OTC_GLOBAL_FILTERS'), column='VALUE' , where=global_filter_where)    %}
       {% endif %}

       {{ log("NEW get_otc_globalfilter_values : " ~  global_filter_values, info = false) }}
        

       {{ return(global_filter_values) }}


{% endmacro %}  -->



