{% macro get_CurrentSessionID() %}

    {% set table_query %}
       
        select current_session() AS SESSIONID
    {% endset %}
    
          
                        {% if execute %}
                        {%- set result = run_query(table_query).columns[0].values()[0] -%}
                            {{return(result)}}
                        {% else %}
                            {{return( false ) }}
                        {% endif %}
                            
{% endmacro %}