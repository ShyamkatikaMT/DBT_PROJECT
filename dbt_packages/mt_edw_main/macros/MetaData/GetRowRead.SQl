{% macro get_rowread(SourceTbe) %}

    {% set table_query %}
           
        SELECT NVL(COUNT(1),0) ROWCT FROM {{SourceTbe}} 
    {% endset %}
    
          
                        {% if execute %}
                        {%- set result = run_query(table_query).columns[0].values() -%}
                         {%- set  result = result|replace("('", '')|replace("',)", '') -%}
                            {{return(result)}}
                        {% else %}
                            {{return( false ) }}
                        {% endif %}
                            
{% endmacro %}