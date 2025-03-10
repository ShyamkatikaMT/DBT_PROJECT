
{% macro age_dept(column_name, default='DS') %}
CASE WHEN {{column_name}}='DE' THEN '{{default}}' ELSE 'OTHER'
END AS {{column_name}}
{% endmacro%}

