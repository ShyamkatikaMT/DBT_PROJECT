
{#
{% set my_name ='Shyam Sundar' %}
{% set my_age =24 %}

{{my_name}} age is {{my_age}}


{% set animals=['Elephant','Lion','deer','cow']%}

{% for i in animals%}
    {% if{{i}} == 'Lion' %}
        wild
    {% else %}
        non-wild
    {% end if %}
{% endfor %}


{% set animals = ['Elephant', 'Lion', 'deer', 'cow'] %}

{% for i in animals %}
    {% if i == 'Lion' %}
        wild
    {% else %}
        non-wild
    {% endif %}
{% endfor %}
#}

{%- set animals = ['Elephant', 'Lion', 'deer', 'cow'] -%}

{%- for i in animals -%}
    {%- if i == 'Lion' -%}
        {%- set type= 'wild' -%}
    {{type}}
    {%- else -%}
        {%- set type= 'non_wild' -%}
    {{type}}
    {% endif %}
{% endfor %}


