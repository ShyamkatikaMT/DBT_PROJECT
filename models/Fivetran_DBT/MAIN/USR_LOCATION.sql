{{
    config(
        materialized='incremental'
    )
}}
SELECT * FROM {{ref('USR_BNG_LOCATIONS_VW')}}
union all
SELECT * FROM {{ref('USR_HYD_LOCATIONS_VW')}}
union all
SELECT * FROM {{ref('USR_KLP_LOCATIONS_VW')}}
union all
SELECT * FROM {{ref('USR_VSKP_LOCATIONS_VW')}}