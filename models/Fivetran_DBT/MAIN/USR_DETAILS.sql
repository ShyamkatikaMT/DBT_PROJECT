{{
    config(
        materialized='incremental'
    )
}}
SELECT * FROM {{ref('USR_BNG_DETAILS_VW')}}
union all
SELECT * FROM {{ref('USR_HYD_DETAILS_VW')}}
union all
SELECT * FROM {{ref('USR_KLP_DETAILS_VW')}}
union all
SELECT * FROM {{ref('USR_VSKP_DETAILS_VW')}}