{{
    config(
        materialized='incremental'
    )
}}
SELECT * FROM {{ref('USR_BNG_OPEN_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_HYD_OPEN_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_KLP_OPEN_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_VSKP_OPEN_ORDERS_VW')}}