{{
    config(
        materialized='incremental'
    )
}}
SELECT * FROM {{ref('USR_BNG_PICKED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_HYD_PICKED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_KLP_PICKED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_VSKP_PICKED_ORDERS_VW')}}