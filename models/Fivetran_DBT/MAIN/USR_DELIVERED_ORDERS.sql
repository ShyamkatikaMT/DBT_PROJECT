{{
    config(
        materialized='incremental'
    )
}}
SELECT * FROM {{ref('USR_BNG_DELIVERED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_HYD_DELIVERED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_KLP_DELIVERED_ORDERS_VW')}}
union all
SELECT * FROM {{ref('USR_VSKP_DELIVERED_ORDERS_VW')}}