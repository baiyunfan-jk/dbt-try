{{ config(materialized='view') }}
with source as (

    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_req_new_config_bckt') }} 

),
request as (
     select dt
     ,media
     ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
     ,bucket
     ,sum(request_pv)request_pv
     ,sum(release_pv)release_pv
     ,sum(request_uv)request_uv
     ,sum(release_uv)release_uv
     from source
     where dt>=date_sub(date('${pDate}') ,7)--V7改: 头条和快手，加：'百度开屏' '百度百青藤' '百度'
     group by 1,2,3,4
    )
    
select * from request
