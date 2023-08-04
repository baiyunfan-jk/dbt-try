{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_bucket_cfgid_cost') }} 

),
stgy as (
    select * from {{ref('stg_glaucus__rta_bckt_accgroup_config')}}
),
spend as (
    select date_format(a.dt,'yyyyMMdd')as pday,
    case when qd='微信MP' then '微信' else qd end as media,--广点通,微信MP,穿山甲,头条,null,快手
    coalesce(a.bucket,'无')bucket,
    coalesce(a.config_id,'无')config_id,
    coalesce(cfg2.strategys_name,'无')strategys_name,
    costs
    from source a left join stgy cfg2
        on a.config_id = cfg2.config_id 
            and a.bucket=cfg2.bucket --and date_format(a.dt,'yyyyMMdd')=cfg2.input_pday
            and from_unixtime(unix_timestamp(cfg2.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(a.dt,1)
    where a.dt >=date_sub(date('${pDate}') ,7)
)
select * from spend
