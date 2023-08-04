{{ config(materialized='view') }}

with source as (

   select * from {{ ref('stg_glaucus__rta_req_media_bckt_accgroup_pvuv_agg') }}

),
stgy as (
  select * from {{ ref('stg_glaucus__rta_bckt_accgroup_config')}}
),
request as (
select date_format(a.dt,'yyyyMMdd')as p_day,
  , case when a.media ='腾讯' then '微信' else a.media as media
  , coalesce(req.bucket,'无')bucket
  , coalesce(req.config_id,'无')config_id
  , coalesce(cfg.strategys_name,'无')strategys_name
  , request_pv
  , release_pv
  , request_uv
  , release_uv
from source a left join stgy cfg2
  on a.config_id = cfg2.config_id and a.bucket=cfg2.bucket --and date_format(a.dt,'yyyyMMdd')=cfg2.input_pday
    and from_unixtime(unix_timestamp(cfg2.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(a.dt,1)
where dt >=date_sub(date('${pDate}') ,7)
)

select * from request
