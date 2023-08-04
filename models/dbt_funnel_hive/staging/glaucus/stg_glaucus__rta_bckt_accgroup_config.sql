{{ config(materialized='view') }}
with source as (
    select * from {{source('data_ai_glaucus_preagg','prd_glaucus_p_gl_account_decision_conf_sync_pda')}}
),
stgy as ( -- 策略中文名 排老黑2.0/PreA4.0低分高分差异化出价
    select
        config_id,
        bucket,
        input_date,
        date_format(input_date,'yyyyMMdd')input_pday,
        first(strategys_name) strategys_name
    from
(
    select strategyName as strategys_name, agId as config_id, input_date, bucket
    from source LATERAL VIEW EXPLODE(split(bucketIds, ",")) as bucket
    where pday>= date_format(date_sub(date('${pDate}') ,8),'yyyyMMdd')
)
    group by 1,2,3,4
)
select * from stgy
