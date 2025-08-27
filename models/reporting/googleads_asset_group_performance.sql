{{ config (
    alias = target.database + '_googleads_asset_group_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
asset_group_name,
asset_group_id::varchar,
asset_group_status,
landing_page,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue
FROM {{ ref('googleads_performance_by_asset_group') }}
LEFT JOIN 
    (SELECT count(*), landing_page, ad_asset_group_id::varchar as asset_group_id FROM {{ source('gsheet_raw','google_landing_pages') }} GROUP BY 2,3) USING(asset_group_id)
