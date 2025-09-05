{{ config (
    alias = target.database + '_googleads_asset_group_performance'
)}}

SELECT 
account_id,
campaign_name,
ag.campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Max' THEN 'Campaign Type: Performance Max'
    ELSE campaign_type_default
END AS campaign_type_default,
CASE WHEN campaign_name ~* '_NYC' THEN 'NYC'
    WHEN campaign_name ~* '_BayArea' THEN 'Bay Area'
END AS region,
asset_group_name,
ag.asset_group_id,
asset_group_status,
landing_page,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue
FROM {{ ref('googleads_performance_by_asset_group') }} ag
LEFT JOIN {{ source('gsheet_raw','google_landing_pages') }} lp ON ag.asset_group_id::varchar = lp.ad_asset_group_id::varchar
