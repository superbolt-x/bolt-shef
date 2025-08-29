{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
landing_page,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Max' THEN 'Campaign Type: Performance Max'
    ELSE campaign_type_default
END AS campaign_type_default,
CASE WHEN campaign_name ~* '_NYC' THEN 'NYC'
    WHEN campaign_name ~* '_BayArea' THEN 'Bay Area'
END AS region,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue
FROM {{ ref('googleads_performance_by_ad') }} ad
LEFT JOIN {{ source('gsheet_raw','google_landing_pages') }} lp 
    ON ad.ad_id::varchar = lp.ad_id::varchar AND ad.ad_group_id::varchar = lp.ad_asset_group_id::varchar AND ad.campaign_id::varchar = lp.campaign_id::varchar
