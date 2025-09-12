{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* '_PRO' THEN 'Campaign Type: Prospecting'
    WHEN campaign_name ~* '_RET' THEN 'Campaign Type: Retargeting'
END AS campaign_type_default,
CASE WHEN campaign_name ~* '_NYC' THEN 'NYC'
    WHEN campaign_name ~* '_BayArea' THEN 'Bay Area'
    WHEN campaign_name ~* '_Seattle' THEN 'Seattle'
    WHEN campaign_name ~* '_Chicago' THEN 'Chicago'
    WHEN campaign_name ~* '_LA' THEN 'LA'
END AS region,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
"offsite_conversion.fb_pixel_custom.meal-planning-mvp-purchase" as purchases,
"offsite_conversion.fb_pixel_custom.meal-planning-mvp-purchase_value" as revenue
FROM {{ ref('facebook_performance_by_campaign') }}
