{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* '_PRO' THEN 'Campaign Type: Prospecting'
    WHEN campaign_name ~* '_RET' THEN 'Campaign Type: Retargeting'
END AS campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
"offsite_conversion.fb_pixel_custom.meal-planning-mvp-purchase" as purchases,
"offsite_conversion.fb_pixel_custom.meal-planning-mvp-purchase_value" as revenue
FROM {{ ref('facebook_performance_by_ad') }}
