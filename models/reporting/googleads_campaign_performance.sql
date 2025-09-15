{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Max' THEN 'Campaign Type: Performance Max'
    ELSE campaign_type_default
END AS campaign_type_default,
CASE WHEN campaign_name ~* '_NYC' THEN 'NYC'
    WHEN campaign_name ~* '_BayArea' THEN 'Bay Area'
    WHEN campaign_name ~* '_Seattle' THEN 'Seattle'
    WHEN campaign_name ~* '_Chicago' THEN 'Chicago'
    WHEN campaign_name ~* '_LA' THEN 'LA'
    ELSE 'Multiple'
END AS region,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign') }}
