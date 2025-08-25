{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_amplitude_data as
  (SELECT 'NYC' as region, date::date as date, COALESCE(SUM(new_york_ny),0) as subscribers
  FROM {{ source('gsheet_raw','amplitude_orders_byregion') }}
  GROUP BY 1,2
  UNION ALL
  SELECT 'Bay Area' as region, date::date as date, COALESCE(SUM(san_francisco_ca),0) as subscribers
  FROM {{ source('gsheet_raw','amplitude_orders_byregion') }}
  GROUP BY 1,2),
  
dg_amplitude_data as
  (SELECT *, {{ get_date_parts('date') }} FROM initial_amplitude_data),

amplitude_data as 
    ({%- for date_granularity in date_granularity_list %}    
      SELECT 
          'Amplitude' as channel,
          '{{date_granularity}}' as date_granularity,
          {{date_granularity}} as date,
          region, 
          null as campaign_type_default, null as campaign_name, null as landing_page,
          SUM(0) as spend, SUM(0) as clicks, SUM(0) as impressions, SUM(0) as purchases, SUM(0) as revenue,
          COALESCE(SUM(subscribers),0) as subscribers
      FROM dg_amplitude_data
      GROUP BY 1,2,3,4,5,6,7
      {% if not loop.last %}UNION ALL
      {% endif %}
    {% endfor %}),

paid_data as
    (SELECT channel, date_granularity, date::date, region, campaign_type_default, campaign_name, landing_page,
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue, SUM(0) as subscribers
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, region, campaign_type_default, campaign_name, landing_page,
            spend, link_clicks as clicks, impressions, purchases, revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, region, campaign_type_default, campaign_name, landing_page,
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','googleads_ad_performance') }}
        )
    GROUP BY 1,2,3,4,5,6,7)
  
SELECT channel,
    date,
    date_granularity,
    region,
    campaign_type_default,
    campaign_name,
    landing_page,
    spend,
    clicks,
    impressions,
    purchases,
    revenue,
    subscribers
FROM 
  (SELECT * FROM paid_data
  UNION ALL 
  SELECT * FROM amplitude_data)
