with all_ads as (
  select ad_date,url_parameters, campaign_name, 
    coalesce (spend, impressions, reach, clicks, leads, value,0) as spend,impressions, reach, clicks, leads, value
  from facebook_ads_basic_daily fabd   
    left join facebook_adset on fabd.adset_id=facebook_adset.adset_id 
    left join facebook_campaign on fabd.campaign_id=facebook_campaign.campaign_id 
  union all 
  select ad_date,url_parameters,campaign_name,
    coalesce (spend, impressions, reach, clicks, leads, value,0) as spend,impressions, reach, clicks, leads, value
  from Google_ads_basic_daily
),
  
cte_1 as (
  select 
    date_trunc('month',ad_date) as ad_month,
    url_parameters,
    sum(spend) as total_spend,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(value) as total_value,
    case 
      when url_parameters like '%utm_campaign=nan%' then null
      else lower(substring(url_parameters, 'utm_campaign=([^&#$]+)'))
    end as utm_campaign,

    case 
      when sum(clicks) != 0 then sum(spend) / sum(clicks)
      else 0
    end as cpc,
  
    case 
      when sum(impressions) != 0 then sum(clicks)::numeric / sum(impressions)
      else 0
    end as ctr,

    case
      when sum(clicks) != 0 then 1000 * sum(spend) / sum(clicks)
      else 0  
    end as cpm,
  
    case 
      when sum(spend) != 0 then (sum(value) - sum(spend))::numeric / sum(spend)
      else 0
    end as romi
    
    from all_ads
    group by ad_date, url_parameters
),

final_table as (
  select 
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    utm_campaign,
    ad_month,
    romi,
    cpm,
    cpc,
    ctr,
    case 
      when lag(cpc,1) over(partition by utm_campaign) = 0 then 0
      else (cpc-lag(cpc,1) over(partition by utm_campaign))/lag(cpc,1) over(partition by utm_campaign) 
    end as diff_of_cpc,
    
    case 
      when lag(cpm,1) over(partition by utm_campaign) = 0 then 0
      else (cpm-lag(cpm,1) over(partition by utm_campaign))/lag(cpm,1) over(partition by utm_campaign)
    end as diff_of_cpm,

    case 
      when lag(ctr,1) over(partition by utm_campaign) = 0 then 0
      else (ctr-lag(ctr,1) over(partition by utm_campaign))/lag(ctr,1) over(partition by utm_campaign) 
    end as diff_of_ctr,
    
    case
      when lag(romi,1) over(partition by utm_campaign) = 0 then 0
      else (romi-lag(romi,1) over(partition by utm_campaign))/lag(romi,1) over(partition by utm_campaign)
    end as diff_of_romi
    
  from cte_1
)
select 
  ad_month,
  utm_campaign,
  total_spend,
  total_impressions,
  total_clicks,
  total_value,
  romi,
  cpm,
  cpc,
  ctr,
  diff_of_cpc,
  diff_of_cpm,
  diff_of_ctr,
  diff_of_romi
  
from  final_table;
  
