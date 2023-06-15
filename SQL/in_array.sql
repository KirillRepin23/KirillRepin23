SELECT
  EventDate
  ,CASE
    WHEN REGEXP_CONTAINS(Campaign, r'p01') OR REGEXP_CONTAINS(Campaign, r'and') THEN 'Android'
    WHEN REGEXP_CONTAINS(Campaign, r'p02') OR REGEXP_CONTAINS(Campaign, r'ios') THEN 'iOS'
    WHEN REGEXP_CONTAINS(Campaign, r'p03') THEN 'Onelink'
  END AS platform
  ,CASE 
    WHEN MediaSource IN UNNEST(ARRAY(SELECT DISTINCT media_source 
                                     FROM `amediateka-bq.Rates_Limits_Plans.Rates_static_actual`
                                     WHERE DATE_TRUNC(EventDate, month) = DATE_TRUNC(date(start_date), month)
                                     )) THEN MediaSource 
    ELSE 'Other'
   END AS Media_Source
  ,CASE
    WHEN REGEXP_CONTAINS(Campaign, r'_a485') THEN '2leads'
    WHEN REGEXP_CONTAINS(Campaign, r'_a617') THEN 'Borscht'
    WHEN REGEXP_CONTAINS(Campaign, r'_a564') THEN 'Colead'
    WHEN REGEXP_CONTAINS(Campaign, r'_a339') THEN 'GoMobile'
    WHEN REGEXP_CONTAINS(Campaign, r'_a776') THEN 'Gradientt'
    WHEN REGEXP_CONTAINS(Campaign, r'_a999') THEN 'In-house'
    WHEN REGEXP_CONTAINS(Campaign, r'_a499') THEN 'Mediaserfer'
    WHEN REGEXP_CONTAINS(Campaign, r'_a224') THEN 'Mobisharks'
    WHEN REGEXP_CONTAINS(Campaign, r'_a823') THEN 'MobX'
    WHEN REGEXP_CONTAINS(Campaign, r'_a662') THEN 'Rocket10'
    WHEN REGEXP_CONTAINS(Campaign, r'_a799') THEN 'ThinkMobile'
    WHEN REGEXP_CONTAINS(Campaign, r'_a391') THEN 'TopTraffic'
    WHEN REGEXP_CONTAINS(Campaign, r'_a888') THEN 'Mobio'
    WHEN REGEXP_CONTAINS(Campaign, r'_c212') THEN 'beta vk'
    WHEN REGEXP_CONTAINS(Campaign, r'_a382') THEN 'MobUpps'
    WHEN REGEXP_CONTAINS(Campaign, r'_a773') THEN 'Promotix'
    WHEN REGEXP_CONTAINS(Campaign, r'_a111') THEN 'xapads'
   END AS partner
  ,CASE
    WHEN REGEXP_CONTAINS(Campaign, r'_c210') THEN 'in-app'
    WHEN REGEXP_CONTAINS(Campaign, r'_c208') THEN 'Yandex.Direct'
    WHEN REGEXP_CONTAINS(Campaign, r'_c207') THEN 'VK'
   END AS source
  ,CASE
    WHEN REGEXP_CONTAINS(Campaign, r'm405') THEN 'cpa'
    WHEN REGEXP_CONTAINS(Campaign, r'm404') THEN 'cpi'
   END AS cost_model
  ,CASE
    WHEN REGEXP_CONTAINS(Campaign, r'u1') THEN 'UA'
    WHEN REGEXP_CONTAINS(Campaign, r'u2') THEN 'RET'
   END AS campaign_type
  ,'af_trial_subscribe_payture' AS event_type
  ,COUNT(EventName) AS events
FROM `amediateka-bq.Main.appsflyer_amdteka` 
WHERE
  EventName = "af_trial_subscribe_payture"
GROUP BY
  1,2,3,4,5,6,7
ORDER BY
  EventDate ASC