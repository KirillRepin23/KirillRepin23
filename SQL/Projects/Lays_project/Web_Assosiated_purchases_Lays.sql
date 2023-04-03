WITH basic_purchases AS
  (
  SELECT
    date
    ,clientId
    ,trafficSource.campaign
    ,trafficSource.source
    ,trafficSource.medium
    ,h.product
  FROM `radiant-entry-230109.OWOXBI_Streaming.owoxbi_sessions_202*`, UNNEST(hits) h
  WHERE
    (_TABLE_SUFFIX BETWEEN '21118' AND '21130' OR _TABLE_SUFFIX BETWEEN '21201' AND '21231' OR _TABLE_SUFFIX BETWEEN '30101' AND '30109') 
    AND h.ecommerceAction.action_type = 'purchase'
    AND h.transaction.transactionId IS NOT NULL
  ),

lays_purchases as
  (
  SELECT
    date
    ,clientId
    ,campaign
    ,source
    ,medium
    ,p.productName
    ,p.productQuantity
    ,p.productPrice
    ,p.productQuantity * p.productPrice AS lays_sum_cost
    ,p.productSKU
    ,p.Id
  FROM basic_purchases, UNNEST(product) p
  WHERE
    REGEXP_CONTAINS(p.productName, r"Lay's|Lays")
  ),

basic_atr AS
  (
  SELECT
    date
    ,clientId
    ,trafficSource.campaign
    ,trafficSource.source
    ,h.type
  FROM `radiant-entry-230109.OWOXBI_Streaming.owoxbi_sessions_202*` , UNNEST(hits) h
  WHERE
    1=1
    AND (_TABLE_SUFFIX BETWEEN '21118' AND '21130' OR _TABLE_SUFFIX BETWEEN '21201' AND '21231' OR _TABLE_SUFFIX BETWEEN '30101' AND '30109') 
    AND (h.type = 'pageview' OR h.type = 'event' OR h.type = 'timing')
   ),

agg_basic_atr AS
  (
  SELECT
    clientId
    ,ARRAY_AGG(campaign) AS agg_campaign
  FROM basic_atr
  GROUP BY
    1
  ),


final AS
  (
  SELECT
    l.date AS purchase_date
    ,l.clientId AS clientId
    ,l.source AS last_click_source
    ,r.agg_campaign AS agg_campaign
    ,l.productName AS productName
    ,l.productQuantity AS productQuantity
    ,l.productPrice AS productPrice
    ,l.lays_sum_cost AS lays_sum_cost
    ,l.id AS productId
    ,l.productSKU AS product_SKU
  FROM lays_purchases AS l
  LEFT JOIN agg_basic_atr AS r 
  ON l.clientId = r.clientId
  WHERE
    'y-search-dsa-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign)
    OR
    'y-search-tgo-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign)
    OR
    'y-network-tgo-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign)
    OR
    'snacks|gp|kw|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'snacks|gp|followers|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'snacks|gp|int-buyers|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'msk-spb|dr-snacks_int|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'msk-spb|dr-snacks_base|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'msk-spb|dr-snacks_kt|1845|n|cpc' IN UNNEST(agg_campaign)
    OR
    'yandex_cpm|banner_rtb|vprok_snacks' IN UNNEST(agg_campaign)
    OR
    'yandex_cpa|banner_rtb|vprok_snacks' IN UNNEST(agg_campaign)
  ORDER BY date DESC
  )


SELECT
  CASE
    WHEN 'y-search-dsa-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign) THEN 'y-search-dsa-snacks_1122-aud_all-all-day-rf'
    WHEN 'y-network-tgo-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign) THEN 'y-network-tgo-snacks_1122-aud_all-all-day-rf'
    WHEN 'y-search-tgo-snacks_1122-aud_all-all-day-rf' IN UNNEST(agg_campaign) THEN 'y-search-tgo-snacks_1122-aud_all-all-day-rf'
    WHEN 'snacks|gp|kw|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'snacks|gp|kw|1845|n|cpc'
    WHEN 'snacks|gp|followers|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'snacks|gp|followers|1845|n|cpc'
    WHEN 'snacks|gp|int-buyers|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'snacks|gp|int-buyers|1845|n|cpc'
    WHEN 'snacks|gp|int-buyers|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'snacks|gp|int-buyers|1845|n|cpc'
    WHEN 'msk-spb|dr-snacks_int|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'msk-spb|dr-snacks_int|1845|n|cpc'
    WHEN 'msk-spb|dr-snacks_base|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'msk-spb|dr-snacks_base|1845|n|cpc'
    WHEN 'msk-spb|dr-snacks_kt|1845|n|cpc' IN UNNEST(agg_campaign) THEN 'msk-spb|dr-snacks_kt|1845|n|cpc'
    WHEN 'yandex_cpm|banner_rtb|vprok_snacks' IN UNNEST(agg_campaign) THEN 'yandex_cpm|banner_rtb|vprok_snacks'
    WHEN 'yandex_cpa|banner_rtb|vprok_snacks' IN UNNEST(agg_campaign) THEN 'yandex_cpa|banner_rtb|vprok_snacks'
    END AS associated_campaign
   ,product_SKU
   ,productName,
   SUM(CAST(productQuantity AS INT64)) quantity
  ,ROUND(SUM(lays_sum_cost),2) lays_sum_cost  
FROM final
GROUP BY
 1,2,3
