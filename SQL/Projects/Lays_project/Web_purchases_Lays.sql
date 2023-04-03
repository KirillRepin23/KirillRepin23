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
    ,FIRST_VALUE(trafficSource.campaign) OVER w AS lnd_click_campaign
    ,FIRST_VALUE(date) OVER w AS lnd_click_date
  FROM `radiant-entry-230109.OWOXBI_Streaming.owoxbi_sessions_202*` , UNNEST(hits) h
  WHERE
    1=1
    AND (_TABLE_SUFFIX BETWEEN '21118' AND '21130' OR _TABLE_SUFFIX BETWEEN '21201' AND '21231' OR _TABLE_SUFFIX BETWEEN '30101' AND '30109') 
    AND (h.type = 'pageview' OR h.type = 'event' OR h.type = 'timing')
    AND trafficSource.source != '(direct)' 
    AND trafficSource.source != 'direct'
    AND trafficSource.campaign != '(not set)'
    AND trafficSource.medium != 'organic'
  WINDOW w AS (PARTITION BY clientId ORDER BY date DESC)
  ORDER BY
    clientId
    ,date DESC
  ),

agg_basic_atr AS
  (
  SELECT
    clientId
    ,lnd_click_campaign
    ,lnd_click_date
  FROM basic_atr
  GROUP BY
    1,2,3
  ),

final AS
  (
  SELECT
    l.date AS purchase_date
    ,l.clientId AS clientId
    ,l.campaign AS last_click_campaign
    ,l.source AS last_click_source
    ,r.lnd_click_campaign AS lnd_click_campaign
    ,r.lnd_click_date AS lnd_click_date
    ,CASE
      WHEN (l.campaign = '(not set)' OR l.campaign IS NULL) THEN r.lnd_click_campaign
        ELSE l.campaign
      END AS final_lnd_click_campaign
    ,l.productName AS productName
    ,l.productQuantity AS productQuantity
    ,l.productPrice AS productPrice
    ,l.lays_sum_cost AS lays_sum_cost
    ,l.id AS productId
    ,l.productSKU AS product_SKU
  FROM lays_purchases AS l
  LEFT JOIN agg_basic_atr AS r 
  ON l.clientId = r.clientId AND (l.date >= r.lnd_click_date)
  ORDER BY date DESC
  )

SELECT
  final_lnd_click_campaign AS campaign
  ,product_SKU
  ,productName
  ,SUM(CAST(productQuantity AS INT64)) lays_quantity_cost
  ,ROUND(SUM(lays_sum_cost),2) lays_sum_cost  
FROM final
WHERE
  final_lnd_click_campaign IN ('y-search-dsa-snacks_1122-aud_all-all-day-rf',
                                'y-search-tgo-snacks_1122-aud_all-all-day-rf',
                                'y-network-tgo-snacks_1122-aud_all-all-day-rf',
                                'snacks|gp|kw|1845|n|cpc',
                                'snacks|gp|followers|1845|n|cpc',
                                'snacks|gp|int-buyers|1845|n|cpc',
                                'msk-spb|dr-snacks_int|1845|n|cpc',
                                'msk-spb|dr-snacks_base|1845|n|cpc',
                                'msk-spb|dr-snacks_kt|1845|n|cpc',
                                'yandex_cpm|banner_rtb|vprok_snacks',
                                'yandex_cpa|banner_rtb|vprok_snacks')
GROUP BY
  1,2,3