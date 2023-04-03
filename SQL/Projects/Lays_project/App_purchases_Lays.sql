WITH basic AS
  (
  SELECT
    event_time_selected_timezone AS event_time
    ,campaign
    ,REGEXP_EXTRACT(event_value,r'\[\{\"(.+?)\"\}\]') raw_text
  FROM `radiant-entry-230109.appsflayer_events.*`
  WHERE 
    _TABLE_SUFFIX in ('ios_events','android_events')
    AND (event_name = 'af_purchase' )
    AND date(Event_Time_selected_timezone) between '2022-11-17' AND '2023-01-09'
    AND media_source != 'organic'
    AND (REGEXP_CONTAINS(LOWER(campaign), r'realweb')
    AND REGEXP_CONTAINS(LOWER(campaign), r'snacks') OR  REGEXP_CONTAINS(LOWER(campaign), r'/[snacks/]'))
   ),

next AS
  (
  SELECT
    event_time
    ,campaign
    ,SPLIT(raw_text,'"},{"') AS pairs
  FROM basic
  ),

prefinal AS
  (
  SELECT
    event_time
    ,campaign
    ,REPLACE(REGEXP_EXTRACT(p, r'id\"\:\"\d*'), 'id":"', '') productId
    ,REPLACE(REGEXP_EXTRACT(p, r'quantity\"\:\"\w*'), 'quantity":"', '') quantity
  FROM next, UNNEST(pairs) AS p
  ),

final AS
  (
  SELECT
    event_time
    ,campaign
    ,productId
    ,quantity
    ,productname
  FROM prefinal
  LEFT JOIN perekrestokvprok-bq.test_dataset.lays_id -- Это табличка, где лежит список ID товаров Lays и соответствующих названий товаров Lays
  ON productId = product_id
  )

SELECT
  campaign
  ,productId
  ,productname  
  ,SUM(CAST(quantity AS INT64)) AS total_quantity
FROM final
WHERE
 productname IS NOT NULL
GROUP BY
  1,2,3