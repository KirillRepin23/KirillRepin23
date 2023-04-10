WITH Check_Order_id AS               -- проверка на NULL значения, повторы, MAX и MIN значения
(
  SELECT                                                 
    "order_id" as Column_Name,                                -- столбец order_id; если нужно взять другие - подставить соответствующее название здесь и вследующих строках
    COUNT(*) as cnt,                                          
    COUNT (DISTINCT order_id) AS cnt_distinct,
    --APPROX_COUNT_DISTINCT(order_id) AS cnt_aprox_distinct,
    SUM(IF (user_id IS NULL, 1, 0)) AS sum_null,
    MAX(order_id) AS max,
    MIN(order_id) AS min
  FROM `bigquery-public-data.thelook_ecommerce.orders`  
),

Orders_Joined AS                     -- Пример, если нужно взять данные из двух таблиц

(
  SELECT 
    ord.order_id,
    Date(ord.created_at) AS Date,
    ord.user_id,
    SUM(ROUND(itm.sale_price, 2) * ord.num_of_item) as Check_Price,
    Date(MAX(ord.created_at) OVER() + INTERVAL "1" DAY) AS Now
  FROM `bigquery-public-data.thelook_ecommerce.orders`  as ord
  JOIN `bigquery-public-data.thelook_ecommerce.order_items` as itm
    ON ord.order_id = itm.order_id
    AND Date(ord.created_at) = Date(itm.created_at)
    AND ord.user_id = itm.user_id
  WHERE itm.returned_at IS NULL
  GROUP BY ord.order_id,
    ord.created_at,
    ord.user_id
),

Orders AS

(  
  SELECT
    order_id,                                                    -- номер заказа
    Date(ord.created_at) AS Date,                                -- дата заказа
    user_id,                                                     -- UserID
    SUM(ROUND(sale_price, 2) * num_of_item) as Check_Price,      -- стоимость товара * кол-во
    Date(MAX(created_at) OVER() + INTERVAL "1" DAY) AS Now       -- Сегодня = дата последнего заказа + 1 день
  FROM 
    sales
  WHERE 
    user_id IS NOT NULL
  GROUP BY order_id,
    created_at,
    user_id
)

Recency_Frequency_Monetary AS                 

(
  SELECT
    user_id,
    COUNT(DISTINCT order_id) AS Frequency,
    MIN(date_diff(Now, Date, Day)) AS Recency,
    SUM(Check_Price) AS Monetary
  FROM
    Orders
  WHERE Date >= NOW - INTERVAL "365" Day
  GROUP BY user_id
),

Percentiles AS 

(
  SELECT
    APPROX_QUANTILES(Frequency, 100)[OFFSET(20)] AS F_20,
    APPROX_QUANTILES(Frequency, 100)[OFFSET(40)] AS F_40,
    APPROX_QUANTILES(Frequency, 100)[OFFSET(60)] AS F_60,
    APPROX_QUANTILES(Frequency, 100)[OFFSET(80)] AS F_80,

    APPROX_QUANTILES(Recency, 100)[OFFSET(20)] AS R_20,
    APPROX_QUANTILES(Recency, 100)[OFFSET(40)] AS R_40,
    APPROX_QUANTILES(Recency, 100)[OFFSET(60)] AS R_60,
    APPROX_QUANTILES(Recency, 100)[OFFSET(80)] AS R_80,

    APPROX_QUANTILES(Monetary, 100)[OFFSET(20)] AS M_20,
    APPROX_QUANTILES(Monetary, 100)[OFFSET(40)] AS M_40,
    APPROX_QUANTILES(Monetary, 100)[OFFSET(60)] AS M_60,
    APPROX_QUANTILES(Monetary, 100)[OFFSET(80)] AS M_80
  FROM Recency_Frequency_Monetary
),

RFM AS 

(
  SELECT
    user_id,
    Recency,
    Frequency,
    Monetary,
    CASE
       WHEN Recency <= R_20 THEN 5
       WHEN Recency <= R_40 THEN 4
       WHEN Recency <= R_60 THEN 3
       WHEN Recency <= R_80 THEN 2
       ELSE 1
    END AS R,
    CASE
       WHEN Frequency <= F_20 THEN 1
       WHEN Frequency <= F_40 THEN 2
       WHEN Frequency <= F_60 THEN 3
       WHEN Frequency <= F_80 THEN 4
       ELSE 5
    END AS F,
    CASE
       WHEN Monetary <= M_20 THEN 1
       WHEN Monetary <= M_40 THEN 2
       WHEN Monetary <= M_60 THEN 3
       WHEN Monetary <= M_80 THEN 4
       ELSE 5
    END AS M
  FROM Recency_Frequency_Monetary
  CROSS JOIN Percentiles

),

RFM_Map AS 

(
  (SELECT '[1-2][1-2]' AS Key, 'hibernating' AS Value)

  UNION ALL

  (SELECT '[1-2]5' AS Key, 'can not lose' AS Value)

  UNION ALL

  (SELECT '3[1-2]' AS Key, 'about to sleep' AS Value)

  UNION ALL

  (SELECT '33' AS Key, 'need attention' AS Value)

  UNION ALL

  (SELECT '[3-4][4-5]' AS Key, 'loyal customers' AS Value)

  UNION ALL

  (SELECT '41' AS Key, 'promising' AS Value)

  UNION ALL

  (SELECT '51' AS Key, 'new customers' AS Value)

  UNION ALL

  (SELECT '[4-5][2-3]' AS Key, 'potential loyalists' AS Value)

  UNION ALL

  (SELECT '5[4-5]' AS Key, 'champions' AS Value)
),  

RFM_Segmented AS 

(
  SELECT 
    *,
    concat(R, F, M) AS RFM_Score
  FROM RFM
  CROSS JOIN RFM_Map
  WHERE REGEXP_CONTAINS(CONCAT(R, F), Key)
  ORDER BY user_id
)

SELECT 
  * 
FROM 
  RFM_Segmented