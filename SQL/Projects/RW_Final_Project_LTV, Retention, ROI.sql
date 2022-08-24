WITH
-- Джойним af_purchase_events и crm_data по дате покупки и order_id, фильтруя данные, где нет инфо об источнике или платформе или дате установки 
  events_orders AS
    (
    SELECT 
      l.install_date AS install_date
      ,l.event_date AS event_date
      ,l.media_source AS media_source
      ,l.appsflyer_id AS appsflyer_id
      ,l.order_id AS order_id
      ,l.platform AS platform
      ,r.orderDate AS orderDate
      ,CAST(r.orderNumber AS int64) AS orderNumber  --Меняем формат orderNumber на int, так как дальнейшие сравнения orderNumber в формате string могут привести к ошибкам (например '105'<'99')
      ,r.Region AS Region
      ,r.Sum AS sum
      ,r.promoId AS promoId
    FROM `realweb-152714`.`school22_project`.`af_purchase_events` AS l
    INNER JOIN
      `realweb-152714`.`school22_project`.`crm_data` AS r
    ON l.order_id = r.orderID
       AND
       l.event_date = PARSE_DATE('%Y-%m-%d',  r.orderDate)
    WHERE 
      install_date < event_date --Фильтруем строки, где дата покупки предшествует дате установки
      AND
      --Анализ причин незаполнения полей с инфо об источнике или платформе или дате установки лежит вне настоящей задачи, фильтруем такие строки
      platform IS NOT NULL
      AND 
      install_date IS NOT NULL
      AND
      media_source IS NOT NULL
    ORDER BY
      event_date ASC
    ),

-- Создаем алиас с уникальными парами appsflyer_id и orderNumber, так как у некоторых пользователей ошибочно присвоены одни и те же порядковые номера заказов
-- Для каждой пары "пользователь - порядковый номера заказа" берем наименьшую дату покупки и наименьший order_id
  appsflyer_id_orderNumber AS
    (
    SELECT
      appsflyer_id
      ,orderNumber
      ,MIN(event_date) AS event_date
      ,MIN(order_id) AS order_id 
    FROM events_orders
    GROUP BY
      appsflyer_id
      ,orderNumber
    ),

-- Джойним events_orders и appsflyer_id_orderNumber, фильтруя NULL значения, чтобы получить итоговую таблицу заказов пользователей приложения
-- Дополнительно создаем столбцы с минимальным порядковым номером заказа в приложении для пользователя
-- и проверяем был ли использован при первом заказе в приложении промокод: если да, то ставим признак 'promotion_status' = 1, если нет то 'promotion_status' = 0
  events_orders_cleared AS
    (
    SELECT
      l.appsflyer_id AS appsflyer_id
      ,l.orderNumber AS orderNumber
      ,l.event_date AS event_date
      ,l.order_id AS order_id
      ,r.install_date AS install_date
      ,r.media_source AS media_source
      ,r.platform AS platform
      ,r.orderDate AS orderDate
      ,r.Region AS Region
      ,r.Sum AS sum
      ,r.promoId AS promoId
      ,min(l.orderNumber) over (PARTITION BY l.appsflyer_id) min_app_order
      ,CASE WHEN r.promoId IS NOT NULL AND min(l.orderNumber) over (PARTITION BY l.appsflyer_id) = l.orderNumber THEN '1' 
            ELSE '0' --В случае если пользователь использовал при первом заказе в приложении промокод присваиваем такому заказу статус 'promotion_status' = '1', иначе '0'             
       END AS promotion_status
    FROM appsflyer_id_orderNumber AS l
    LEFT JOIN
      events_orders AS r
    ON l.appsflyer_id = r.appsflyer_id
       AND 
       l.orderNumber = r.orderNumber
       AND 
       l.event_date = r.event_date
       AND
       l.order_id = r.order_id
    WHERE
      platform IS NOT NULL
      AND 
      install_date IS NOT NULL
      AND
      media_source IS NOT NULL
    ),

--Создаем алиас с Aquisition cost пользователей используя cost_data

  ac_cost AS
    (
    SELECT
      l.date AS date
      ,l.media_source AS media_source
      ,l.platform AS platform
      ,ROUND(l.cost / r.users ,2)  AS ac_cost  
    FROM
      `realweb-152714`.`school22_project`.`cost_data` AS l
    INNER JOIN
    --Формируем подзапрос, где находим количество пользователей по install_date, platform, media_source, чтобы распределить по этим пользователям рекламные расходы из cost_data (где они есть)
      (
      SELECT
        install_date
        ,platform
        ,media_source
        ,COUNT(DISTINCT appsflyer_id) users
      FROM events_orders_cleared
      GROUP BY
        install_date
        ,platform
        ,media_source
      ) AS r
    ON l.date = r.install_date
       AND
       l.media_source = r.media_source
       AND
       l.platform = r.platform
    ),

--Добавляем косты в таблицу заказов пользователей приложения: ac_cost, self_cost, blogger_cost, delivery_cost

  events_orders_costs AS
   (
   SELECT
      l.appsflyer_id AS appsflyer_id
      ,l.orderNumber AS orderNumber
      ,l.event_date AS event_date
      ,l.order_id AS order_id
      ,l.install_date AS install_date
      ,l.media_source AS media_source
      ,l.platform AS platform
      ,l.orderDate AS orderDate
      ,l.Region AS Region
      ,l.sum AS sum
      ,l.promoId AS promoId
      ,MAX(l.promotion_status) over (PARTITION BY l.appsflyer_id) AS promotion_status --Если пользователь использовал для первого заказа в приложении промокод, присваиваем всем строкам, 
      ,l.min_app_order AS min_app_order                                               --оносящимся к пользователю статус 'promotion_status' = '1'
      ,CASE
         WHEN l.orderNumber = min(l.orderNumber) over (PARTITION BY l.appsflyer_id) THEN r.ac_cost --Для удобства суммирования расходов присваиваем ac_cost только одной строке для каждого пользователя
         ELSE 0
       END AS ac_cost
      ,CASE
         WHEN l.sum >=3000 THEN ROUND((l.sum-1000)*0.65,2) --Для заказов с суммой менее 3000 рублей доставка входит в сумму заказа и учитывается в расчетах маржинальности, 
         ELSE ROUND((l.sum)*0.65,2)                        --для заказов свыше 3000 принимаем такой же порядок. Другой аргумент: расходы на доставку клиентам курьерской службы можно рассматривать 
       END AS self_cost                                    --как прямые расходы,которые должны быть учтены в расчетах маржинальности
      ,CASE
      --Учитываем условие по промокодам блоггеров. Строк, где одновременно содержалось бы два или более кодов от блоггеров, нет 
         WHEN l.promoId LIKE '%7525%' AND l.sum>=2000 THEN 800
         WHEN l.promoId LIKE '%7641%' AND l.sum>=1000 THEN 600
         WHEN l.promoId LIKE '%7061%' AND l.sum>=3000 AND l.orderNumber = l.min_app_order THEN 200
         WHEN l.promoId LIKE '%7937%' AND l.sum>=3000 THEN 600
         WHEN l.promoId LIKE '%5403%' AND l.sum>=1500 THEN 600
         ELSE 0
       END AS blogger_cost
      ,CASE 
         WHEN l.sum >= 3000 THEN 1000
         ELSE 0
       END AS delivery_cost
    FROM events_orders_cleared AS l
    LEFT JOIN 
      ac_cost AS r
    ON l.install_date = r.date
       AND
       l.media_source = r.media_source
       AND
       l.platform = r.platform
    ), 

--Формируем названия когорт

  cohorts AS
    (
    SELECT
      STRING(DATE_TRUNC(date(install_date), month)) AS install_date --Для формирования когорт выбираем месячный временной интервал 
      ,media_source
      ,platform
      ,promotion_status
      ,CONCAT(STRING(DATE_TRUNC(date(MIN(install_date)), month)), '_', media_source, '_', platform, '_', promotion_status) AS cohort_name
    FROM events_orders_costs
    GROUP BY
      media_source
      ,platform
      ,install_date
      ,promotion_status
    ),

--Присваиваем заказам пользователей соответствующие названия когорт

events_orders_costs_cohorts AS
    (
    SELECT
      l.appsflyer_id AS appsflyer_id
      ,l.orderNumber AS orderNumber
      ,l.event_date AS event_date
      ,l.order_id AS order_id
      ,r.install_date AS install_date
      ,l.media_source AS media_source
      ,l.platform AS platform
      ,l.orderDate AS orderDate
      ,l.Region AS Region
      ,l.sum AS sum
      ,l.promoID AS promoID
      ,l.promotion_status AS promotion_status
      ,l.min_app_order AS min_app_order
      ,l.ac_cost AS ac_cost
      ,l.self_cost AS self_cost
      ,l.blogger_cost AS blogger_cost
      ,l.delivery_cost AS delivery_cost
      ,r.cohort_name AS cohort_name
    FROM events_orders_costs AS l
    LEFT JOIN 
      cohorts AS r
    ON STRING(DATE_TRUNC(date(l.install_date), month)) = r.install_date
       AND
       l.media_source = r.media_source
       AND
       l.platform = r.platform
       AND
       l.promotion_status = r.promotion_status
    ),

--Настоящий алиас позволяет удалить из списка заказы пользователей, которым в результате ошибки в данных присвоены два различных названия когорты
--Если таких пользователей не исключать из данных, то они будут искажать отчетность
--В этой задаче таких пользотвалей получилось немного - 236 (с учетом предыдущих очисток)
--Необходимо отдельно проанализировать почему в рамках полугода у одного и того же пользователя могут отличаться медиа-источник и дата установки приложения

final_table_without_double_cohort_names_id AS
   ( 
   SELECT
     l.appsflyer_id AS appsflyer_id
     ,l.orderNumber AS orderNumber
     ,l.event_date AS event_date
     ,l.order_id AS order_id
     ,l.install_date AS install_date
     ,l.media_source AS media_source
     ,l.platform AS platform
     ,l.orderDate AS orderDate
     ,l.Region AS Region
     ,l.sum AS sum
     ,l.promoID AS promoID
     ,l.promotion_status AS promotion_status
     ,l.min_app_order AS min_app_order
     ,l.ac_cost AS ac_cost
     ,l.self_cost AS self_cost
     ,l.blogger_cost AS blogger_cost
     ,l.delivery_cost AS delivery_cost
     ,l.cohort_name AS cohort_name
   FROM events_orders_costs_cohorts AS l
   LEFT JOIN
     (
     SELECT  
       appsflyer_id,
       COUNT(DISTINCT cohort_name) cnt_cohorts
       FROM events_orders_costs_cohorts
     GROUP BY
       appsflyer_id
     ) AS r
   ON l.appsflyer_id = r.appsflyer_id
   WHERE
     r.cnt_cohorts = 1
   ),



-- Считаем метрики по условиям задачи

  metrics AS
    (
    SELECT
      cohort_name
      ,install_date
      ,media_source
      ,platform
      ,promotion_status
      ,COUNT(DISTINCT appsflyer_id) AS cohort_base
      ,ROUND(SUM(sum)/COUNT(order_id),2) AS avg_check --Средний чек
      ,ROUND(SUM(sum)/COUNT (DISTINCT appsflyer_id),2) AS LTV_revenue --LTV по доходу
      ,ROUND(SUM(sum - self_cost)/COUNT (DISTINCT appsflyer_id),2) AS LTV_profit --LTV по прибыли
      ,CASE 
        WHEN SUM(ac_cost)>0 OR SUM(blogger_cost)>0 THEN ROUND(SUM(sum - self_cost - blogger_cost - ac_cost)/SUM(self_cost + blogger_cost + ac_cost)*100,2) --ROI для источников с маркетинговыми расходами
        ELSE NULL
       END AS ROI
    FROM events_orders_costs_cohorts
    GROUP BY
      cohort_name
      ,install_date
      ,media_source
      ,platform
      ,promotion_status
    ),

--Находим retention для когорт
  retention AS
    (
    SELECT
      cohort_name
      ,install_date
      ,media_source
      ,platform
      ,promotion_status
      ,cohort_base
      ,month_diff
      ,COUNT(DISTINCT appsflyer_id) users_by_month
      ,CASE
        WHEN month_diff = 0 THEN 100 --Для нулевого месяца считаем, что retention = 100%
        ELSE ROUND(COUNT(DISTINCT appsflyer_id)*100/cohort_base,2)
      END AS month_retention
    FROM
      (
      --Путем джойна создаем таблицу активности пользователей, где каждому appsflyer_id соотвествует набор дат (event_date), в которые он совершал покупки 
      SELECT
        l.cohort_name AS cohort_name
        ,cohort_base
        ,install_date
        ,event_date
        ,media_source
        ,platform
        ,promotion_status
        ,appsflyer_id
        ,EXTRACT(month FROM date(event_date)) - EXTRACT(month FROM date(install_date)) AS month_diff --Считаем разницу в месяцах между датой установки приложения и датой каждой совершенной покупки
      FROM 
        (
        -- Считаем численность когорт в соответствии с названиями. Инфо о дате установки, источниках, платформах, использованию промокода подтягиваем для возможности фильтрации в отчете PBI
        SELECT
          cohort_name
          ,install_date
          ,media_source
          ,platform
          ,promotion_status
          ,COUNT(DISTINCT appsflyer_id) cohort_base
        FROM events_orders_costs_cohorts
        GROUP BY
          cohort_name
          ,install_date
          ,media_source
          ,platform
          ,promotion_status
        ) AS l
      LEFT JOIN 
        --Создаем подзапрос с датами покупок пользователей и указанием соотвествующей когорты
        (
        SELECT
          STRING(DATE_TRUNC(date(event_date), month)) event_date
          ,appsflyer_id
          ,cohort_name
        FROM
          events_orders_costs_cohorts
        ) AS r
      ON l.cohort_name = r.cohort_name
      )
    GROUP BY
      cohort_name
      ,install_date
      ,media_source
      ,platform
      ,promotion_status
      ,cohort_base
      ,month_diff
    ORDER BY
      cohort_name
      ,month_diff 
    )

--Создаем сводную таблицу метрик и retention для экспорта в PBI
SELECT 
  l.cohort_name AS cohort_name
  ,l.install_date AS install_date
  ,l.media_source AS media_source
  ,l.platform AS platform
  ,l.promotion_status AS promotion_status
  ,l.cohort_base AS cohort_base
  ,l.month_diff AS month_diff
  ,l.users_by_month AS users_by_month
  ,l.month_retention AS month_retention
  ,MIN(month_diff) over(PARTITION BY l.cohort_name) min_month_diff
  ,CASE
    WHEN month_diff = MIN(month_diff) over(PARTITION BY l.cohort_name) THEN r.avg_check
    ELSE 0
   END AS avg_check
  ,CASE
    WHEN month_diff = MIN(month_diff) over(PARTITION BY l.cohort_name) THEN r.LTV_revenue
    ELSE 0
   END AS LTV_revenue
  ,CASE
    WHEN month_diff = MIN(month_diff) over(PARTITION BY l.cohort_name) THEN r.LTV_profit
    ELSE 0
   END AS LTV_profit
  ,CASE
    WHEN month_diff = MIN(month_diff) over(PARTITION BY l.cohort_name) THEN r.ROI
    ELSE 0
   END AS ROI
  ,CASE
    WHEN month_diff = MIN(month_diff) over(PARTITION BY l.cohort_name) THEN l.cohort_base
    ELSE 0
   END AS uniq_users
FROM retention AS l
LEFT JOIN 
  metrics AS r
ON l.cohort_name = r.cohort_name
