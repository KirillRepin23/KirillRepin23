-- SQL-запрос для построения когорт по параметрам и подготовке данных к выгрузке в систему визуализации данных
-- В запросе используется стандартный диалект SQL (BigQuery).

WITH user_cohorts
    AS
    (
    SELECT
        user_id --Integer
        ,first_session_date --Date
        ,session_date --Date
        ,platform_id --Integer
        ,media_source --String
        ,geo --String
        ,subscription_type --String
        ,recency --String
        ,frequency --String
        ,volume --String
        ,interest_category --String
        ,conversion_probability --String
        ,CONCAT(STRING(first_session_date),'_',STRING(platform_id),'_',media_source,'_',geo,'_',subscription_type,'_',recency,'_',frequency,'_',volume,'_',interest_category,'_',conversion_probability) cohort_name
    FROM
        table_for_retention_segmentation --Таблица со всеми необходимыми для задачи исходными данными, в том числе с категориями, требующими отдельных математических расчетов
    ),

retention AS
    (
    SELECT
      cohort_name
      ,first_session_date
      ,platform_id
      ,media_source
      ,geo
      ,subscription_type
      ,recency
      ,frequency
      ,volume
      ,interest_category
      ,conversion_probability
      ,cohort_base
      ,day_diff
      ,COUNT(DISTINCT user_id) users_by_day
      ,CASE
        WHEN day_diff = 0 THEN 100 --Для нулевого дня считаем, что retention = 100%
        ELSE ROUND(COUNT(DISTINCT user_id)*100/cohort_base,2)
      END day_retention
    FROM
      (
      --Путем джойна создаем таблицу активности пользователей, где каждому user_id соотвествует набор дат (session_date), в которые у пользователя были сессии 
      SELECT
        l.cohort_name AS cohort_name
        ,cohort_base
        ,first_session_date
        ,session_date
        ,platform_id
        ,media_source
        ,geo
        ,subscription_type
        ,recency
        ,frequency
        ,volume
        ,interest_category
        ,conversion_probability
        ,user_id
        ,DATE_DIFF(session_date, first_session_date, DAY) day_diff --Считаем разницу в днях между датой первой сессии и датой последующих сессий
      FROM 
        (
        -- Считаем численность когорт в соответствии с названиями. Инфо о дате первой сессии и параметрах пользователя подтягиваем для возможности фильтрации в дашборде
        SELECT
          cohort_name
          ,first_session_date
          ,platform_id
          ,media_source
          ,geo
          ,subscription_type
          ,recency
          ,frequency
          ,volume
          ,interest_category
          ,conversion_probability
          ,COUNT(DISTINCT user_id) cohort_base
        FROM user_cohorts
        GROUP BY
          cohort_name
          ,first_session_date
          ,platform_id
          ,media_source
          ,geo
          ,subscription_type
          ,recency
          ,frequency
          ,volume
          ,interest_category
          ,conversion_probability
        ) AS l
      LEFT JOIN 
        --Создаем подзапрос с датами сессий пользователей и указанием соответствующей когорты
        (
        SELECT
          session_date
          ,user_id
          ,cohort_name
        FROM
          user_cohorts
        ) AS r
      ON l.cohort_name = r.cohort_name
      )
    GROUP BY
      cohort_name
      ,first_session_date
      ,platform_id
      ,media_source
      ,geo
      ,subscription_type
      ,recency
      ,frequency
      ,volume
      ,interest_category
      ,conversion_probability
      ,cohort_base
      ,day_diff
    ORDER BY
      cohort_name
      ,day_diff 
    )

SELECT * FROM retention

--Передаем подготовленную выше таблицу в систему визуализации данных для построения таблицы и графика retention