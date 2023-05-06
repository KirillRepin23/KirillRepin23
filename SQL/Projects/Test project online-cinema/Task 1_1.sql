-- 1.Количество просмотров по монетизациям SVOD и AVOD (по отдельности) на платформах 10 и 11 (всего) за последние 30 дней. 
-- Выдача должна состоять из трёх столбцов: дата, просмотры AVOD, просмотры SVOD.
-- В запросе используется стандартный диалект SQL (BigQuery).

SELECT 
  *
FROM
  (
  SELECT
    CAST(show_date AS DATE) show_d
    ,paid_type
  FROM 
    content_watch AS l
  LEFT JOIN
    content AS r
  ON
    l.content_id = r.content_id
  WHERE
    DATETIME_DIFF(CURRENT_DATETIME('Europe/Moscow'),show_date,DAY) < 30
    AND platform_id IN (10,11)
    AND paid_type IN ('SVOD','AVOD')
  )
  --Выполняем PIVOT столбца paid_type с созданием столбцов count_of_AVOD и count_of_SVOD. 
  --Заносим в них результаты подсчета количества просмотров по монетизациям SVOD и AVOD в зависимости от даты (show_d).
  --Если в итоговой таблице отсутствует какая-то дата, то значит в эту дату просмотров SVOD и AVOD не было.
  PIVOT(
  COUNT(paid_type) AS count_of
  FOR paid_type IN ('AVOD','SVOD')
  )