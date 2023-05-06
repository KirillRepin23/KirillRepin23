-- 2.Ежемесячный ТОП-5 сериалов и единичного контента по количеству смотрящих людей за всё время. 
-- Это должен быть один запрос, выдающий и топ-5 сериалов, и топ-5 фильмов.
-- В запросе используется стандартный диалект SQL (BigQuery).

-- Соединяем две исходные таблицы,  присваиваем контенту параметры media_id (идентификатор фильма или целого сериала) и media_type (тип контента: сериал или фильм).
-- Группируем по параметрам и подсчитываем количество уникальных пользователей посмотревших в конкретный месяц, конкретный media_id с известным media_type
WITH prev
  AS
  (
  SELECT
    FORMAT_DATETIME('%Y-%m', show_date) month
    ,CASE 
      WHEN compilation_id IS NULL THEN l.content_id 
      ELSE compilation_id
    END media_id
    ,CASE 
      WHEN compilation_id IS NULL THEN 'film'
      ELSE 'series'
    END media_type
    ,COUNT(DISTINCT user_id) uniq_user_views
  FROM 
    content_watch AS l
  LEFT JOIN
    content AS r
  ON
    l.content_id = r.content_id
    GROUP BY
      1,2,3
  ),

next AS
  (
  SELECT
    month
    ,media_type
    ,RANK() OVER (PARTITION BY month,media_type ORDER BY uniq_user_views DESC) rank --Выставляем ранг сериала или фильма в зависимости от месяца и типа контента
    ,media_id
    ,uniq_user_views
  FROM prev
  QUALIFY rank <= 5 --Отсекаем от результатов выполнения оконной функции те строки, где ранг больше 5
  ORDER BY
    1 DESC,2,3
  )

--Для более наглядного отображения результатов используем агрегирующую функцию ARRAY_AGG с групировкой по месяцу и типу контента
SELECT
  month
  ,media_type
  ,ARRAY_AGG(rank) rank
  ,ARRAY_AGG(media_id) media_id
  ,ARRAY_AGG(uniq_user_views) uniq_user_views
FROM next
GROUP BY
  1,2 