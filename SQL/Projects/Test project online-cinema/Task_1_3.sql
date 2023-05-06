-- 3.Список пользователей, у которых в последние 7 дней в один день был сначала просмотр с источником трафика organic, а следом за ним - просмотр с referral. 
-- Просмотр с organic не обязательно должен быть первым в день. 
-- Между просмотрами с organic и с referral не должно быть других просмотров.
-- В запросе используется стандартный диалект SQL (BigQuery).

WITH prev AS
  (
  SELECT
    user_id
    ,show_date
    ,utm_medium
    ,LEAD(show_date,1) over (PARTITION BY user_id ORDER BY show_date) next_show_date  --Находим дату и время следующего просмотра
    ,LEAD(utm_medium,1) over (PARTITION BY user_id ORDER BY show_date) utm_medium_next_watch --Находим источник трафика следующей сессии
    ,LAST_VALUE(show_date) over (PARTITION BY user_id ORDER BY show_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_show_date --Находим дату и время последней сессии каждого пользователя
  FROM content_watch
  )

--Оставляем только те уникальные user_id, которые удовлетворяют условию задачи 
SELECT DISTINCT user_id
FROM prev
WHERE
  LOWER(utm_medium) = 'organic' 
  AND LOWER(utm_medium_next_watch) = 'referral'
  AND DATETIME_DIFF(last_show_date,show_date,DAY) < 7 --Если считаем последние 7 дней от начала последней сессии каждого пользователя 
  --ИЛИ ЗАМЕНИТЬ ПОСЛЕДНЮЮ СТРОЧКУ ЗАПРОСА НА
  --AND DATETIME_DIFF(CURRENT_DATETIME('Europe/Moscow'),show_date,DAY) < 7  --Если считаем последние 7 дней с текущего момента