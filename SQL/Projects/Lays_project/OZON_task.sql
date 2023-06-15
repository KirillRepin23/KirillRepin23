--SELECT
--    user_clien_id
--    ,date
--    ,action_type
--    ,widget_id
--    ,COUNT(widget_id) OVER (PARTITION BY )
    
WITH prev AS
    (
    SELECT
        date
        ,widget_id
        ,COUNT(object_type) count_views
    FROM Events
    WHERE
        widget_id = '522955'
        AND widget_id = '522957'
        AND object_type = 'view'
    GROUP BY
        1,2
    ),

WITH next AS
    (
    SELECT
        date
        ,widget_id
        ,COUNT(object_type) count_clicks
    FROM Events
    WHERE
        (widget_id = '522955'
        OR widget_id = '522957')
        AND object_type = 'click'
    GROUP BY
        1,2
    )

SELECT
    l.widget_id
    ,l.date
    ,count_clicks/count_views
FROM next AS l

WHERE
    widget_id = '522955'
    AND widget_id = '522957'
    
    
    
    
SELECT
    date
    ,widget_id
    ,object_type
    ,CASE
        WHEN object_type = 'click' THEN COUNT(object_type)
        WHEN object_type = 'view' THEN COUNT(object_type)
    END AS count_object
    
FROM Events
WHERE
    widget_id = '522955'
    AND widget_id = '522957'
GROUP BY
    