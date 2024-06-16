INSERT INTO jb.user_session_dm WITH TRUNCATE

SELECT
    user_id,
    product_code,
    user_session_id,
    toDateTime(MIN(timestamp), 'Europe/Amsterdam') AS start_session_dttm,
    toDateTime(MAX(timestamp), 'Europe/Amsterdam') AS end_session_dttm,
    CAST(MAX(timestamp) - MIN(timestamp) AS Int) AS session_duration_sec,
    CAST(COUNT(event_id) AS Int) AS total_event_cnt,
    CAST(COUNT(IF(event_id IN ('a', 'b', 'c'), 1, NULL)) AS Int) AS user_event_cnt,
    CAST(COUNT(IF(event_id NOT IN ('a', 'b', 'c'), 1, NULL)) AS Int) AS system_event_cnt
FROM 
    jb.user_session_dds 
WHERE
    user_session_id IS NOT NULL
GROUP BY
    user_id,
    product_code,
    user_session_id
ORDER BY
    user_id,
    product_code,
    user_session_id
;
