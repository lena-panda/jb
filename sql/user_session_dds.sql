INSERT INTO jb.user_session_dds (user_id, product_code, timestamp, event_id, user_session_id) 

WITH 
-- Take the new batch
new_events AS (
    SELECT
        user_id,
        timestamp,
        product_code,
        event_id,
        load_timestamp
    FROM
        jb.user_action_raw
    WHERE
        load_timestamp = (SELECT MAX(load_timestamp) FROM jb.user_action_raw)
),

-- Add tail from previous batch
all_events AS (
    SELECT
        events.*,
        CASE
            WHEN event_id IN ('a', 'b', 'c')
                THEN timestamp
            ELSE NULL
        END AS user_event_timestamp
    FROM (
        SELECT
            'prev' AS type,
            user_id,
            product_code,
            Max(dds.timestamp) AS timestamp,
            argMax(event_id, dds.timestamp) AS event_id,
            argMax(user_session_id, dds.timestamp) AS user_session_id
        FROM 
            jb.user_session_dds AS dds
        WHERE
            dds.timestamp >= (SELECT MIN(timestamp) - 300 FROM new_events)
            AND dds.event_id IN ('a', 'b', 'c')
        GROUP BY
            user_id,
            product_code
        UNION ALL
        SELECT
            'new' AS type,
            user_id,
            product_code,
            timestamp,
            event_id,
            NULL AS user_session_id
        FROM
            new_events
    ) AS events
),

-- Calculate time_diff for user events
events_time_diff AS (
    SELECT
        timestamp,
        user_id,
        product_code,
        event_id,
        type,
        prev_user_event_timestamp,
        prev_user_session_id,
        CASE
            WHEN timestamp IS NOT NULL
                THEN timestamp - prev_user_event_timestamp 
            ELSE NULL
        END AS time_diff -- sec
    FROM (
        SELECT
            timestamp,
            user_id,
            product_code,
            event_id,
            type,
            LAST_VALUE(user_event_timestamp) OVER w AS prev_user_event_timestamp, -- instead of LAG (ClickHouse doesn`t have it)
            LAST_VALUE(user_session_id) OVER w AS prev_user_session_id
        FROM
            all_events
        WINDOW w AS (
            PARTITION BY user_id, product_code
            ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
    )
),

-- Define session start
session_start AS (
    SELECT
        user_id,
        timestamp,
        product_code,
        event_id,
        type,
        prev_user_session_id,
        time_diff,
        CASE
            WHEN event_id IN ('a', 'b', 'c') AND (time_diff IS NULL OR time_diff > 300)
                THEN 1
            ELSE 0
        END AS is_start_session,
        CASE
            WHEN event_id NOT IN ('a', 'b', 'c') AND (time_diff IS NULL OR time_diff > 300) -- if you want to add 1 minute more for non-users action after session, add here 60 sec
                THEN 0
            ELSE 1
        END AS is_in_session
    FROM
        events_time_diff
),

-- Generate a session number
session_num_generation AS (
    SELECT
        user_id,
        timestamp,
        product_code,
        event_id,
        type,
        prev_user_session_id,
        time_diff,
        is_start_session,
        CASE
            WHEN is_in_session = 1
                THEN CAST(SUM(is_start_session) OVER w AS Int) 
            ELSE NULL
        END AS session_number
    FROM
        session_start
    WINDOW w AS (
        PARTITION BY user_id, product_code
        ORDER BY timestamp
    )
),

-- Define beginning and end of each session
session_bounds AS (
    SELECT
        user_id,
        product_code,
        timestamp,
        event_id,
        type,
        prev_user_session_id,
        session_number,
        CASE
            WHEN session_number IS NOT NULL
                THEN MIN(timestamp) OVER w
            ELSE NULL
        END AS session_start_time
    FROM
        session_num_generation
    WINDOW w AS (
        PARTITION BY user_id, product_code, session_number
    )
)

-- Final result
SELECT
    user_id,
    product_code,
    timestamp,
    event_id,
    CASE
        WHEN session_number = 1 AND prev_user_session_id IS NOT NULL
            THEN prev_user_session_id
        ELSE
            CONCAT(user_id, '#', product_code, '#', session_start_time)
    END AS user_session_id
FROM
    session_bounds
WHERE
    type = 'new'
ORDER BY
    timestamp,
    user_id,
    product_code,
    event_id
;