WITH
    {day:Date} AS target,
    events AS (
        SELECT user_id, movie_id, event_type, progress_seconds
        FROM raw_events FINAL
        WHERE toDate(timestamp) = target
    ),
    activity AS (
        SELECT user_id, toDate(timestamp) AS active_day,
               max(event_type = 'VIEW_STARTED') AS started
        FROM raw_events FINAL
        WHERE toDate(timestamp) <= target + 7
          AND toDate(timestamp) <= toDate(now('UTC'))
        GROUP BY user_id, active_day
    ),
    first_views AS (
        SELECT user_id, minIf(active_day, started) AS first_day
        FROM activity
        GROUP BY user_id
        HAVING max(started) = 1 AND first_day = target
    ),
    returns AS (
        SELECT dateDiff('day', first_day, active_day) AS lag,
               uniqExact(activity.user_id) AS users
        FROM activity INNER JOIN first_views USING (user_id)
        WHERE active_day BETWEEN target AND target + 7
        GROUP BY lag
    ),
    cohort AS (SELECT count() AS users FROM first_views),
    summary AS (
        SELECT uniqExact(user_id) AS dau,
               countIf(event_type = 'VIEW_STARTED') AS starts,
               countIf(event_type = 'VIEW_FINISHED') AS finishes,
               avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS watch_time
        FROM events
    )
SELECT 'dau' AS metric, '' AS dimension, toNullable(toFloat64(dau)) AS value FROM summary
UNION ALL
SELECT 'average_watch_seconds', '', watch_time FROM summary
UNION ALL
SELECT 'view_conversion', '', finishes / nullIf(starts, 0) FROM summary
UNION ALL
SELECT 'top_movies', movie_id, toFloat64(views)
FROM (
    SELECT movie_id, count() AS views FROM events
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY movie_id ORDER BY views DESC, movie_id LIMIT 10
)
UNION ALL
SELECT 'retention', concat('D', toString(number)),
       if(target + toInt32(number) > toDate(now('UTC')), NULL,
          returns.users / nullIf(cohort.users, 0))
FROM numbers(8) CROSS JOIN cohort LEFT JOIN returns ON returns.lag = toInt64(number)
UNION ALL
SELECT 'cohort_size', '', toFloat64(users) FROM cohort
FORMAT JSONEachRow
