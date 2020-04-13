SELECT
  EVENT_DT, EVENT_TIMESTAMP, EVENT_TIME, SESSION_ID, EVENT_NAME, FIREBASE_ID, BF_EVENT, AF_EVENT, DURATION_SECOND, 
  IF(EVENT_NAME = 'session_start', '세션시작', b1.internal_event_detail) AS EVENT_NAME_KR,
  IF(BF_EVENT = 'session_start', '세션시작', b2.internal_event_detail) AS BF_EVENT_KR,
  IF(AF_EVENT IS NULL, '세션종료', b3.internal_event_detail) AS AF_EVENT_KR
FROM (
  SELECT
    *,
    LAG(EVENT_NAME)  OVER (PARTITION BY SESSION_ID ORDER BY EVENT_TIMESTAMP) AS BF_EVENT,
    LEAD(EVENT_NAME) OVER (PARTITION BY SESSION_ID ORDER BY EVENT_TIMESTAMP) AS AF_EVENT,
    TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(EVENT_TIMESTAMP) OVER (PARTITION BY SESSION_ID ORDER BY EVENT_TIMESTAMP)), TIMESTAMP_MICROS(EVENT_TIMESTAMP), SECOND) AS DURATION_SECOND
  FROM(   
    SELECT
      event_date AS EVENT_DT,
      event_timestamp AS EVENT_TIMESTAMP,
      FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as EVENT_TIME,
      CONCAT(user_pseudo_id, '_', event_date, '_', ep.value.int_value) as SESSION_ID,
      event_name as EVENT_NAME,
      user_pseudo_id as FIREBASE_ID
    FROM
      `neo-smart-gcm.analytics_196410282.events_*`,
      UNNEST (event_params) as ep
    WHERE
      (event_name LIKE 'SCREEN%' OR event_name LIKE 'EVENT%' OR event_name = 'session_start') AND (ep.key = 'ga_session_id') AND _TABLE_SUFFIX BETWEEN '20200407' AND '20200407' # 조회기간 (시작일, 종료일)
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 6, 4, 2
    )
  ORDER BY 6, 4, 2
) a
LEFT JOIN `neo-smart-gcm.analytics_196410282.event_name_list` AS b1 ON a.EVENT_NAME = b1.internal_event_name
LEFT JOIN `neo-smart-gcm.analytics_196410282.event_name_list` AS b2 ON a.BF_EVENT = b2.internal_event_name
LEFT JOIN `neo-smart-gcm.analytics_196410282.event_name_list` AS b3 ON a.AF_EVENT = b3.internal_event_name
ORDER BY 6, 4, 2
