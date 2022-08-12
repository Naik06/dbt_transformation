{{
  config(
    materialized = 'table',
    partition_by={
      "field": " CommentDate",
      "granularity": "day"
    }
  )
}}


SELECT
ArticleID,
date(CommentDateUpdated) CommentDate,
COUNT(DISTINCT CommentID) AS NumComments
FROM {{ ref ('coral_comments_fct') }}
WHERE 1=1
AND dw_partition_date between "2021-11-01" and"2021-11-30"
AND CommentStatus = 'ACCEPTED'
GROUP BY 1,2