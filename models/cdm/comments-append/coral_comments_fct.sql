{{
  config(
    materialized = 'table',
    partition_by={
      "field": "dw_partition_date",
      "granularity": "day"
    }
  )
}}



SELECT
source,
Version,
CommentID,
ParentCommentID,
ArticleID,
AuthorID,
CommentText,
CommentHistory,
CommentDateCreated,
CommentDateUpdated,
ReplyCount,
CommentStatus,
dw_partition_date,
IngestTime as dw_ingesttime
FROM
(
WITH T AS
(
SELECT
source,
__v as Version,
_id as CommentID,
parent_id as ParentCommentID,
asset_id as ArticleID,
author_id as AuthorID,
body as CommentText,
body_history as CommentHistory,
created_at as CommentDateCreated,
updated_at as CommentDateUpdated,
reply_count as ReplyCount,
status as CommentStatus,
FIRST_VALUE(IngestTime) OVER (PARTITION BY _id ORDER BY IngestTime) AS IngestTime,
ROW_NUMBER() OVER (PARTITION BY _id ORDER BY IngestTime) AS RN
FROM
{{ source('sdm_coral_comments','comments_ingest') }}
WHERE DatePartitioned > DATE_SUB("2021-11-01", INTERVAL 1 DAY)
AND DatePartitioned <= "2021-11-30"
)
SELECT *,date(IngestTime) as dw_partition_date  FROM T
WHERE DATE(IngestTime) between "2021-11-01" and "2021-11-30"
AND RN = 1
)