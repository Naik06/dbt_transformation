
{{
  config(
    materialized = 'table',
    partition_by={
      "field": "dw_partition_date",
      "granularity": "day"
    }
  )
}}

select 'web' source,
articleid,
prevArticleID,
nextArticleID,
date(datetime(date_time,"Australia/Sydney")) VisitDay,
dw_partition_date,
sum(page_view) PageView
from  {{ ref ('clickstream_web_fct') }}
where dw_partition_date between "2021-11-01" and"2021-11-30" 
and articleid is not null
and ( nextArticleID is not null
       or
      prevArticleID is not null )

and page_view > 0
group by
articleid,
prevArticleID,
nextArticleID,
VisitDay,
dw_partition_date
union all
select 'app' source,
articleid,
prevArticleID,
nextArticleID,
date(datetime(date_time,"Australia/Sydney")) VisitDay,
dw_partition_date,
sum(page_view) PageView
from  {{ ref ('clickstream_app_fct') }}
where dw_partition_date between "2021-11-01" and"2021-11-30" 
and articleid is not null
and ( nextArticleID is not null
       or
      prevArticleID is not null )
and page_view > 0
group by
articleid,
prevArticleID,
nextArticleID,
VisitDay,
dw_partition_date
union all
select 'amp' source,
articleid,
prevArticleID,
nextArticleID,
date(datetime(date_time,"Australia/Sydney")) VisitDay,
dw_partition_date,
sum(page_view) PageView
from  {{ ref ('clickstream_amp_fct') }}
where dw_partition_date between "2021-11-01" and"2021-11-30" 
and articleid is not null
and ( nextArticleID is not null
       or
      prevArticleID is not null )
and page_view > 0
group by
articleid,
prevArticleID,
nextArticleID,
VisitDay,
dw_partition_date