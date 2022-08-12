
{{
  config(
    materialized = 'table',
    partition_by={
      "field": "GradientTimeSlot",
      "data_type": "day",
      "granularity": "day"
    }
  )
}}


--create table `ncau-data-newsquery-dev.bdm_verity.content_interactions_detail_pv`
--partition by date(GradientTimeSlot)
--as
with article_published as
(
    select content_id as ArticleID,first_publish_timestamp as first_published
    from {{ source('bdm_verity','content_dim') }}
), article_views as 
(
    select ceh.articleid,
    timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE) date_time,
    timestamp_diff(timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE),first_published,MINUTE) minutes_from_published,
    timestamp_add(
        timestamp_trunc(first_published,HOUR),
        INTERVAL cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 MINUTE
        ) as first_published,
        cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 as interval_min,
    sum(page_view) PageViews,
    sum(0) Pageviews_web,
    sum(page_view) Pageviews_amp,
    sum(0) Pageviews_app,
    sum(if(Membertype='Subscriber',page_view,0)) SubscriberPV,
    sum(BreachVisit) BreachVisits,
    from `cdm_adobe.clickstream_amp_demographics_fct`  ceh join article_published
    on (ceh.articleid = article_published.ArticleID)
    where dw_partition_date between "2021-11-01" and"2021-11-30" 
    and ceh.ArticleID is not null
    and page_view > 0
    group by 1,2,3,4,5

union all


    select ceh.articleid,
    timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE) date_time,
    timestamp_diff(timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE),first_published,MINUTE) minutes_from_published,
    timestamp_add(
        timestamp_trunc(first_published,HOUR),
        INTERVAL cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 MINUTE
        ) as first_published,
        cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 as interval_min,
    sum(page_view) PageViews,
    sum(0) Pageviews_web,
    sum(0) Pageviews_amp,
    sum(page_view) Pageviews_app,
    sum(if(Membertype='Subscriber',page_view,0)) SubscriberPV,
    sum(BreachVisit) BreachVisits,
    from {{ ref( 'clickstream_app_demographics_fct' )}}  ceh join article_published
    on (ceh.articleid = article_published.ArticleID)
    where dw_partition_date between "2021-11-01" and"2021-11-30" 
    --and ceh.articleid in ('81f03e39ac36a19075467570923d6b05','8bf8d2fd194207649af7b53c83cb4f85','88e25069bcca571e5cc33c34004f7d15')
    and ceh.ArticleID is not null
    and page_view > 0
    group by 1,2,3,4,5

union all 


    select ceh.articleid,
    timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE) date_time,
    timestamp_diff(timestamp_trunc(timestamp(datetime(date_time,"Australia/Sydney")),MINUTE),first_published,MINUTE) minutes_from_published,
    timestamp_add(
        timestamp_trunc(first_published,HOUR),
        INTERVAL cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 MINUTE
        ) as first_published,
        cast(floor(timestamp_diff(first_published,timestamp_trunc(first_published,HOUR),MINUTE)/5) as INT64)*5 as interval_min,
    sum(page_view) PageViews,
    sum(page_view) Pageviews_web,
    sum(0) Pageviews_amp,
    sum(0) Pageviews_app,
    sum(if(Membertype='Subscriber',page_view,0)) SubscriberPV,
    sum(BreachVisit) BreachVisits,
    from {{ ref( 'clickstream_web_demographics_fct') }}  ceh join article_published
    on (ceh.articleid = article_published.ArticleID)
    where dw_partition_date between "2021-11-01" and"2021-11-30" 
    --and ceh.articleid in ('81f03e39ac36a19075467570923d6b05','8bf8d2fd194207649af7b53c83cb4f85','88e25069bcca571e5cc33c34004f7d15')
    and ceh.ArticleID is not null
    and page_view > 0
    group by 1,2,3,4,5

)
select article_views.articleid,
timestamp_add(article_views.first_published,INTERVAL minute_slice_start MINUTE) GradientTimeSlot,
minute_slice_start as GradientSlice,
sum(PageViews) as PageViews,
sum(Pageviews_web) as Pageviews_web,
sum(Pageviews_amp) as Pageviews_amp,
sum(PageViews_app) as Pageviews_app,
sum(SubscriberPV)  as SubscriberPV,
sum(BreachVisits)  as BreachVisits
from article_views join {{ source('prstn_verity','insights_gradient_minutes') }} defs
on (article_views.minutes_from_published between defs.minute_slice_start and defs.minute_slice_end)
group by 1,2,3