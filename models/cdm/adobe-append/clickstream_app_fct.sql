{{
  config(
    materialized = 'table',
    partition_by={
      "field": "dw_partition_date",
      "granularity": "day"
    }
  )
}}



--create table `ncau-data-newsquery-dev.cdm_adobe.clickstream_app_fct` 
  
--PARTITION BY
  --dw_partition_date as
--select * from (

WITH
 adobe_raw_records_01 AS
(SELECT 
  'ADOBE APP' AS SourceCode, post_visid_high, post_visid_low, visit_num, visit_page_num, post_prop2, post_prop3, post_prop4, post_prop5
	, post_prop6, post_prop9, page_url, post_prop12, post_prop14, post_prop15, event_list, post_prop17
	, post_prop18, post_prop19, post_prop20, post_prop21, post_prop16, geo_country, OS, ref_domain, ref_type
	, geo_city, geo_region, geo_zip, language, browser, connection_type, user_agent, date_time, referrer
	, post_page_event, post_evar60, post_evar65, post_evar23, eVar11, post_event_list
    , cast(null as string) post_evar81
    , post_evar16
    , evar34
    , evar54
    , post_evar21 as BREACH_TYPE
    , ifnull(post_campaign,campaign) as TRACKING_CODE
    , cast(null as string) as UUID
	, FIRST_VALUE(IngestTime) OVER (PARTITION BY post_visid_high, post_visid_low, visit_num, date_time ORDER BY IngestTime) AS IngestTime,
 datepartitioned,

###############Flag Columns 
{{ if_statement ()}}
-----------------------
  FROM {{ source('sdm_adobe_app','clickstream_ingest') }} A1
    WHERE
      hit_source not in (5,8,9)
     ------and not (ifnull(post_prop9,'Unspecified') LIKE '%story%' or ifnull(post_prop9,'Unspecified') 
     ------LIKE '%article%' or ifnull(post_prop9,'Unspecified') like '%blogs%' or ifnull(post_prop9,'Unspecified') LIKE '%gallery%' )
      and post_visid_high is not null
      and post_visid_low is not null
      and visit_num is not null
      and visit_page_num is not null
      and exclude_hit = '0'
        --and post_page_event=0  ----
          and DatePartitioned between DATE_SUB("2021-11-01",INTERVAL 1 DAY) and "2021-11-30" ----DATE_SUB('2021-11-01',INTERVAL 1 DAY) and '2021-11-30'
                    /* Read one day prior to BackLoadStart to read sessions open at 12:00 AM and calculate engagement time */
),
adobe_raw_records_02 as (

 SELECT
  *
  , LEAD(date_time,1) OVER (PARTITION BY post_visid_high, post_visid_low, visit_num ,date(datetime(date_time,"Australia/Sydney")) ORDER BY visit_page_num, date_time) as nextstamp
  , LEAD(visit_page_num,1) OVER (PARTITION BY post_visid_high, post_visid_low, visit_num ,date(datetime(date_time,"Australia/Sydney"))  ORDER BY visit_page_num, date_time) as nextvisnum
  , LEAD(post_prop16,1) OVER (PARTITION BY post_visid_high, post_visid_low, visit_num ,date(datetime(date_time,"Australia/Sydney"))  ORDER BY visit_page_num, date_time) as nextArticleID
  , LAG(post_prop16,1) OVER (PARTITION BY post_visid_high, post_visid_low, visit_num ,date(datetime(date_time,"Australia/Sydney"))  ORDER BY visit_page_num, date_time) as prevArticleID
  FROM adobe_raw_records_01
  WHERE DATE(IngestTime)  between "2021-11-01" and "2021-11-30"   ---- between  '2021-11-01' and '2021-11-30'  
),
 adobe_raw_records_03 as (
SELECT
  CONCAT(SAFE_CAST(post_visid_high AS STRING),SAFE_CAST(post_visid_low AS STRING)) as UB_ID,
  CONCAT(SAFE_CAST(post_visid_high AS STRING),SAFE_CAST(post_visid_low AS STRING),SAFE_CAST(visit_num AS STRING)) as VISIT_ID,
  1 as HIT_ID,
  visit_num,
  visit_page_num,
  post_prop2 as Brand,
  post_prop3 as Site,
  post_prop4 as SiteSection,
  post_prop5 as SiteSection2,
  post_prop6 as SiteSection3,
  post_prop9 as ContentType,
  page_url as URL,
  CASE when post_prop12 is null then '' when post_prop12 = 'not set' then '' else post_prop12 end as PCSID,
  CASE WHEN SourceCode='ADOBE AMP' THEN
    /*
        If membertype is not available, rely on content rule to determine member type
        If locked content then subscriber else anonymous
    */
    CASE WHEN post_prop14 is null THEN
        CASE when post_prop15 = 'premium' then 'Subscriber' else 'Anonymous' END
    ELSE
        CASE when post_prop14 = 'registered' then 'Registered' when post_prop14 = 'subscriber' then 'Subscriber' when post_prop14 = 'staff' then 'Staff'  else 'Anonymous' end
    END
  ELSE
    CASE when post_prop14 = 'registered' then 'Registered' when post_prop14 = 'subscriber' then 'Subscriber' when post_prop14 = 'staff' then 'Staff' else 'Anonymous' end 
  END as MemberType,


  CASE WHEN SourceCode='ADOBE AMP' THEN
    CASE WHEN post_prop15='premium' then 'Restricted' else 'Free' END
  ELSE
    CASE when post_prop15 = 'restricted' then 'Restricted' else 'Free' end
  END as ContentRule,
 

  CASE WHEN post_evar81 IS NULL THEN 0
  when event_list LIKE '%,202,%' or event_list LIKE '%202,%' or event_list LIKE '%,202%' AND  post_page_event=0
  AND post_evar81 IS NOT NULL 
  AND post_prop16 IS NULL ----
  then 1 else 0 end as BreachVisit, 
  

  CASE WHEN (event_list LIKE '%,202,%' or event_list LIKE '%=202,%' or event_list LIKE '%,202') ----AND  post_page_event=0 ----
            AND length(post_evar81) = 32 
            AND post_evar81 is not null 
       THEN post_evar81
       WHEN (post_prop9 LIKE '%story%' or post_prop9 LIKE '%article%' or post_prop9 like '%blogs%') THEN
        COALESCE(post_prop16,post_evar16)
       WHEN SourceCode = 'ADOBE WEB' 
        and post_prop9 like '%gallery%'
        and evar34 is not null
        and evar34 like '%/image-gallery/%' THEN
            array_reverse(split(split(evar34,'?')[safe_ordinal(1)],'/'))[safe_ordinal(1)]
  END AS ArticleID, 
  post_prop17 as ArticleName,
  post_prop18 as ArticleAuthor,
  post_prop19 as ArticleSource,
  post_prop20 as ArticlePublishDate,
  post_prop21 as BreachType,

IF(Type='Article',
CASE 
    WHEN ( (post_prop2 = 'the australian' AND date(datetime(date_time,"Australia/Sydney")) >= "2020-08-01")
            OR
           (post_prop2 != 'the australian' AND date(datetime(date_time,"Australia/Sydney")) >= "2019-09-01")
        )
        THEN
            CASE WHEN  (event_list LIKE '%,202,%' or event_list LIKE '%202,%' or event_list LIKE '%,202%') AND  post_page_event=0 THEN
                0
            ELSE
                CASE when COALESCE(post_prop16,post_evar16) is not null and post_page_event = 0 
                    and (post_prop9 LIKE '%story%' or post_prop9 LIKE '%article%' or post_prop9 like '%blogs%') 
                    then 1 
                    when (post_prop9 like '%gallery%'  and SourceCode = 'ADOBE WEB') then 1
   		    when COALESCE(post_prop16,post_evar16) is null and post_page_event = 0 and post_prop9  in ('story','digitaleditions+story') then 1
                else 0 
                end 
            END
    ELSE
       CASE when COALESCE(post_prop16,post_evar16) is not null and post_page_event = 0 
       and (post_prop9 LIKE '%story%' or post_prop9 LIKE '%article%' or post_prop9 like '%blogs%') 
       then 1 
       when (post_prop9 like '%gallery%'  and SourceCode = 'ADOBE WEB') then 1
   when COALESCE(post_prop16,post_evar16) is null and post_page_event = 0 and post_prop9  in ('story','digitaleditions+story') then 1
       else 0 
       end
    END,IF(post_page_event = 0 ,1,0)  ) as page_view,


  geo_country,
  geo_city,
  geo_region,
  geo_zip,
  language,
  browser,
  connection_type,
  OS,
  CASE WHEN REGEXP_CONTAINS(user_agent, '(Tablet|tablet|iPad)') = true THEN 'Tablet'
  WHEN REGEXP_CONTAINS(user_agent, '(Mobile|iP(hone|od)|Android|BlackBerry|IEMobile|Kindle|NetFront|Silk-Accelerated|(hpw|web)OS|Fennec|Minimo|Opera M(obi|ini)|Blazer|Dolfin|Dolphin|Skyfire|Zune)') = true THEN 'Mobile'
  WHEN REGEXP_CONTAINS(user_agent, '(Mobile|iP(hone|od|ad)|Android|BlackBerry|IEMobile|Kindle|NetFront|Silk-Accelerated|(hpw|web)OS|Fennec|Minimo|Opera M(obi|ini)|Blazer|Dolfin|Dolphin|Skyfire|Zune)') = false THEN 'Desktop'
  ELSE ''
  END as DeviceType,
  CASE WHEN post_prop3 is null then ''
  WHEN REGEXP_CONTAINS(post_prop3, 'web$' ) = true THEN 'web'
  WHEN REGEXP_CONTAINS(post_prop3, 'msite$' ) = true THEN 'mobile'
  else 'other'
  END as SiteType,
  date_time,
  referrer,
  CASE
  when visit_page_num = 1 then ref_domain
  else null
  end as VisitRefDomain,
  CASE
  when visit_page_num = 1 then ref_type
  else null
  end as VisitRefType,
  IF(visit_page_num=1,1,0) as Visits,
  IF(post_page_event=101,1,0) as Downloads,
  CASE
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) > 30 then 'Greater than 30 seconds'
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) >= 15 then '15-30 seconds'
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) >= 10 then '10-15 seconds'
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) >= 6 then '6-10 seconds'
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) >= 3 then '3-5 seconds'
  when (SAFE_CAST(post_evar60 AS FLOAT64)/10) < 3 then 'Less than 3 seconds'
  else 'Unknown'
  end as PageLoadTime,
  CASE when post_evar65 is null then null
  when post_evar65 = 'false' then null
  when post_evar65 LIKE 'true%' then 'enabled'
  else 'Err-roar'
  end as AdBlock,
  post_evar23 as LinkTracking,
  IF(post_event_list LIKE '%,230,%',post_evar23,null) as ActivityCentreClick,
  IF(post_event_list LIKE '%,203,%',post_evar23,null) as LoginEvent,
  eVar11 as NewsKey,

 IF ( nextstamp is not null and date_time is not null, 
            IF( TIMESTAMP_DIFF(nextstamp,date_time,SECOND) < 0,  0,TIMESTAMP_DIFF(nextstamp,date_time,SECOND) )
        , 0) as SecondsSpent,
IF (nextvisnum is null and visit_page_num = 1,1,0) PageBounce,
IF(
post_event_list LIKE '%,11,%' OR post_event_list LIKE '%,11' OR post_event_list LIKE '11,%'
--OR post_event_list LIKE '%,152,%' OR post_event_list LIKE '%,152' OR post_event_list LIKE '152,%'
--OR post_event_list LIKE '%,153,%' OR post_event_list LIKE '%,153' OR post_event_list LIKE '153,%'
--OR post_event_list LIKE '%,154,%' OR post_event_list LIKE '%,154' OR post_event_list LIKE '154,%'
,1,0) as NumComments,
IF(post_page_event !=0 and event_list LIKE '%20118%' ,1,0) as LinksClicked,
--SUM(IF(post_evar23 LIKE '%https%' AND post_evar23 LIKE '%story%',1,0)) as LinksClicked,
SourceCode,
nextArticleID,
prevArticleID,
/* Define AdLite pageview where member is a subscriber and brand is newscomau */
CASE WHEN post_prop2='newscomau' AND 
     ( (SourceCode='ADOBE AMP' and post_prop15='premium') 
                OR
        (SourceCode != 'ADOBE AMP' and post_prop14 = 'subscriber')
    ) 
    THEN
        1
ELSE
    0
END as AdLiteArticleView,
/* BIDEV-4580 - Softbreaches is fired with evar54 containing softbreach type
post_prop16 is the breach destination
*/
CASE WHEN  (event_list LIKE '%,202,%' or event_list LIKE '%202,%' or event_list LIKE '%,202%' AND  post_page_event=0) 
    and post_prop16 is not null 
    and evar54 is not null THEN
    1
ELSE
    0
END softBreachView,
evar54 softBreachType,
BREACH_TYPE,
TRACKING_CODE,
UUID,
is_breach_sub_flag,
is_non_breach_sub_flag,
is_share_flag,
is_link_clicks_flag,
is_activity_centre_clicks_flag,
is_newsletter_signup_flag,
is_videostart_flag,
is_article_comment_flag,
is_comment_reply_flag,
is_comment_like_flag,
is_comment_viewmore_flag,
if((is_article_comment_flag=1 or is_comment_reply_flag=1 or is_comment_like_flag=1 or is_comment_viewmore_flag=1),1,0) as is_comment_action_flag,
newsletter,
campaign,
section_level_4,
section_level_5,
Cohort_Traffic,
Masthead_Traffic,
Score_Traffic,
Test_Traffic,
-------
-- newsletter_type,
-- newsletter_position1,
-- newsletter_position2,
-------
 evar34 as page_url,
 breach_destination,
 think_source_code,
 think_pkg_code,
 gallery_image,
 gallery_id,
--if ((ifnull(post_prop9,'Unspecified') LIKE '%story%' or ifnull(post_prop9,'Unspecified') LIKE '%article%' or
 --ifnull(post_prop9,'Unspecified')
 --like '%blogs%' or ifnull(post_prop9,'Unspecified') LIKE '%gallery%'),'Article','Non-Article') Type,
datepartitioned as  dw_partition_date,
IngestTime as src_ingest_time,
Type

-----

from  adobe_raw_records_02
)
select *,
ops_ncfr.GENERATE_NQUID("NCA_5231_1_CAPI",ArticleID) ArticleIDKey,
ops_ncfr.GENERATE_NQUID("NCA_6369_1_THNK",PCSID) CONSUMER_PROFILE_ID,
UPPER(to_hex(sha256(LTRIM(RTRIM(UPPER(IFNULL(CAST( UUID AS STRING),'-1'))))))) ADOBE_UUID
 from adobe_raw_records_03
--- )