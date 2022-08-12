
{% macro if_statement() %}

if((post_event_list like  '=%215,%' or post_event_list like '%,215,%' or post_event_list like '%,215' or post_event_list like '215,%')
and post_prop9 LIKE 'article'
and post_page_event = 0
and post_evar16 is not null
,1,0) as is_breach_sub_flag,
if((post_event_list like  '=%215,%' or post_event_list like '%,215,%' or post_event_list like '%,215' or post_event_list like '215,%')
and post_prop9 LIKE 'acq+shopfront'
and post_evar16 is not null
,1,0) as is_non_breach_sub_flag,
if((post_event_list like  '=%205,%' or post_event_list like '%,205,%' or post_event_list like '%,205' or post_event_list like '205,%')
and post_page_event in (0,100)
,1,0) as is_share_flag,
if((post_event_list like  '=%209,%' or post_event_list like '%,209,%' or post_event_list like '%,209' or post_event_list like '209,%')
and post_page_event in (0,100,102)
,1,0) as is_link_clicks_flag,
if((post_event_list like  '=%230,%' or post_event_list like '%,230,%' or post_event_list like '%,230' or post_event_list like '230,%')
,1,0) as is_activity_centre_clicks_flag,
if((post_event_list like  '=%20140,%' or post_event_list like '%,20140,%' or post_event_list like '%,20140' or post_event_list like '20140,%')
and post_page_event in (100)
,1,0) as is_newsletter_signup_flag,
if((post_event_list like  '=%701,%' or post_event_list like '%,701,%' or post_event_list like '%,701' or post_event_list like '701,%')
and post_page_event in (76)
,1,0) as is_videostart_flag,
--Comments
if((post_event_list like  '=%210,%' or post_event_list like '%,210,%' or post_event_list like '%,210' or post_event_list like '210,%')
and post_page_event in (0,100)
,1,0) as is_article_comment_flag,
if((post_event_list like  '=%20151,%' or post_event_list like '%,20151,%' or post_event_list like '%,20151' or post_event_list like '20151,%')
and post_page_event in (0,100)
,1,0) as is_comment_reply_flag,
if((post_event_list like  '=%20152,%' or post_event_list like '%,20152,%' or post_event_list like '%,20152' or post_event_list like '20152,%')
and post_page_event in (0,100)
,1,0) as is_comment_like_flag,
if((post_event_list like  '=%20153,%' or post_event_list like '%,20153,%' or post_event_list like '%,20153' or post_event_list like '20153,%')
and post_page_event in (0,100)
,1,0) as is_comment_viewmore_flag,
####################campaign columns
case when lower(split(campaign,'{')[offset(0)]) like '%facebook%' then 'Facebook'
when lower(split(campaign,'{')[offset(0)]) like '%email%' then 'Email'
when lower(split(campaign,'{')[offset(0)]) like '%referral%' then 'Referral'
else 'UNKNOWN' end as newsletter,
case when (lower(split(campaign,'{')[offset(0)]) like '%facebook%' or lower(split(campaign,'{')[offset(0)]) like '%email%' or lower(split(campaign,'{')[offset(0)]) like '%referral%')
 then upper(split(campaign,'{')[offset(0)])
else 'UNKNOWN' end as campaign,
post_prop7 as section_level_4,
post_prop8 as section_level_5,
####################breach columns
CASE WHEN upper(post_evar21) like '%-HOT-%' THEN 'HOT' 
WHEN upper(post_evar21) like '%-WARM-%' THEN 'WARM'
WHEN upper(post_evar21) like '%-COLD-%' THEN 'COLD'
ELSE
'UNKNOWN'
END as Cohort_Traffic,
substr(post_evar21,1,2)   as Masthead_Traffic,
CASE WHEN upper(post_evar21) like '%-SCORE-%' THEN 'SCORE' 
WHEN upper(post_evar21) like '%-NOSCORE-%' THEN 'NOSCORE'
ELSE
'UNKNOWN'
END as  Score_Traffic,
CASE WHEN upper(post_evar21) like '%-TEST-%' THEN 'TEST' 
WHEN upper(post_evar21) like '%-CONTROL-%' THEN 'CONTROL'
ELSE
'UNKNOWN'
END  as Test_Traffic,
--------
-- split(evar127,'|')[safe_ordinal(1)] newsletter_type,
-- split(evar127,'|')[safe_ordinal(2)] newsletter_position1,
-- split(evar127,'|')[safe_ordinal(2)] newsletter_position2,
-----------------selecting columns for non-articles
--CASE when post_prop14 = 'registered' then 'Registered' when post_prop14 = 'subscriber' then 'Subscriber'
 --else 'Anonymous' end member_type,
evar37 breach_destination,
evar13 think_source_code,
evar38 think_pkg_code,
evar39 gallery_image,
evar48 gallery_id,
if ((ifnull(post_prop9,'Unspecified') LIKE '%story%' or ifnull(post_prop9,'Unspecified') LIKE '%article%' or
 ifnull(post_prop9,'Unspecified')
 like '%blogs%' or ifnull(post_prop9,'Unspecified') LIKE '%gallery%'),'Article','Non-Article') Type

 {% endmacro %}