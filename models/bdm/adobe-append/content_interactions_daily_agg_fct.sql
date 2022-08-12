
{{
  config(
    materialized = 'table',
    cluster_by = "articleid",
    partition_by={
      "field": "VisitDay",
      "granularity": "day"
    }
  )
}}


--create or replace table `ncau-data-newsquery-dev.bdm_verity.content_interactions_daily_agg_fct` 
 -- PARTITION BY
--VisitDay
--CLUSTER BY
 ---  articleid
  --as 
--select * from (

with d1 as (
select articleid
--,date_time 
,date(datetime(date_time,"Australia/Sydney")) as VisitDay
,brand,membertype,sourcecode,contenttype,devicetype,breach_type,
individual_gender,
lifestage_desc,
household_income_desc,
mosaic_type,
individual_age,
affluence_desc,
CASE
WHEN visitRefType = 1 THEN 'INTERNAL'
WHEN visitRefType = 2 THEN 'Other Web Sites'
WHEN visitRefType = 3 THEN 'Search'
WHEN visitRefType = 4 THEN 'Hard Drive'
WHEN visitRefType = 5 THEN 'NewsGroups'
WHEN visitRefType = 6 THEN 'Typed/ Bookmarked'
WHEN visitRefType = 7 THEN 'Email'
WHEN visitRefType = 8 THEN 'No JavaScript'
WHEN visitRefType = 9 THEN 'Social Networks'
END AS VisitType,

sum(page_view) as page_view_web,0 as page_view_app,0 as page_view_amp,
sum(breachvisit) as breachvisit_web,0 as breachvisit_app,0 as breachvisit_amp ,
sum(SecondsSpent) as SecondsSpent_web,0 as SecondsSpent_app,0 as SecondsSpent_amp ,
sum(is_share_flag) as is_share_flag_web,0 as is_share_flag_app,0 as is_share_flag_amp ,
sum(is_newsletter_signup_flag) as is_newsletter_signup_flag_web,0 as is_newsletter_signup_flag_app,0 as is_newsletter_signup_flag_amp,
sum(is_comment_action_flag) as is_comment_action_flag_web, 0 as is_comment_action_flag_app,0 as is_comment_action_flag_amp, 
sum(is_comment_viewmore_flag) as  is_comment_viewmore_flag_web,0 as is_comment_viewmore_flag_app, 0 as is_comment_viewmore_flag_amp,

sum(is_comment_reply_flag) as is_comment_reply_flag_web, 0 as  is_comment_reply_flag_app,0  as is_comment_reply_flag_amp,
sum(is_comment_like_flag) as is_comment_like_flag_web,0 as is_comment_like_flag_app, 0 as is_comment_like_flag_amp,
0 as BodyLinksClicked_web,sum(LinksClicked) as BodyLinksClicked_app,0 as BodyLinksClicked_amp,
 from
{{ ref ('clickstream_web_demographics_fct') }}
 where dw_partition_date between "2021-11-01" and"2021-11-30"
 ---and contenttype not like '%shopfront%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15


UNION ALL

select articleid
--,date_time
,date(datetime(date_time,"Australia/Sydney")) as VisitDay
,brand,membertype,sourcecode,contenttype,devicetype,breach_type,
individual_gender,
lifestage_desc,
household_income_desc,
mosaic_type,
individual_age,
affluence_desc,
CASE
WHEN visitRefType = 1 THEN 'INTERNAL'
WHEN visitRefType = 2 THEN 'Other Web Sites'
WHEN visitRefType = 3 THEN 'Search'
WHEN visitRefType = 4 THEN 'Hard Drive'
WHEN visitRefType = 5 THEN 'NewsGroups'
WHEN visitRefType = 6 THEN 'Typed/ Bookmarked'
WHEN visitRefType = 7 THEN 'Email'
WHEN visitRefType = 8 THEN 'No JavaScript'
WHEN visitRefType = 9 THEN 'Social Networks'
END AS VisitType,

0 as page_view_web,sum(page_view) as page_view_app,0 as page_view_amp,
0 as breachvisit_web,sum(breachvisit) as breachvisit_app,0 as breachvisit_amp ,
0 as SecondsSpent_web,sum(SecondsSpent)  as SecondsSpent_app,0 as SecondsSpent_amp ,
0 as is_share_flag_web,sum(is_share_flag) as is_share_flag_app,0 as is_share_flag_amp ,
0 as is_newsletter_signup_flag_web,sum(is_newsletter_signup_flag) as is_newsletter_signup_flag_app,0 as is_newsletter_signup_flag_amp,
0 as is_comment_action_flag_web, sum(is_comment_action_flag) as is_comment_action_flag_app,0 as is_comment_action_flag_amp, 
0 as  is_comment_viewmore_flag_web,sum(is_comment_viewmore_flag)  as is_comment_viewmore_flag_app, 0 as is_comment_viewmore_flag_amp,

0 as is_comment_reply_flag_web, sum(is_comment_reply_flag) as  is_comment_reply_flag_app,0  as is_comment_reply_flag_amp,
0 as is_comment_like_flag_web,sum(is_comment_like_flag)as is_comment_like_flag_app, 0 as is_comment_like_flag_amp,
0 as BodyLinksClicked_web,sum(LinksClicked) as BodyLinksClicked_app,0 as BodyLinksClicked_amp,
 from
{{ ref ('clickstream_app_demographics_fct') }}
 where dw_partition_date  between "2021-11-01" and"2021-11-30"
-- and contenttype not like '%shopfront%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION ALL

select articleid
--,date_time
, date(datetime(date_time,"Australia/Sydney")) as VisitDay
,brand,membertype
,sourcecode
,contenttype
,devicetype
,breach_type,
individual_gender,
lifestage_desc,
household_income_desc,
mosaic_type,
individual_age,
affluence_desc,
CASE
WHEN visitRefType = 1 THEN 'INTERNAL'
WHEN visitRefType = 2 THEN 'Other Web Sites'
WHEN visitRefType = 3 THEN 'Search'
WHEN visitRefType = 4 THEN 'Hard Drive'
WHEN visitRefType = 5 THEN 'NewsGroups'
WHEN visitRefType = 6 THEN 'Typed/ Bookmarked'
WHEN visitRefType = 7 THEN 'Email'
WHEN visitRefType = 8 THEN 'No JavaScript'
WHEN visitRefType = 9 THEN 'Social Networks'
END AS VisitType,

0 as page_view_web, 0 as page_view_app,sum(page_view)as page_view_amp,
0 as breachvisit_web,0 as breachvisit_app,sum(breachvisit) as breachvisit_amp ,
0 as SecondsSpent_web,0  as SecondsSpent_app,sum(SecondsSpent) as SecondsSpent_amp ,
0 as is_share_flag_web,0 as is_share_flag_app,sum(is_share_flag)as is_share_flag_amp ,
0 as is_newsletter_signup_flag_web,0 as is_newsletter_signup_flag_app,sum(is_newsletter_signup_flag) as is_newsletter_signup_flag_amp,
0 as is_comment_action_flag_web, 0 as is_comment_action_flag_app,sum(is_comment_action_flag) as is_comment_action_flag_amp, 
0 as  is_comment_viewmore_flag_web,0  as is_comment_viewmore_flag_app, sum(is_comment_viewmore_flag) as is_comment_viewmore_flag_amp,

0 as is_comment_reply_flag_web, 0 as  is_comment_reply_flag_app,sum(is_comment_reply_flag)  as is_comment_reply_flag_amp,
0 as is_comment_like_flag_web,0 as is_comment_like_flag_app,sum(is_comment_like_flag)   as is_comment_like_flag_amp,
0 as BodyLinksClicked_web,0 as BodyLinksClicked_app,sum(LinksClicked) as BodyLinksClicked_amp,
 from
{{ ref ('clickstream_amp_demographics_fct') }}
 where dw_partition_date between  "2021-11-01" and"2021-11-30"
-- and contenttype not like '%shopfront%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
,d2 as (
select ArticleId,
VisitDay,
Brand,
MemberType,
VisitType,
--sourcecode as SourceCode,
ContentType,
devicetype as DeviceType,
breach_type BreachType,
individual_gender as IndividualGender,
lifestage_desc as LifestageDesc,
household_income_desc as HouseholdIncomeDesc,
mosaic_type as MosaicType,
individual_age as IndividualAge,
affluence_desc as AffluenceDesc,
sum(page_view_web+page_view_app+page_view_amp) TotalPageViews,
sum(page_view_web) as PageView_web,
sum(page_view_amp) as PageView_amp,
sum(page_view_app) as PageView_app,

sum(breachvisit_web+breachvisit_app+breachvisit_amp) BreachVisit,
sum(breachvisit_web) as BreachVisit_web,
sum(breachvisit_app) as BreachVisitamp,
sum(breachvisit_amp) as BreachVisit_app,

sum(SecondsSpent_web + SecondsSpent_app + SecondsSpent_amp) SecondsSpent,
sum(SecondsSpent_web ) as SecondSpent_web,
sum(SecondsSpent_amp) as SecondSpent_amp,
sum(SecondsSpent_app) as SecondSpent_app,

sum(is_share_flag_web + is_share_flag_app + is_share_flag_amp) TotalShares,
sum(is_share_flag_web) as Shares_web,
sum(is_share_flag_app) as Shares_amp,
sum(is_share_flag_amp) as Shares_app,

sum(is_newsletter_signup_flag_web + is_newsletter_signup_flag_app + is_newsletter_signup_flag_amp) TotalNewsLetterSignups,
sum(is_newsletter_signup_flag_web) as NewsLetterSignup_web,
sum(is_newsletter_signup_flag_app) as NewsLetterSignup_amp,
sum(is_newsletter_signup_flag_amp) as NewsLetterSignup_app,

sum(is_comment_action_flag_web + is_comment_action_flag_app + is_comment_action_flag_amp) TotalCommentActions,
sum(is_comment_action_flag_web) as CommentAction_web,
sum(is_comment_action_flag_app) as CommentAction_amp,
sum(is_comment_action_flag_amp) as CommentAction_app,

sum(is_comment_viewmore_flag_web + is_comment_viewmore_flag_app + is_comment_viewmore_flag_amp) CommentViewMore,
sum(is_comment_viewmore_flag_web) as CommentViewMore_web,
sum(is_comment_viewmore_flag_app) as CommentViewMore_amp,
sum(is_comment_viewmore_flag_amp) as CommentViewMore_app,

sum(is_comment_reply_flag_web + is_comment_reply_flag_app + is_comment_reply_flag_amp) TotalCommentReplies,
sum(is_comment_reply_flag_web) as CommentReplies_web,
sum(is_comment_reply_flag_app) as CommentReplies_amp,
sum(is_comment_reply_flag_amp) as CommentReplies_app,

sum(is_comment_like_flag_web + is_comment_like_flag_app + is_comment_like_flag_amp) TotalCommentLikes,
sum(is_comment_like_flag_web) as TotalComment_web,
sum(is_comment_like_flag_app) as TotalComment_amp,
sum(is_comment_like_flag_amp) as TotalComment_app,

sum(BodyLinksClicked_web + BodyLinksClicked_app + BodyLinksClicked_amp) TotalBodyLinksClicked,
sum(BodyLinksClicked_web) as BodyLinksClicked_web ,
sum(BodyLinksClicked_app) as BodyLinksClicked_app ,
sum(BodyLinksClicked_amp) as BodyLinksClicked_amp ,

from d1
group by ArticleId,
VisitDay,
Brand,
MemberType,
VisitType,
--SourceCode,
ContentType,DeviceType,BreachType,
IndividualGender,
LifestageDesc,
HouseholdIncomeDesc,
MosaicType,
IndividualAge,
AffluenceDesc


)
select * 
 from d2
--)