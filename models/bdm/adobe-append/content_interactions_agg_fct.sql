{{ config(
    materialized='table',
    cluster_by= "ArticleId"
)}}


--create or replace table `ncau-data-newsquery-dev.bdm_verity.content_interactions_agg_fct` 
--cluster by ArticleId
--as 
select ArticleId,
Brand,
MemberType,
--sourcecode as SourceCode,
ContentType,
devicetype as DeviceType,
BreachType,
IndividualGender,
LifestageDesc,
HouseholdIncomeDesc,
MosaicType,
IndividualAge,
AffluenceDesc,
VisitType,
sum(TotalPageViews) TotalPageViews,
sum(PageView_web) as PageView_web,
sum(PageView_amp) as PageView_amp,
sum(PageView_app) as PageView_app,

sum(BreachVisit) BreachVisit,
sum(breachvisit_web) as BreachVisit_web,
sum(breachvisit_app) as BreachVisit_app,
sum(BreachVisitamp) as BreachVisitamp,

sum(SecondsSpent) SecondsSpent,
sum(SecondSpent_web ) as SecondSpent_web,
sum(SecondSpent_amp) as SecondSpent_amp,
sum(SecondSpent_app) as SecondSpent_app,

sum(TotalShares) TotalShares,
sum(Shares_web) as Shares_web,
sum(Shares_amp) as Shares_amp,
sum(Shares_app) as Shares_app,

sum(TotalNewsLetterSignups) TotalNewsLetterSignups,
sum(NewsLetterSignup_web) as NewsLetterSignup_web,
sum(NewsLetterSignup_amp) as NewsLetterSignup_amp,
sum(NewsLetterSignup_app) as NewsLetterSignup_app,

sum(TotalCommentActions) TotalCommentActions,
sum(CommentAction_web) as CommentAction_web,
sum(CommentAction_amp) as CommentAction_amp,
sum(CommentAction_app) as CommentAction_app,

sum(CommentViewMore) CommentViewMore,
sum(CommentViewMore_web) as CommentViewMore_web,
sum(CommentViewMore_amp) as CommentViewMore_amp,
sum(CommentViewMore_app) as CommentViewMore_app,

sum(TotalCommentReplies) TotalCommentReplies,
sum(CommentReplies_web) as CommentReplies_web,
sum(CommentReplies_amp) as CommentReplies_amp,
sum(CommentReplies_app) as CommentReplies_app,

sum(TotalCommentLikes) TotalCommentLikes,
sum(TotalComment_web) as TotalComment_web,
sum(TotalComment_amp) as TotalComment_amp,
sum(TotalComment_app) as TotalComment_app,

sum(TotalBodyLinksClicked) TotalBodyLinksClicked,
sum(BodyLinksClicked_web) as BodyLinksClicked_web ,
sum(BodyLinksClicked_app) as BodyLinksClicked_app ,
sum(BodyLinksClicked_amp) as BodyLinksClicked_amp ,

from {{ ref('content_interactions_daily_agg_fct') }}
group by
ArticleId,
Brand,
MemberType,
--SourceCode,
ContentType,
DeviceType,
BreachType,
IndividualGender,
LifestageDesc,
HouseholdIncomeDesc,
MosaicType,
IndividualAge,
AffluenceDesc,
VisitType