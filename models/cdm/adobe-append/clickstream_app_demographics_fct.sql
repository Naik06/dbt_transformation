{{
  config(
    materialized = 'table',
    partition_by={
      "field": "dw_partition_date",
      "granularity": "day"
    }
  )
}}



--create or replace table `ncau-data-newsquery-dev.cdm_adobe.clickstream_app_demographics_fct`
-- PARTITION BY
--   dw_partition_date as
--select * from
--(
with adobe_subscriber_events as
(
    /* Select distinct subscribers to reduce number of lookups */
    /* 4136122 rows  -> 152073 rows */
    select distinct CONSUMER_PROFILE_ID as dw_subscriber_party_id
    from {{ ref ( 'clickstream_app_fct' ) }} 
    where dw_partition_date between "2021-11-01" and "2022-01-31"   ----"2021-11-01" and "2022-01-31"---11 hours slower
    --and date(datetime(date_time,"Australia/Sydney")) between  @startDate and @endDate ----between "2021-11-01" and "2022-01-31"
    and MemberType = 'Subscriber'
    and safe_cast(pcsid as INT64) is not null
),subscriber_demographics as
(
    /* Lookup demographics for each unique pcsidi */
    /* lookup 152073 instead of  4136122 */
    select adobe_subscriber_events.dw_subscriber_party_id,
    ifnull(if(individual_gender is not null and individual_gender in ('m','f'), upper(individual_gender),individual_gender),'Not Available') as individual_gender,
  ifnull(lifestage_desc,'Unknown') lifestage_desc,
  ifnull(household_income_desc,'Unknown') household_income_desc,
  ifnull(substr(mosaic_type,1,1),'Unknown') mosaic_type,
  ifnull(individual_age,'Not Available') individual_age,
  ifnull(affluence_desc,'Unknown') affluence_desc
    from adobe_subscriber_events left join {{ source ('dataform','subscriber_demographics_lookup') }} cpsa
    on (adobe_subscriber_events.dw_subscriber_party_id = cpsa.dw_subscriber_party_id)
),
anonymous_subscriber_events as (
   select distinct ADOBE_UUID dw_browser_id ,
    from {{ ref ( 'clickstream_app_fct' ) }} 
    where dw_partition_date between "2021-11-01" and "2022-01-31"   ----"2021-11-01" and "2022-01-31"---11 hours slower
    --and date(datetime(date_time,"Australia/Sydney")) between  @startDate and @endDate ----between "2021-11-01" and "2022-01-31"
    and MemberType != 'Subscriber'
),




anonymous_demographics as (
   select anonymous_subscriber_events.dw_browser_id,
    ifnull(if(individual_gender is not null and individual_gender in ('m','f'), upper(individual_gender),individual_gender),'Not Available') as individual_gender,
  ifnull(lifestage_desc,'Unknown') lifestage_desc,
  ifnull(household_income_desc,'Unknown') household_income_desc,
  ifnull(substr(mosaic_type,1,1),'Unknown') mosaic_type,
  ifnull(individual_age,'Not Available') individual_age,
  ifnull(affluence_desc,'Unknown') affluence_desc,
    from anonymous_subscriber_events left join {{ source ('dataform','anonymous_demographics_lookup') }} cpsa
    on (anonymous_subscriber_events.dw_browser_id  = cpsa.dw_browser_id )
)
select app_subscriber_events.* except(page_view) ,
if (app_subscriber_events.articleid is not null and app_subscriber_events.contenttype not like '%acq+shopfront%'   and app_subscriber_events.contenttype
not like '%breach+shopfront%' and app_subscriber_events.contenttype not like '%shopfront%', page_view , 0) as page_view,
  ifnull(if(individual_gender is not null and individual_gender in ('m','f'), upper(individual_gender),individual_gender),'Not Available') as individual_gender,
  ifnull(lifestage_desc,'Unknown') lifestage_desc,
  ifnull(household_income_desc,'Unknown') household_income_desc,
  ifnull(substr(mosaic_type,1,1),'Unknown') mosaic_type,
  ifnull(individual_age,'Not Available') individual_age,
  ifnull(affluence_desc,'Unknown') affluence_desc
from {{ ref ( 'clickstream_app_fct' ) }}  app_subscriber_events left join subscriber_demographics
on (app_subscriber_events.CONSUMER_PROFILE_ID = subscriber_demographics.dw_subscriber_party_id)
where dw_partition_date between "2021-11-01" and "2022-01-31" ---"2021-11-01" and "2022-01-31"
    --and date(datetime(date_time,"Australia/Sydney")) between @startDate and @endDate -----"2021-11-01" and "2022-01-31"
    and MemberType = 'Subscriber'
    UNION ALL 
  select app_non_subscriber_events.* except(page_view) ,
if (app_non_subscriber_events.articleid is not null and app_non_subscriber_events.contenttype not like '%acq+shopfront%'   and app_non_subscriber_events.contenttype
not like '%breach+shopfront%' and app_non_subscriber_events.contenttype not like '%shopfront%', page_view , 0) as page_view,
  ifnull(if(individual_gender is not null and individual_gender in ('m','f'),
   upper(individual_gender),individual_gender),'Not Available') as individual_gender,
  ifnull(lifestage_desc,'Unknown') lifestage_desc,
  ifnull(household_income_desc,'Unknown') household_income_desc,
  ifnull(substr(mosaic_type,1,1),'Unknown') mosaic_type,
  ifnull(individual_age,'Not Available') individual_age,
  ifnull(affluence_desc,'Unknown') affluence_desc
from {{ ref ( 'clickstream_app_fct' ) }} app_non_subscriber_events left join anonymous_demographics
on ( app_non_subscriber_events.UUID  = anonymous_demographics.dw_browser_id)
where dw_partition_date between "2021-11-01" and "2022-01-31" 
    --and date(datetime(date_time,"Australia/Sydney")) between @startDate and @endDate -----"2021-11-01" and "2022-01-31"
    and MemberType != 'Subscriber'
    --)