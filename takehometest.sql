-- PART 1: CHECKING DATA FORMAT/STRUCTURE
----------------------------------------------------------------
----------------------------------------------------------------

select count(distinct user_id) as distinct_users
	,count(distinct account_id) as distinct_accounts
	,count(*) as size_table
from source_data.tasks_used_da;

select min(date)
	,max(date)
	,count(distinct date)
from source_data.tasks_used_da;

--Note: we have data for every date between jan 1 and june 1, 2017


-- PART 2: QUERY THAT I USED TO ANALYSE ACTIVE USERS AND CHURN
----------------------------------------------------------------
----------------------------------------------------------------


with dates as (
select distinct date as day_date -- from above I know that this will generate a dataset of complete dates within timeframe; use this as a join table
from source_data.tasks_used_da)

,user_active_dates as (
select user_id
	,1 				as is_active
	,d.day_date 	as active_date 
	,active_date - lead(active_date, 1) over (partition by user_id order by active_date asc) 	as diff_next_active_date
	,row_number() over (partition by user_id order by active_date desc) 						as row_num_inv
	,row_number() over (partition by user_id order by active_date asc) 							as row_num
	,'2017-06-02' - active_date 																as diff_from_end_of_period
from source_data.tasks_used_da da
left join dates d ON d.day_date <= da.date + interval '28 days' -- generate an "active" date for that day and the 28 days following
	and d.day_date >= da.date
group by 1,3  -- only want one row per active date, per user
order by 1,3) 

,user_churn_dates as (
select user_id
	,1 			as is_churn
	,row_num 	as active_days_before_churn
	,day_date	as churn_date
from user_active_dates ua
left join dates d ON d.day_date <= ua.active_date + interval '28 days'
	and d.day_date > ua.active_date -- generate a "churn" date for the 28 days following the last active date
where diff_next_active_date < -1 -- we either look for dates where there is a gap from one active date to the next..
or (row_num_inv = 1 and diff_from_end_of_period > 1) -- ..or when the last active date is before the end of the time period
group by 1,3,4 -- -- only want one row per churned date, per user, also keep active days before churn so we can use to group later
)

,final as -- create a union of user,date pairs to indicate when each user is active and/or churned
(
select user_id
	,active_date 	as date 
	,is_active
	,row_num 		as active_day
	,0 				as is_churn
	,NULL 			as active_days_before_churn
from user_active_dates

union 

select user_id
	,churn_date 	as date 
	,0 				as is_active
	,NULL 			as active_day
	,is_churn
	,active_days_before_churn
from user_churn_dates

order by 1,3)

select date
	,sum(is_active) 															as MAU -- summing all users active on that day
	,sum(case when active_day = 1 then is_active end) 							as new_MAU -- summing all users active on that day for the first time (active day = 1)
	,sum(is_churn) 																as churn -- summing all users who count as churn on that day
	,sum(case when active_days_before_churn <= 29 then is_churn else 0 end) 	as quick_churn -- summing all users who count as churn on that day who were only active for one period 
from final
group by 1 -- grouped by day
order by 1 -- ordered by day 
;