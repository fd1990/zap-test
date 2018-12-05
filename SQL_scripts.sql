-- I generate 6 tables; 3 are used to generate visualisations and 3 show my working to get to that point 

-- #1 fdevlin.date_join_table
-- #2 fdevlin.user_monthly_active_dates
-- #3 fdevlin.user_monthly_churn_dates
-- #4 fdevlin.summary_mau
-- #5 fdevlin.summary_dau
-- #6 fdevlin.cohorted_dau


-- Table 1: date join table 
-- from checking row count,min/max date I know that this will generate a dataset of complete dates within timeframe; use this as a join table
DROP TABLE fdevlin.date_join_table;
CREATE TABLE fdevlin.date_join_table AS 

SELECT date as day_date 
FROM source_data.tasks_used_da
GROUP BY 1;


-- Table 2: user monthly active dates
-- A user is considered active on any day where they have at least one task executed in the prior 28 days. 
DROP TABLE fdevlin.user_monthly_active_dates;
CREATE TABLE fdevlin.user_monthly_active_dates AS 
(
SELECT user_id
	,d.day_date 											as active_date 
	,1 												as is_active
	,lead(active_date, 1) over (partition by user_id order by active_date asc) - active_date	as diff_next_active_date
	,row_number() over (partition by user_id order by active_date desc) 				as row_num_inv
	,row_number() over (partition by user_id order by active_date asc) 				as row_num
	,'2017-06-02' - active_date 																as diff_from_end_of_period
FROM source_data.tasks_used_da da
LEFT JOIN fdevlin.date_join_table d ON d.day_date <= da.date + interval '28 days' -- generate an "active" date for that day and the 28 days following
	AND d.day_date >= da.date
WHERE sum_tasks_used > 0
GROUP BY 1,2  -- only want one row per active date, per user
ORDER BY 1,2
); 


-- Table 3: user monthly churn dates
-- A user is considered to be churn the 28 days following their last being considered active, and a user is no longer part of churn if they become active again.

DROP TABLE fdevlin.user_monthly_churn_dates;
CREATE TABLE fdevlin.user_monthly_churn_dates AS

WITH first_churn_date as (
SELECT user_id
	,1 								as is_churn
	,row_num 							as active_days_before_churn
	,active_date							as last_active_date
	,COALESCE(diff_next_active_date, diff_from_end_of_period) - 1 	as max_churn_period -- we add the "-1" to the diff_next_active to get days between active dates rather than days until the next active date
FROM fdevlin.user_monthly_active_dates ua
WHERE diff_next_active_date > 1 -- we either look for dates where there is a gap from one active date to the next..
	OR (row_num_inv = 1 and diff_from_end_of_period > 1) -- ..or when the last active date is before the end of the time period
)

,all_churn_dates as (
SELECT user_id
	,1 as is_churn
	,active_days_before_churn
	,max_churn_period
	,d.day_date as churn_date
	,row_number() over (partition by user_id, last_active_date order by churn_date asc) as row_num
FROM first_churn_date fcd
LEFT JOIN fdevlin.date_join_table d ON d.day_date <= fcd.last_active_date + interval '28 days'
	AND d.day_date > fcd.last_active_date -- generate a "churn" date for the 28 days following the last active date
ORDER BY 1,4,5)

SELECT user_id
	,churn_date
	,is_churn
	,active_days_before_churn
FROM all_churn_dates
WHERE row_num <= max_churn_period -- we want to ensure that we don't keep counting churn after the timeframe (2017-06-01) or if the user becomes active again
ORDER BY 1,2
;


-- Table 4: Model that allows you to analyze and visualize Monthly Active Users and churn over time

DROP TABLE fdevlin.summary_mau;
CREATE TABLE fdevlin.summary_mau AS 

WITH union_data as -- create a union of user,date pairs to indicate when each user is active and/or churned
(
select user_id
	,active_date 	as date 
	,is_active
	,row_num 	as active_day
	,0 		as is_churn
	,NULL 		as active_days_before_churn
from fdevlin.user_monthly_active_dates

union 

select user_id
	,churn_date 	as date 
	,0 		as is_active
	,NULL 		as active_day
	,is_churn
	,active_days_before_churn
from fdevlin.user_monthly_churn_dates

order by 1,3)

select date
	,sum(is_active) 								as MAU -- summing all users active on that day
	,sum(case when active_day = 1 then is_active end) 				as new_MAU -- summing all users active on that day for the first time (active day = 1)
	,sum(is_churn) 									as churn -- summing all users who count as churn on that day
	,sum(case when active_days_before_churn <= 29 then is_churn else 0 end) 	as quick_churn -- summing all users who count as churn on that day who were only active for one period 
from union_data ud
group by 1 -- grouped by day
order by 1 -- ordered by day 
;


-- Table 5: Model that allows you to analyze and visualize Daily Active Users over time; and compare this to MAU
DROP TABLE fdevlin.summary_dau;
CREATE TABLE fdevlin.summary_dau AS 

SELECT da.date
	,COUNT(DISTINCT da.user_id) as dau
	,mau.mau
FROM source_data.tasks_used_da da
LEFT JOIN fdevlin.summary_mau mau on da.date = mau.date
WHERE sum_tasks_used > 0 
GROUP BY 1,3
ORDER BY 1;	


-- Table 6: Model that allows you to analyze Daily Active Users cohort retention over time

DROP TABLE fdevlin.cohorted_dau;
CREATE TABLE fdevlin.cohorted_dau AS 

WITH user_dim as (select
	user_id
	,active_date as first_active_date
from fdevlin.user_monthly_active_dates 
where row_num = 1)

SELECT date(date_trunc('month',ud.first_active_date)) 	as month_first_active
	,da.date - first_active_date 			as days_since_active
	,COUNT(DISTINCT da.user_id) 			as number_users_active
FROM source_data.tasks_used_da da
LEFT JOIN user_dim ud on da.user_id = ud.user_id
WHERE days_since_active <= 28
	AND sum_tasks_used > 0
GROUP BY 1,2
ORDER BY 1,2
; 
