-- fixing any data and sanity checks


-- check ab_data

-- counts 
SELECT 
 count(*) filter (where "group" = 'control' and landing_page = 'old_page') as correct_old, 
 count(*) filter (where "group" = 'treatment' and landing_page = 'new_page') as correct_new,
 count(*) filter (where "group" = 'control' and landing_page = 'new_page') as wrong_old,
 count(*) filter (where "group" = 'treatment' and landing_page = 'old_page') as wrong_new
 
FROM public.ab_data;


-- clean data 
CREATE TABLE ab_data_clean AS
SELECT *
FROM ab_data
WHERE 
    ("group" = 'control'   AND landing_page = 'old_page')
 OR ("group" = 'treatment' AND landing_page = 'new_page');



-- percentages to see if testing went wrong
with cte as (
select 
 count(*) filter (where "group" = 'control' and landing_page = 'old_page') as control_count,
 count(*) filter (where "group" = 'treatment' and landing_page = 'new_page') as treatment_count,
 count(*) as total
from public.ab_data_clean
)

select 
	control_count,
	treatment_count,
	round( 100.0 * control_count / total, 2) as control_pct,
 	round( 100.0 * treatment_count / total, 2) as treatment_pct
from cte


-- check countries

--  country count 
Select country, count(*)
from public.countries
group by country 
order by country desc



-- combine countries and ab_data




with cte as (
select 
 country,
 count(*) filter (where "group" = 'control' and landing_page = 'old_page') as control_count,
 count(*) filter (where "group" = 'treatment' and landing_page = 'new_page') as treatment_count,
 count(*) as total
from ab_data_clean as ab
join countries as c on ab.user_id = c.user_id
group by country
)

select 
	country,
	control_count,
	treatment_count,
	round( 100.0 * control_count / total, 2) as control_pct,
 	round( 100.0 * treatment_count / total, 2) as treatment_pct
from cte
order by control_count desc, treatment_count desc