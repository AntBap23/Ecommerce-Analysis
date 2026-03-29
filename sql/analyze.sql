-- analysis


-- conversion percentage overall
select 
    "group",
    count(*) as total_users,
    sum(converted) as conversions,
    round(avg(converted) * 100.0, 4) as conversion_rate_pct
from ab_data_clean as ab
join countries as c on ab.user_id = c.user_id
group by "group"
order by "group";


-- conversion percentage per country

select 
    country,
    "group",
    count(*) as total_users,
    sum(converted) as conversions,
    round(avg(converted) * 100.0, 4) as conversion_rate_pct
from ab_data_clean as ab
join countries as c on ab.user_id = c.user_id
group by "group", country
order by "group", country, total_users desc;


-- Avg time on page
SELECT 
    "group",
    converted,
    AVG(
        CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)
    ) AS avg_seconds_on_page
FROM ab_data_clean as ab 
join countries as c on ab.user_id = c.user_id
GROUP BY "group", converted
ORDER BY "group", converted;


-- Avg time on page by country
SELECT 
	country,
    "group",
    converted,
    AVG(
        CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)
    ) AS avg_seconds_on_page
FROM ab_data_clean as ab 
join countries as c on ab.user_id = c.user_id
GROUP BY "group", converted, country
ORDER BY "group", converted;


-- summary statistics on session time
SELECT 
    "group",
    converted,
    ROUND(MIN(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS min_seconds,
    ROUND(MAX(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS max_seconds,
    ROUND(AVG(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS avg_seconds,
    ROUND(STDDEV(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS stddev_seconds
FROM ab_data_clean as ab
join countries as c on ab.user_id = c.user_id
GROUP BY "group", converted
ORDER BY "group", converted;


-- summary statistics on session time
SELECT 
    country,
    "group",
    converted,
    ROUND(MIN(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS min_seconds,
    ROUND(MAX(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS max_seconds,
    ROUND(AVG(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS avg_seconds,
    ROUND(STDDEV(CAST(SPLIT_PART(timestamp, ':', 1) AS NUMERIC) * 60 +
        CAST(SPLIT_PART(timestamp, ':', 2) AS NUMERIC)), 2) AS stddev_seconds
FROM ab_data_clean as ab
join countries as c on ab.user_id = c.user_id
GROUP BY "group", converted, country
ORDER BY "group", converted;

