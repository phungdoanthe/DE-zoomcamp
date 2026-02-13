select 
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_amount_2020
from {{ ref('fct_monthly_zone_revenue') }}
where extract(year from revenue_month) = 2020 and service_type ='Green'
group by pickup_zone
order by total_amount_2020 desc