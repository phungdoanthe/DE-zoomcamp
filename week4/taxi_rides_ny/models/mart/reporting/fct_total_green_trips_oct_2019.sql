select sum(total_monthly_trips) as sum_total_monthly_trips
from {{ ref("fct_monthly_zone_revenue")}}
where service_type = 'Green' and revenue_month ='2019-10-01'