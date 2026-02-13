select count(*) as total_row 
from {{ ref('fct_monthly_zone_revenue')}}

-- 12184 --