select
  t1.location, 
  avg(t2.popularity) as avg_popularity
from facebook_employees t1
left join facebook_hack_survey t2 
 on t1.id = t2.employee_id
group by t1.location 
;