select 
  count(*) as n_admins
from worker
where department = "Admin" and joining_date >= '2014-04-01';