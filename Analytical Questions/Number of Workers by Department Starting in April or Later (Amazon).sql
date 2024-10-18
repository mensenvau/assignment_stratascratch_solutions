select department, count(*) as num_workers
from worker
where joining_date >= "2014-04-01"
group by department;