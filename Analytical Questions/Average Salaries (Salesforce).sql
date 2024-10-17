select 
   department, 
   first_name,
   salary,
   AVG(salary) over (PARTITION BY department)
from employee;