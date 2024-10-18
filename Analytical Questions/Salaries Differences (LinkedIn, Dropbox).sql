with cte as (
 select
  t2.department,
  max(t1.salary) as max_salary
 from db_employee t1
 left join db_dept t2 on t1.department_id = t2.id
 group by t2.department
)

select 
abs(
 (select max_salary from cte where department = "marketing" ) - 
 (select max_salary from cte where department = "engineering")
) as salary_difference;

