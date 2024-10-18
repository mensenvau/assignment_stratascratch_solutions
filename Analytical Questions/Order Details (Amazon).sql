select 
   c.first_name,
   o.order_date,
   o.order_details,
   o.total_order_cost
from customers c
inner join orders o on c.id = o.cust_id 
where c.first_name in ("Eva", "Jill");