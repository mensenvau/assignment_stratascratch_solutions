select 
  c.first_name,
  c.last_name,
  c.city,
  o.order_details
from customers c 
left join orders o on c.id = o.cust_id;
