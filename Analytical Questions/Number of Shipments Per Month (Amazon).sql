select 
 count(shipment_id),
 date_format(shipment_date, '%Y-%m') as date_ym
from amazon_shipment
group by date_format(shipment_date, '%Y-%m');