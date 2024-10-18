select 
  city,
  property_type,
  avg(bathrooms) as n_bathrooms_avg,
  avg(bedrooms) as n_bedrooms_avg
from airbnb_search_details
group by city,
  property_type;