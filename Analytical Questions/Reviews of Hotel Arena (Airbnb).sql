select 
  hotel_name,
  reviewer_score,
  count(*)
from hotel_reviews
where hotel_name = "Hotel Arena"
group by reviewer_score;