select * 
from facebook_posts 
where post_id in (
  select distinct post_id from facebook_reactions
  where reaction = "heart" 
);