-- Find libraries who haven't provided the email address in circulation year 2016 but their notice preference definition is set to email
select 
  distinct home_library_code  
from library_usage
where 
  provided_email_address = false and 
  circulation_active_year = 2016 and
  notice_preference_definition = "email";