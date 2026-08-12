select 
  "USER_ID", 
  "ACTION", 
  to_char("DATES", 'YYYY-MM-DD') as 
  "DATES"
from 
  "USERS_01";
