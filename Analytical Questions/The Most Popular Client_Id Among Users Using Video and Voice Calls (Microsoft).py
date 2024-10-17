# Import your libraries
import pyspark
from pyspark.sql.functions import col, when, count, sum


# Start writing code
arr = [
    'video call received',
    'video call sent',
    'voice call received', 
    'voice call sent'
    ]

df = fact_events \
    .groupBy("user_id") \
    .agg((
        sum(when(col("event_type").isin(arr), 1).otherwise(0)) / 
        count("event_type")).alias("per_user")
    ) \
    .join(fact_events, "user_id") \
    .filter(col("per_user") >= 0.5) \
    .select("client_id") \
    .orderBy(col("per_user").desc()) \
    .limit(1)

    #.where(col("prc") >= 50)
 
    
    
    
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()