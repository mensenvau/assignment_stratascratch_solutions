# Import your libraries
import pyspark
from pyspark.sql.functions import sum, max, round, col, when, date_format

# Start writing code
df = sf_events \
    .groupBy("account_id", "user_id") \
    .agg(date_format(max("date"), "yyyy-MM").alias("last_date")) \
    .withColumn("dec", when(col("last_date") > "2020-12", 1).otherwise(0) ) \
    .withColumn("jan", when(col("last_date") > "2021-01", 1).otherwise(0) ) \
    .groupBy("account_id") \
    .agg(round(sum("jan")/sum("dec")).alias("retention_percentage"))
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()