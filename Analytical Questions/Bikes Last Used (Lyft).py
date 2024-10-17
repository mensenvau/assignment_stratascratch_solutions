# Import your libraries
import pyspark
from pyspark.sql.functions import col, max

# Start writing code
df = dc_bikeshare_q1_2012 \
    .groupBy("bike_number").agg(max("end_time").alias("last_used"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()