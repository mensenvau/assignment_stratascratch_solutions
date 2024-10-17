# Import your libraries
import pyspark
from pyspark.sql.functions import when, col, min, max, avg, concat_ws

# Start writing code
df = airbnb_host_searches \
    .withColumn("host_id", concat_ws("price", "room_type", "host_since", "zipcode", "number_of_reviews")) \
    .select("host_id", "price", "number_of_reviews") \
    .distinct() \
    .withColumn("host_popularity", 
     when(col("number_of_reviews") > 40, 'Hot') \
    .when(col("number_of_reviews") > 15, 'Popular') \
    .when(col("number_of_reviews") > 5, 'Trending Up') \
    .when(col("number_of_reviews") > 0, 'Rising') \
    .when(col("number_of_reviews") == 0, "New") \
    ) \
    .select("host_popularity", "price") \
    .groupBy("host_popularity") \
    .agg(
    min("price").alias("min_price"),
    avg("price").alias("avg_price"),
    max("price").alias("max_price"),
    )

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()