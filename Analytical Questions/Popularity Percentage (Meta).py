# Import your libraries
import pyspark
from pyspark.sql.functions import col, count

# Start writing code
df = facebook_friends.union(facebook_friends.select("user2", "user1"))

total_count = df.select("user1").distinct().count()

df = df \
    .groupBy("user1").agg(count(col("user2")).alias("count")) \
    .withColumn("popularity_percent", col("count")/total_count * 100) \
    .orderBy(col("user1").asc()) \
    .select("user1", "popularity_percent")
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()