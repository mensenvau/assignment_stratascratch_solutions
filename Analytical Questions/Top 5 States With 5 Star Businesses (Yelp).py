# Import your libraries
import pyspark
from pyspark.sql.functions import col, count, rank
from pyspark.sql.window import Window

# Start writing code
df = yelp_business \
    .where(col("stars") == 5) \
    .groupBy("state").agg(count(col("business_id")).alias("n_businesses")) \
    .orderBy(col("n_businesses").desc(), col("state").asc()) \
    .withColumn("rn", rank().over(Window.orderBy(col("n_businesses").desc()))) \
    .where(col("rn") <= 5) \
    .select("state", "n_businesses")

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()