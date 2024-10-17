# Import necessary libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import *

w1 = Window.partitionBy("user_id").orderBy("created_at")
w2 = Window.partitionBy("user_id", "product_id").orderBy("created_at")

df = marketing_campaign \
    .withColumn("rnk1", rank().over(w1)) \
    .withColumn("rnk2", rank().over(w2)) \
    .where((col("rnk1") > 1) & (col("rnk2") == 1)) \
    .agg(countDistinct(col("user_id")).alias("user_count"))

# Convert to pandas to check the result
df.toPandas()
