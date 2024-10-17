# Import your libraries
import pyspark
from pyspark.sql.functions import col, count, lit, lag
from pyspark.sql.window import Window

# Start writing code
window = Window.partitionBy("company_name").orderBy(col("year").asc())

df = car_launches.groupBy("year", "company_name") \
        .agg(count(lit(1)).alias("net_products")) \
        .withColumn("prev_net_products", lag(col("net_products")).over(window)) \
        .withColumn("diff", col("net_products") - col("prev_net_products")) \
        .filter(col("year") == 2020) \
        .select("company_name", col("diff").alias("net_products"))


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()