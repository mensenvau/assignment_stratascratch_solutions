# Import your libraries
import pyspark
from pyspark.sql.functions import date_format, col, sum, lag, round
from pyspark.sql.window import Window 

# Start writing code
window = Window.orderBy(col("year_month").asc())

df = sf_transactions \
    .withColumn("year_month", date_format(col("created_at"), "yyyy-MM")) \
    .groupBy("year_month").agg(sum("value").alias("r")) \
    .withColumn("pr", lag(col("r")).over(window)) \
    .withColumn("revenue_diff_pct", ((col("r") - col("pr")) / col("pr")) * 100) \
    .select("year_month", round(col("revenue_diff_pct"), 2).alias("revenue_diff_pct")) \
    .orderBy(col("year_month").asc())

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()