# Import your libraries
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, coalesce

# Create a Spark session
spark = SparkSession.builder \
    .appName("Monotonically Increasing ID Example") \
    .getOrCreate()
    
# Generate list of numbers
start_number = 0
end_number = cookbook_titles.agg({"page_number": "max"}).collect()[0][0]
rng = spark.range(start_number, end_number, 2).selectExpr("id as page_number")

df_left = cookbook_titles \
    .filter(col("page_number") % 2 == 1) \
    .withColumn("page_number", col("page_number") - 1) \
    .select("page_number", col("title").alias("right_title"))

df_right = cookbook_titles \
    .filter(col("page_number") % 2 == 0) \
    .select("page_number", col("title").alias("left_title"))
    
df = df_left.alias("l") \
    .join(df_right.alias("r"),
        df_left["page_number"] == df_right["page_number"], "outer") \
    .select(coalesce(col("r.page_number"), col("l.page_number")).alias("page_number"), "left_title", "right_title")

df = rng.join(df, df["page_number"] == rng["page_number"], "left") \
    .select(rng["page_number"], "left_title", "right_title") \
    .orderBy("page_number")
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()