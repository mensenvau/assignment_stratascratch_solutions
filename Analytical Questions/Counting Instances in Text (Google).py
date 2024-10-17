# Import your libraries
import pyspark
from pyspark.sql.functions import col, lit, explode, split, count

# Start writing code
df = google_file_store \
    .withColumn("word", explode(split(col("contents")," ")) ) \
    .where((col("word") == lit("bull")) | (col("word") == lit("bear"))) \
    .groupBy(col("word")).agg(count("word").alias("nentry"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()