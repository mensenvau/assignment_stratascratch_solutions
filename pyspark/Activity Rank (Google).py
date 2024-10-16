# Import your libraries
import pyspark
from pyspark.sql.functions import col, rank, row_number, count
from pyspark.sql.window import Window

# Start writing code
window = Window.orderBy(col("total_emails").desc(), col("from_user").asc())

df = google_gmail_emails.groupBy("from_user").agg(count("id").alias("total_emails"))
df = df.withColumn("rank", row_number().over(window))


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()