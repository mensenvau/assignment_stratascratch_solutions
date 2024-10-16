# Import your libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# Start writing code
df = worker.join(title, worker.worker_id == title.worker_ref_id ,"inner")

window =  Window.orderBy(col("salary").desc())

df = df.withColumn("rank",rank() \
    .over(window)) \
    
df = df.filter(df.rank == 1).select(col("worker_title").alias("best_paid_title"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()

