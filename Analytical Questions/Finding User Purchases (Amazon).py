# Import your libraries
import pyspark
from pyspark.sql.window import Window 
from pyspark.sql.functions import col, lag, datediff

# Start writing code
window = Window.partitionBy("user_id").orderBy(col("created_at").desc())
df = amazon_transactions \
    .withColumn("prev_created_at", lag(col("created_at")).over(window)) \
    .withColumn("diff",datediff(col("prev_created_at"), col("created_at"))) \
    .filter(col("diff") <= 7) \
    .select("user_id") \
    .drop_duplicates()
    
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()