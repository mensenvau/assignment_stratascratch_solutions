# Import your libraries
import pyspark
from pyspark.sql.functions import col, when, sum

# Start writing code
df = ms_user_dimension.alias("t1") \
    .join(ms_acc_dimension.alias("t2"), col("t1.acc_id") == col("t2.acc_id") , "left") \
    .join(ms_download_facts.alias("t3"), col("t1.user_id") == col("t3.user_id"), "left") \
    .select("t1.user_id", "paying_customer", "date", "downloads") \
    .withColumn("yes", when(col("paying_customer")=="yes", col("downloads"))) \
    .withColumn("no", when(col("paying_customer")=="no", col("downloads"))) \
    .groupBy("date").agg(sum("no").alias("no"), sum("yes").alias("yes")) \
    .where(col("no") > col("yes"))
    

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()