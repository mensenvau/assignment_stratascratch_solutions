# Import your libraries
import pyspark
from pyspark.sql.functions import col, asc,desc

# Start writing code
df = forbes_global_2010_2014.orderBy(col("profits").desc()).limit(3)
df = df.select("company", "profits")

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()