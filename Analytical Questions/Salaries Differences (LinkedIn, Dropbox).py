# Import your libraries

import pyspark
from pyspark.sql.functions import col, when, abs, max

df = db_employee \
    .join(db_dept, db_employee["department_id"] == db_dept["id"], "inner") \
    .agg(
        max(when(col("department") == "marketing", col("salary"))).alias("mar_max"),
        max(when(col("department") == "engineering", col("salary"))).alias("eng_max")
    ) \
    .withColumn("salary_difference", abs(col("eng_max") - col("mar_max"))) \
    .select("salary_difference")

# To validate your solution, convert your final pySpark df to a pandas df
df = df.toPandas()
