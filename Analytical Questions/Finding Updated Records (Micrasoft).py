# Import your libraries
import pyspark

# Start writing code
df = ms_employee_salary \
    .groupBy("id", "first_name", "last_name", "department_id") \
    .agg({ "salary": "max" }) \
    .orderBy("id")
    
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()