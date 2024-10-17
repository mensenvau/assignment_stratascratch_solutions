from pyspark.sql.functions import count, col, size, split, sum, max

df = airbnb_search_details \
    .withColumn('count', size(split(col('amenities'), ',')).cast("long")) \
    .groupBy("city") \
    .agg(sum(col("count")).alias("sum")) \
    .orderBy(col("sum").desc())

max_count = df.agg(max('sum')).first()[0]
df = df.filter(col('sum') == max_count).select('city')

df_pandas = df.toPandas()

df_pandas
